package db

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

// Write-path benchmarks. The read-path benchmarks live in bench_test.go and
// share setupBenchDB/seed/runScaled with these.
//
// InsertBlockDataBatch is the whole realtime ingest write path: one Postgres
// transaction per block covering blocks, transactions, logs, token_transfers,
// internal_transactions, address_stats and chain_counters. Nothing here calls
// RefreshTokenStats -- that runs a level up in the indexer and is measured
// separately (PRST-4493), so these numbers are the insert cost alone.

// benchTxsPerBlock is the block density the write path is measured at. A quiet
// chain sits in the tens of transactions per block and a busy one in the low
// hundreds, so 250 is in the loaded range where ingest cost actually matters.
const benchTxsPerBlock = 250

// erc20TransferTopic is the real Transfer(address,address,uint256) signature,
// so a sampled row looks like production rather than filler.
const erc20TransferTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

// blockShape describes one per-block ingest workload.
//
// Shape is the point of this benchmark. A block of plain value transfers and a
// block of the same number of ERC-20 transfers are completely different ingest
// workloads: the second one also writes a log and a token_transfers row per
// transaction. Reporting tx/s without saying which shape produced it is how
// PRST-4453 ended up with a misleading number.
type blockShape struct {
	name             string
	logsPerTx        int
	transfersPerTx   int
	gasPerTx         uint64
	skipAddressStats bool
	// scatterHashes generates transaction hashes that are uniformly
	// distributed instead of ascending. See benchScatteredHash.
	scatterHashes bool
}

// gasPerTx are the ordinary EVM costs: 21,000 for a bare value transfer,
// ~65,000 for an ERC-20 transfer that rewrites two balance slots and emits a
// log. They only feed the reported gas/s, not the SQL.
var blockShapes = []blockShape{
	{name: "plain", logsPerTx: 0, transfersPerTx: 0, gasPerTx: 21_000},
	{name: "erc20", logsPerTx: 1, transfersPerTx: 1, gasPerTx: 65_000},
	// Catchup mode sets SkipAddressStats to sidestep the address_stats
	// deadlock (PRST-4495). Measuring it as its own shape isolates what
	// address_stats maintenance costs on the realtime path.
	{name: "erc20-no-address-stats", logsPerTx: 1, transfersPerTx: 1, gasPerTx: 65_000, skipAddressStats: true},
	// transactions.hash is the PRIMARY KEY, so the distribution of the hashes
	// decides how much of that index has to be resident. The shapes above use
	// ascending hashes, which only ever touch the right-hand edge of the
	// B-tree; real hashes are uniformly distributed and dirty a random leaf per
	// row. This shape is otherwise identical to erc20, so the delta between the
	// two is the cost of key distribution alone.
	{name: "erc20-scattered-hash", logsPerTx: 1, transfersPerTx: 1, gasPerTx: 65_000, scatterHashes: true},
}

// benchScatteredHash returns a hash of exactly the same LENGTH as the ascending
// one makeBenchBlockData otherwise builds, with the characters scattered across
// the keyspace. Holding the length constant matters: a real 66-character hash
// would change both the distribution and the index key size at once, and only
// the distribution is under test here. Deterministic, so runs are comparable.
func benchScatteredHash(blockNum uint64, i int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%d", blockNum, i)))
	// "0xbenchtx" + two %020d is 49 characters; match it.
	return "0x" + hex.EncodeToString(sum[:])[:47]
}

func BenchmarkInsertBlockDataBatch(b *testing.B) {
	for _, shape := range blockShapes {
		b.Run(shape.name, func(b *testing.B) {
			runScaled(b, func(b *testing.B, d *DB) {
				benchInsertBlockData(b, d, shape)
			})
		})
	}
}

func benchInsertBlockData(b *testing.B, d *DB, shape blockShape) {
	b.StopTimer()
	ctx := context.Background()

	// Every INSERT inside InsertBlockDataBatch is ON CONFLICT DO NOTHING, so
	// reusing a block number or tx hash would time a no-op instead of an
	// insert. Start above whatever seed() laid down.
	var head uint64
	if err := d.pool.QueryRow(ctx, `SELECT COALESCE(MAX(number), 0) FROM blocks`).Scan(&head); err != nil {
		b.Fatalf("read seeded head: %v", err)
	}

	// Build every batch up front: generating them is not what we are
	// measuring. b.N stays small because one iteration is a multi-statement
	// round trip, so holding them all is cheap.
	batches := make([]*BlockData, b.N)
	for i := range batches {
		batches[i] = makeBenchBlockData(head+uint64(i)+1, shape)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if err := d.InsertBlockDataBatch(ctx, batches[i]); err != nil {
			b.Fatalf("InsertBlockDataBatch: %v", err)
		}
	}

	// Report all three rates on the same line. tx/s alone is misleading --
	// ingest cost tracks gas and block count too -- and the project target is
	// stated in tx/s, so it has to be readable next to its own shape.
	b.StopTimer()
	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		blocks := float64(b.N)
		txs := blocks * benchTxsPerBlock
		b.ReportMetric(blocks/elapsed, "blocks/s")
		b.ReportMetric(txs/elapsed, "tx/s")
		b.ReportMetric(txs*float64(shape.gasPerTx)/elapsed, "gas/s")
	}
}

// makeBenchBlockData builds one block's worth of ingest input for the given
// shape. Logs and transfers reference transactions in the same batch, because
// both tables carry a foreign key to transactions(hash).
func makeBenchBlockData(blockNum uint64, shape blockShape) *BlockData {
	ts := uint64(time.Now().Unix()) + blockNum

	data := &BlockData{
		Block: &types.Block{
			Number:           blockNum,
			Hash:             fmt.Sprintf("0xbenchblk%012d", blockNum),
			ParentHash:       fmt.Sprintf("0xbenchblk%012d", blockNum-1),
			Timestamp:        ts,
			GasUsed:          shape.gasPerTx * benchTxsPerBlock,
			GasLimit:         30_000_000,
			TransactionCount: benchTxsPerBlock,
		},
		Transactions:     make([]*types.Transaction, 0, benchTxsPerBlock),
		Logs:             make([]*types.Log, 0, benchTxsPerBlock*shape.logsPerTx),
		Transfers:        make([]*types.TokenTransfer, 0, benchTxsPerBlock*shape.transfersPerTx),
		AddressStats:     make(map[string]*AddressStatsDelta, benchTxsPerBlock),
		SkipAddressStats: shape.skipAddressStats,
	}

	token := fmt.Sprintf("0xtoken%035d", 1)
	topic0 := erc20TransferTopic

	for i := 0; i < benchTxsPerBlock; i++ {
		hash := fmt.Sprintf("0xbenchtx%020d%020d", blockNum, i)
		if shape.scatterHashes {
			hash = benchScatteredHash(blockNum, i)
		}
		from := benchSeededAddr(i)
		// Senders come from the pool seed() already wrote, so address_stats
		// takes its ON CONFLICT DO UPDATE branch -- the steady-state hot path.
		// Every tenth recipient is brand new so the INSERT branch, and the
		// addresses_total counter increment that depends on it, are covered
		// too.
		to := benchSeededAddr(i + 1)
		if i%10 == 0 {
			to = fmt.Sprintf("0xnew%025d%012d", blockNum, i)
		}
		gasLimit := shape.gasPerTx
		blockTS := ts

		data.Transactions = append(data.Transactions, &types.Transaction{
			Hash:           hash,
			BlockNumber:    blockNum,
			BlockTimestamp: ts,
			TxIndex:        i,
			From:           from,
			To:             &to,
			Value:          types.JSONString("1000000000000000"),
			GasUsed:        shape.gasPerTx,
			GasPrice:       20_000_000_000,
			GasLimit:       &gasLimit,
			TxType:         2,
			InputData:      "0x",
			Status:         1,
		})

		for j := 0; j < shape.logsPerTx; j++ {
			t1, t2 := from, to
			data.Logs = append(data.Logs, &types.Log{
				TxHash:      hash,
				LogIndex:    j,
				Address:     token,
				Topic0:      &topic0,
				Topic1:      &t1,
				Topic2:      &t2,
				Data:        "0x00000000000000000000000000000000000000000000000000038d7ea4c68000",
				BlockNumber: blockNum,
				Timestamp:   &blockTS,
			})
		}

		for j := 0; j < shape.transfersPerTx; j++ {
			data.Transfers = append(data.Transfers, &types.TokenTransfer{
				TxHash:       hash,
				LogIndex:     j,
				TokenAddress: token,
				From:         from,
				To:           to,
				Value:        types.JSONString("1000000000000000"),
				BlockNumber:  blockNum,
				Timestamp:    &blockTS,
				TransferType: "transfer",
				TokenType:    "ERC20",
			})
		}

		addBenchDelta(data.AddressStats, from, blockNum, shape.transfersPerTx)
		addBenchDelta(data.AddressStats, to, blockNum, shape.transfersPerTx)
	}

	return data
}

// benchSeededAddr mirrors the 20-address pool seed() writes, so deltas against
// it hit rows that already exist.
func benchSeededAddr(i int) string {
	return fmt.Sprintf("0xaddr%036d", i%20)
}

func addBenchDelta(m map[string]*AddressStatsDelta, addr string, blockNum uint64, transfers int) {
	d, ok := m[addr]
	if !ok {
		d = &AddressStatsDelta{Address: addr, BlockNumber: blockNum}
		m[addr] = d
	}
	d.TxCountDelta++
	d.TokenTransferDelta += transfers
	d.BlockNumber = blockNum
}
