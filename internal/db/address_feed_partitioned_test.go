package db

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func partitionHistory(t *testing.T, d *DB, partitionBlocks, upto uint64) {
	t.Helper()
	ctx := context.Background()
	_, err := d.pool.Exec(ctx, `SELECT history_cut_constrain(1)`)
	require.NoError(t, err)
	for _, tbl := range HistoryTables {
		_, err := d.pool.Exec(ctx, `ALTER TABLE `+tbl+`_legacy VALIDATE CONSTRAINT history_cut`)
		require.NoError(t, err)
	}
	_, err = d.pool.Exec(ctx, `SELECT history_cut_finish(1, $1)`, int64(partitionBlocks))
	require.NoError(t, err)
	for top := partitionBlocks * 16; ; top += partitionBlocks * 16 {
		_, err = d.pool.Exec(ctx, `SELECT history_partitions_ensure($1, $2)`, int64(min(top, upto)), int64(partitionBlocks))
		require.NoError(t, err)
		if top >= upto {
			break
		}
	}
}

type planNode struct {
	NodeType string     `json:"Node Type"`
	Relation string     `json:"Relation Name"`
	Index    string     `json:"Index Name"`
	Rows     float64    `json:"Actual Rows"`
	Loops    float64    `json:"Actual Loops"`
	Plans    []planNode `json:"Plans"`
}

func (n planNode) usesIndexSuffix(suffix string) bool {
	if strings.HasSuffix(n.Index, suffix) {
		return true
	}
	for _, c := range n.Plans {
		if c.usesIndexSuffix(suffix) {
			return true
		}
	}
	return false
}

type partitionProbe struct{ executions, partitionScans float64 }

func (n planNode) transactionHashProbes() []partitionProbe {
	var probes []partitionProbe
	if n.NodeType == "Append" && len(n.Plans) > 0 {
		probe := partitionProbe{executions: n.Loops}
		onTransactions, byHash := true, false
		for _, c := range n.Plans {
			onTransactions = onTransactions && strings.HasPrefix(c.Relation, "transactions")
			byHash = byHash || c.usesIndexSuffix("_pkey")
			probe.partitionScans += c.Loops
		}
		if onTransactions && byHash {
			return append(probes, probe)
		}
	}
	for _, c := range n.Plans {
		probes = append(probes, c.transactionHashProbes()...)
	}
	return probes
}

func (n planNode) rowsFrom(prefix string) float64 {
	var sum float64
	if strings.HasPrefix(n.Relation, prefix) {
		sum += n.Rows * n.Loops
	}
	for _, c := range n.Plans {
		sum += c.rowsFrom(prefix)
	}
	return sum
}

func TestGetTransactionsByAddressOnPartitionedHistoryReadsOnlyTheAddressRows(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	const (
		blocks          = 40_000
		txsPerBlock     = 5
		partitionBlocks = 700
	)
	partitionHistory(t, d, partitionBlocks, blocks)

	_, err := d.pool.Exec(ctx, `
		INSERT INTO transactions (hash, block_number, block_timestamp, tx_index, from_address, to_address, value, gas_used, gas_price, nonce, tx_type, input_data, status)
		SELECT 'n' || g, 1 + g / $1, g, g % $1, 'f' || (g % 50000), 't' || (g % 70000), 0, 21000, 1, 0, 0, '0x', 1
		FROM generate_series(0, $2 - 1) g`, txsPerBlock, blocks*txsPerBlock)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `
		INSERT INTO token_transfers (tx_hash, log_index, token_address, from_address, to_address, value, block_number)
		SELECT 'n' || g, 0, 'tok', 'f' || (g % 50000), 't' || (g % 70000), 1, 1 + g / $1
		FROM generate_series(0, $2 - 1, 10) g`, txsPerBlock, blocks*txsPerBlock)
	require.NoError(t, err)

	const (
		withTransfers    = "0xaaaa00000000000000000000000000000000a001"
		withoutTransfers = "0xbbbb00000000000000000000000000000000b002"
		other            = "0xcccc00000000000000000000000000000000c003"
	)
	tx := func(hash string, block uint64, idx int, from, to string) {
		_, err := d.pool.Exec(ctx, `
			INSERT INTO transactions (hash, block_number, block_timestamp, tx_index, from_address, to_address, value, gas_used, gas_price, nonce, tx_type, input_data, status)
			VALUES ($1, $2, $2, $3, $4, $5, 0, 21000, 1, 0, 0, '0x', 1)`, hash, block, idx, from, to)
		require.NoError(t, err)
	}
	transfer := func(hash string, block uint64, logIndex int, from, to string) {
		_, err := d.pool.Exec(ctx, `
			INSERT INTO token_transfers (tx_hash, log_index, token_address, from_address, to_address, value, block_number)
			VALUES ($1, $2, 'tok', $3, $4, 1, $5)`, hash, logIndex, from, to, block)
		require.NoError(t, err)
	}

	tx("a-from-3", 3, 5, withTransfers, other)
	tx("a-from-9001", 9001, 5, withTransfers, other)
	tx("a-to-17000", 17000, 5, other, withTransfers)
	tx("a-from-25000", 25000, 6, withTransfers, other)
	for _, x := range []struct {
		hash  string
		block uint64
		idx   int
		logs  int
		from  bool
	}{
		{"a-tt-2", 2, 5, 1, false},
		{"a-tt-700", 700, 5, 3, false},
		{"a-tt-701", 701, 5, 1, true},
		{"a-tt-14000-6", 14000, 6, 2, false},
		{"a-tt-14000-7", 14000, 7, 1, true},
		{"a-tt-14000-8", 14000, 8, 1, false},
		{"a-tt-25000", 25000, 5, 1, false},
		{"a-tt-39999", 39999, 5, 4, false},
	} {
		tx(x.hash, x.block, x.idx, other, other)
		for l := 0; l < x.logs; l++ {
			if x.from {
				transfer(x.hash, x.block, 100+x.idx*10+l, withTransfers, other)
			} else {
				transfer(x.hash, x.block, 100+x.idx*10+l, other, withTransfers)
			}
		}
	}
	tx("a-from-and-tt-30000", 30000, 5, withTransfers, other)
	transfer("a-from-and-tt-30000", 30000, 150, other, withTransfers)

	tx("b-from-50", 50, 5, withoutTransfers, other)
	tx("b-to-20000", 20000, 5, other, withoutTransfers)
	tx("b-to-20000-6", 20000, 6, other, withoutTransfers)
	tx("b-from-39000", 39000, 5, withoutTransfers, other)

	_, err = d.pool.Exec(ctx, `ANALYZE`)
	require.NoError(t, err)

	reference := func(addr string) []string {
		rows, err := d.pool.Query(ctx, `
			SELECT hash FROM transactions
			WHERE from_address = $1 OR to_address = $1
			   OR hash IN (SELECT tx_hash FROM token_transfers WHERE from_address = $1 OR to_address = $1)
			ORDER BY block_number DESC, tx_index DESC`, addr)
		require.NoError(t, err)
		defer rows.Close()
		var hashes []string
		for rows.Next() {
			var h string
			require.NoError(t, rows.Scan(&h))
			hashes = append(hashes, h)
		}
		require.NoError(t, rows.Err())
		return hashes
	}

	var probedByHash bool
	for _, addr := range []string{withTransfers, withoutTransfers} {
		want := reference(addr)
		require.NotEmpty(t, want)
		for _, limit := range []int{1, 2, 3, 50} {
			t.Run(fmt.Sprintf("%s/limit%d", addr[:6], limit), func(t *testing.T) {
				var got []string
				var bound *AddressFeedBound
				for range len(want) + 2 {
					page, err := d.GetTransactionsByAddress(ctx, strings.ToUpper(addr[:2])+addr[2:], limit, bound)
					require.NoError(t, err)
					if len(page) == 0 {
						break
					}
					require.LessOrEqual(t, len(page), limit)
					for _, tx := range page {
						got = append(got, tx.Hash)
					}
					last := page[len(page)-1]
					idx := uint32(last.TxIndex)
					bound = &AddressFeedBound{Block: last.BlockNumber, Index: &idx}
				}
				require.Equal(t, want, got)
			})
		}

		for _, bound := range []struct {
			block uint64
			idx   uint32
		}{{math.MaxInt64, 0}, {25000, 6}, {14000, 7}} {
			var plan []byte
			require.NoError(t, d.pool.QueryRow(ctx, `EXPLAIN (ANALYZE, FORMAT JSON) `+addressTransactionsQuery,
				addr, bound.block, bound.idx, 6).Scan(&plan))
			var root []struct {
				Plan planNode `json:"Plan"`
			}
			require.NoError(t, json.Unmarshal(plan, &root))
			read := root[0].Plan.rowsFrom("transactions")
			require.LessOrEqual(t, read, float64(60), "%s before %v reads %v transactions rows", addr, bound, read)
			for _, probe := range root[0].Plan.transactionHashProbes() {
				require.LessOrEqual(t, probe.partitionScans, probe.executions,
					"%s before %v: a lookup by hash scans more than its own partition", addr, bound)
				if addr == withTransfers && probe.executions > 0 {
					probedByHash = true
				}
			}
		}
	}
	require.True(t, probedByHash, "the transfer candidates must be looked up by hash")
}
