package db

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/big"
	"regexp"
	"sort"
	"strings"

	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/types"

	"github.com/jackc/pgx/v5"
)

// workMemPattern matches a Postgres memory size (digits + optional unit) so the
// value can be safely interpolated into SET LOCAL, which cannot be parameterized.
var workMemPattern = regexp.MustCompile(`(?i)^[0-9]+(kb|mb|gb|tb)?$`)

func sanitizeWorkMem(v string) string {
	v = strings.TrimSpace(v)
	if workMemPattern.MatchString(v) {
		return v
	}
	return "1GB"
}

type BlockData struct {
	Block                *types.Block
	Transactions         []*types.Transaction
	Logs                 []*types.Log
	Transfers            []*types.TokenTransfer
	Contracts            []*types.Contract
	Tokens               []*types.Token
	NFTTokens            []*types.NFTToken
	InternalTransactions []*types.InternalTransaction
	AddressStats         map[string]*AddressStatsDelta
	SkipAddressStats     bool // Skip address_stats updates (for catchup mode to avoid deadlocks)
}

// zeroAddress is the mint/burn counterparty. Addresses are canonical lowercase
// from migration 005 onward, and every write path lowercases, so a plain
// comparison is enough.
const zeroAddress = "0x0000000000000000000000000000000000000000"

// tokens.total_supply is NUMERIC(78, 0). Every individual transfer value fits
// that, but a running mint-minus-burn total need not: two mints near the
// uint256 ceiling already exceed 78 digits. The sum is computed in unconstrained
// numeric and only the assignment to the column can raise "numeric field
// overflow", so clamping the value before it is assigned removes the error
// entirely.
//
// Saturating is deliberate, and it is a blast-radius decision rather than a
// correctness one. These counters now move inside the ingest transaction, so an
// overflow raised here would roll back the block's transfers, logs and address
// stats, and the retry would fail on the same block forever -- one token with an
// unrepresentable supply would stop the chain. A token whose net supply exceeds
// what the column can hold is already outside what this schema can describe, so
// the derived figure saturates and ingest continues.
const numeric78Max = `(10::numeric^78 - 1)`

// tokenLockKeys maps token addresses to the ascending, deduplicated advisory
// keys that guard them. The hash is FNV-64a over the lowercased address rather
// than the address bytes themselves, so it needs no assumption about the
// address being well-formed hex; a collision between two distinct addresses
// costs a little needless serialisation and nothing else. Sorting is by key,
// not by address -- the acquisition order has to be the order of the thing
// actually being locked.
func tokenLockKeys(addrs []string) []int64 {
	seen := make(map[int64]struct{}, len(addrs))
	keys := make([]int64, 0, len(addrs))
	for _, a := range addrs {
		h := fnv.New64a()
		_, _ = h.Write([]byte(strings.ToLower(a)))
		k := int64(h.Sum64())
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// clampSupply bounds a total_supply expression to what the column can store.
func clampSupply(expr string) string {
	return `LEAST(GREATEST(` + expr + `, -` + numeric78Max + `), ` + numeric78Max + `)`
}

// tokenStatsFromTransfers derives a token's two history-scale counters from
// token_transfers in one pass: the transfer row count, and the ERC20
// mint-minus-burn supply. $1 is the lowercased token address.
//
// The token_type filter sits inside the CASE rather than in the WHERE so that
// COUNT(*) still spans every transfer of the token while the sums stay
// ERC20-only -- the same split the two separate queries in RefreshTokenStats
// used to make, at one scan instead of two.
const tokenStatsFromTransfers = `
	SELECT COUNT(*),
	       COALESCE(
	           SUM(CASE WHEN token_type = 'ERC20' AND from_address = '` + zeroAddress + `' THEN value ELSE 0 END)
	         - SUM(CASE WHEN token_type = 'ERC20' AND to_address   = '` + zeroAddress + `' THEN value ELSE 0 END),
	           0)
	FROM token_transfers WHERE token_address = $1`

// tokenStatsDelta is one token's counter movement contributed by a single
// batch: the transfer rows that genuinely landed, and the ERC20 supply change
// those rows imply. Both are applied in the batch's own transaction, so a read
// that follows the commit sees them.
type tokenStatsDelta struct {
	transferDelta int64
	supplyDelta   *big.Int
}

type AddressStatsDelta struct {
	Address              string
	TxCountDelta         int
	InternalTxCountDelta int
	TokenTransferDelta   int
	IsContract           bool
	BlockNumber          uint64
}

func (d *DB) InsertBlockDataBatch(ctx context.Context, data *BlockData) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var deltaBlocks, deltaTxs, deltaTokens, deltaAddresses, deltaTransfers int64

	// Per-token counters, maintained here rather than recomputed per block.
	// tokenDeltas accumulates this batch's movement; seedTokens lists rows
	// that this batch created and therefore have to be reconciled against
	// whatever history already exists.
	tokenDeltas := map[string]*tokenStatsDelta{}
	var seedTokens []string

	if data.Block != nil {
		ct, err := tx.Exec(ctx, `
			INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, base_fee_per_gas, transaction_count,
				size, difficulty, total_difficulty, nonce, miner, extra_data, state_root, transactions_root, receipts_root)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
			ON CONFLICT (number) DO NOTHING`,
			data.Block.Number, data.Block.Hash, data.Block.ParentHash, data.Block.Timestamp,
			data.Block.GasUsed, data.Block.GasLimit, data.Block.BaseFeePerGas, data.Block.TransactionCount,
			data.Block.Size, data.Block.Difficulty, data.Block.TotalDifficulty, data.Block.Nonce,
			data.Block.Miner, data.Block.ExtraData, data.Block.StateRoot, data.Block.TransactionsRoot, data.Block.ReceiptsRoot)
		if err != nil {
			return err
		}
		deltaBlocks += ct.RowsAffected()
	}

	if len(data.Transactions) > 0 {
		// Precompute the set of tx hashes that have token transfers in this
		// batch so the category bitfield can be set atomically on the insert.
		txsWithTransfers := buildTokenTransferTxSet(data.Transfers)
		var blockTimestamp *uint64
		if data.Block != nil {
			ts := data.Block.Timestamp
			blockTimestamp = &ts
		}
		batch := &pgx.Batch{}
		for _, t := range data.Transactions {
			_, hasTransfer := txsWithTransfers[t.Hash]
			categories := computeTxCategories(t, hasTransfer)
			batch.Queue(`
				INSERT INTO transactions (hash, block_number, block_timestamp, tx_index, from_address, to_address, value, gas_used, gas_price,
					gas_limit, max_fee_per_gas, max_priority_fee_per_gas, nonce, tx_type, input_data, status, error, revert_reason, categories)
				VALUES ($1, $2, $3, $4, LOWER($5), LOWER($6), $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
				ON CONFLICT (hash) DO NOTHING`,
				t.Hash, t.BlockNumber, blockTimestamp, t.TxIndex, t.From, t.To, t.Value, t.GasUsed, t.GasPrice,
				t.GasLimit, t.MaxFeePerGas, t.MaxPriorityFeePerGas, t.Nonce, t.TxType, t.InputData, t.Status, t.Error, t.RevertReason, categories)
		}
		br := tx.SendBatch(ctx, batch)
		for range data.Transactions {
			ct, err := br.Exec()
			if err != nil {
				_ = br.Close()
				return err
			}
			deltaTxs += ct.RowsAffected()
		}
		if err := br.Close(); err != nil {
			return err
		}
	}

	if len(data.Contracts) > 0 {
		batch := &pgx.Batch{}
		for _, c := range data.Contracts {
			batch.Queue(`
				INSERT INTO contracts (address, bytecode, bytecode_hash, creator, creation_tx, block_number, is_verified,
					contract_name, compiler_version, optimization_used, source_code, abi)
				VALUES (LOWER($1), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
				ON CONFLICT (address) DO NOTHING`,
				c.Address, c.Bytecode, c.BytecodeHash, c.Creator, c.CreationTx, c.BlockNumber, c.IsVerified,
				c.ContractName, c.CompilerVersion, c.OptimizationUsed, c.SourceCode, c.ABI)
		}
		br := tx.SendBatch(ctx, batch)
		if err := br.Close(); err != nil {
			return err
		}
	}

	// Every tokens row this transaction will lock, taken in one ascending pass
	// before any of them is touched individually.
	//
	// Sorting each phase separately is not enough, and that was the defect in
	// the first attempt at this. Locks are acquired in two runs -- the upsert
	// below, over the tokens the block discovered, and the delta UPDATEs at the
	// end, over the tokens its transfers touched -- and sorted(A) followed by
	// sorted(B) is a total order only when A and B are the same set. They are
	// not: an ON CONFLICT DO UPDATE locks an existing row just as much as it
	// creates one, so a token already in the database lands in the upsert set
	// whenever this process's token cache has not recorded it yet. One worker
	// then took a high address (discovered) before a low one (transfer), while
	// another with a warm cache took the low one first.
	//
	// Row locks alone cannot carry that total order, because a row that does
	// not exist yet cannot be locked. SELECT ... FOR UPDATE locks only the rows
	// committed when the statement starts and Postgres takes no gap or
	// predicate lock, so an address in the union with no tokens row leaves this
	// pass holding nothing for it -- and the delta UPDATE at the end then takes
	// that row's lock as its first acquisition, out of order, whenever another
	// transaction created the row in between. Two transfers of a token whose
	// metadata fetch has not succeeded yet is all it takes: indexer.go drops
	// such a token from blockData.Tokens while its transfers stay in the batch.
	//
	// So the union is guarded by transaction-scoped advisory locks first. An
	// advisory key exists whether or not its row does, which closes the gap.
	// No transaction takes a tokens row lock before it holds every advisory
	// key in its union, so two transactions past this pass have disjoint
	// unions -- and therefore disjoint row locks -- while two with an
	// overlapping union serialise here, in ascending key order. Key collisions
	// between distinct addresses only widen that serialisation; they cannot
	// reorder it, because the order is the key's.
	//
	// The row pass below is kept on top of it: writers that do not come through
	// here (reorg compensation, RefreshTokenStats, UpdateTokenSupply) take row
	// locks and no advisory ones, and the ascending row pass is what keeps this
	// transaction ordered against them.
	//
	// One statement each, so one round trip each. unnest emits in array order
	// and nothing above it reorders, so the keys are acquired in the order Go
	// sorted them. ORDER BY on the row pass makes its acquisition order
	// explicit rather than leaving it to the plan, and on the primary-key btree
	// it is the same order anyway.
	lockTokens := make(map[string]struct{}, len(data.Tokens)+len(data.Transfers))
	for _, t := range data.Tokens {
		lockTokens[strings.ToLower(t.Address)] = struct{}{}
	}
	for _, t := range data.Transfers {
		lockTokens[strings.ToLower(t.TokenAddress)] = struct{}{}
	}
	if len(lockTokens) > 0 {
		ordered := make([]string, 0, len(lockTokens))
		for addr := range lockTokens {
			ordered = append(ordered, addr)
		}
		sort.Strings(ordered)

		keys := tokenLockKeys(ordered)
		if _, err := tx.Exec(ctx,
			`SELECT pg_advisory_xact_lock(k) FROM unnest($1::bigint[]) AS k`,
			keys); err != nil {
			return err
		}

		if _, err := tx.Exec(ctx,
			`SELECT 1 FROM tokens WHERE address = ANY($1::text[]) ORDER BY address FOR UPDATE`,
			ordered); err != nil {
			return err
		}
	}

	if len(data.Tokens) > 0 {
		// Sorted so the rows this creates are created in ascending order; the
		// rows that already exist are locked by the pass above. The copy is
		// because the caller reuses its slice, matching InsertBalancesBatch.
		orderedTokens := make([]*types.Token, len(data.Tokens))
		copy(orderedTokens, data.Tokens)
		sort.SliceStable(orderedTokens, func(i, j int) bool {
			return strings.ToLower(orderedTokens[i].Address) < strings.ToLower(orderedTokens[j].Address)
		})

		batch := &pgx.Batch{}
		for _, t := range orderedTokens {
			batch.Queue(`
				INSERT INTO tokens (address, symbol, name, decimals, token_type, total_supply, block_number, creation_tx, l1_address)
				VALUES (LOWER($1), $2, $3, $4, $5, $6, $7, $8, $9)
				ON CONFLICT (address) DO UPDATE SET
					symbol = COALESCE(EXCLUDED.symbol, tokens.symbol),
					name = COALESCE(EXCLUDED.name, tokens.name),
					decimals = COALESCE(EXCLUDED.decimals, tokens.decimals),
					-- Promote a previously misclassified row (e.g. an ERC721 first
					-- seen before this fix, stored as the ERC20 default) when a
					-- more specific type is observed, but never downgrade.
					token_type = CASE WHEN EXCLUDED.token_type <> 'ERC20' THEN EXCLUDED.token_type ELSE tokens.token_type END,
					total_supply = COALESCE(EXCLUDED.total_supply, tokens.total_supply)
				RETURNING (xmax = 0)`,
				t.Address, t.Symbol, t.Name, t.Decimals, t.TokenType, t.TotalSupply, t.BlockNumber, t.CreationTx, t.L1Address)
		}
		br := tx.SendBatch(ctx, batch)
		// Results come back in queue order, so this must range the same sorted
		// slice the batch was built from. seedTokens inherits that order, which
		// is what keeps the seed's locks in the one total order too.
		for _, t := range orderedTokens {
			var wasNew bool
			if err := br.QueryRow().Scan(&wasNew); err != nil {
				_ = br.Close()
				return err
			}
			if wasNew {
				deltaTokens++
				seedTokens = append(seedTokens, strings.ToLower(t.Address))
			}
		}
		if err := br.Close(); err != nil {
			return err
		}
	}

	// A tokens row can be created long after the token's first transfer was
	// stored. Discovery needs an RPC metadata fetch, and a fetch that fails is
	// retried on a later block while the transfers keep landing -- so those
	// earlier transfers never reached a delta. Seed each freshly created row
	// from the history already on disk, before this batch's own transfers are
	// added on top below. Absolute recompute made the old code immune to this
	// by accident; the deltas have to say it out loud.
	//
	// This runs once per token over that token's own index range, and for a
	// token discovered on its first transfer it aggregates zero rows.
	if len(seedTokens) > 0 {
		batch := &pgx.Batch{}
		for _, addr := range seedTokens {
			batch.Queue(`
				UPDATE tokens SET
					transfer_count = s.cnt,
					total_supply = CASE WHEN tokens.token_type = 'ERC20'
						THEN `+clampSupply(`s.supply`)+`
						ELSE tokens.total_supply END
				FROM (`+tokenStatsFromTransfers+`) AS s(cnt, supply)
				WHERE tokens.address = $1`, addr)
		}
		br := tx.SendBatch(ctx, batch)
		if err := br.Close(); err != nil {
			return err
		}
	}

	if len(data.NFTTokens) > 0 {
		batch := &pgx.Batch{}
		for _, n := range data.NFTTokens {
			batch.Queue(`
				INSERT INTO nft_tokens (token_address, token_id, owner, token_uri, block_number)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (token_address, token_id) DO UPDATE SET
					owner = EXCLUDED.owner,
					-- tokenURI is captured once at mint; a later transfer carries
					-- no URI, so keep the existing value rather than nulling it.
					token_uri = COALESCE(nft_tokens.token_uri, EXCLUDED.token_uri),
					block_number = EXCLUDED.block_number,
					updated_at = NOW()`,
				n.TokenAddress, n.TokenID, n.Owner, n.TokenURI, n.BlockNumber)
		}
		br := tx.SendBatch(ctx, batch)
		if err := br.Close(); err != nil {
			return err
		}
	}

	if len(data.Logs) > 0 {
		batch := &pgx.Batch{}
		for _, l := range data.Logs {
			batch.Queue(`
				INSERT INTO logs (tx_hash, log_index, address, topic0, topic1, topic2, topic3, data, block_number, timestamp, removed)
				VALUES ($1, $2, LOWER($3), $4, $5, $6, $7, $8, $9, $10, $11)
				ON CONFLICT (tx_hash, log_index) DO NOTHING`,
				l.TxHash, l.LogIndex, l.Address, l.Topic0, l.Topic1, l.Topic2, l.Topic3, l.Data, l.BlockNumber, l.Timestamp, l.Removed)
		}
		br := tx.SendBatch(ctx, batch)
		if err := br.Close(); err != nil {
			return err
		}
	}

	if len(data.Transfers) > 0 {
		batch := &pgx.Batch{}
		for _, t := range data.Transfers {
			batch.Queue(`
				INSERT INTO token_transfers (tx_hash, log_index, token_address, from_address, to_address, value,
					block_number, timestamp, transfer_type, token_type, token_id, is_internal)
				VALUES ($1, $2, LOWER($3), LOWER($4), LOWER($5), $6, $7, $8, $9, $10, $11, $12)
				ON CONFLICT (tx_hash, log_index) DO NOTHING`,
				t.TxHash, t.LogIndex, t.TokenAddress, t.From, t.To, t.Value,
				t.BlockNumber, t.Timestamp, t.TransferType, t.TokenType, t.TokenID, t.IsInternal)
		}
		br := tx.SendBatch(ctx, batch)
		// Results come back in queue order, so the i-th result belongs to the
		// i-th transfer -- which is what lets a row be attributed to its token.
		for _, t := range data.Transfers {
			ct, err := br.Exec()
			if err != nil {
				_ = br.Close()
				return err
			}
			// ON CONFLICT DO NOTHING ⇒ only genuinely new rows count toward the total.
			if ct.RowsAffected() == 0 {
				continue
			}
			deltaTransfers += ct.RowsAffected()

			token := strings.ToLower(t.TokenAddress)
			d := tokenDeltas[token]
			if d == nil {
				d = &tokenStatsDelta{supplyDelta: new(big.Int)}
				tokenDeltas[token] = d
			}
			d.transferDelta++

			// Mirror the supply definition exactly: ERC20 rows only, mints add
			// and burns subtract, and a 0x0 -> 0x0 row does both and so nets
			// to zero rather than being treated as one or the other.
			if t.TokenType != types.TokenTypeERC20 {
				continue
			}
			v, ok := new(big.Int).SetString(string(t.Value), 10)
			if !ok {
				// NUMERIC(78,0) NOT NULL upstream; an unparseable value would
				// silently skew supply, so refuse the batch instead.
				_ = br.Close()
				return fmt.Errorf("transfer %s/%d: invalid value %q", t.TxHash, t.LogIndex, string(t.Value))
			}
			if strings.ToLower(t.From) == zeroAddress {
				d.supplyDelta.Add(d.supplyDelta, v)
			}
			if strings.ToLower(t.To) == zeroAddress {
				d.supplyDelta.Sub(d.supplyDelta, v)
			}
		}
		if err := br.Close(); err != nil {
			return err
		}
	}

	// Apply the per-token counters. Same transaction as the transfer rows they
	// describe, so the two can never be observed out of step -- these two
	// counters are read-after-write correct today and stay that way.
	//
	// total_supply moves only for ERC20 tokens, matching the branch in
	// RefreshTokenStats that used to own it; NULL means "never computed", so
	// COALESCE is what turns the first delta into a real number.
	if len(tokenDeltas) > 0 {
		// Every row these touch is inside the union the pass at the top of this
		// function guarded, so no cycle can form here: a row that existed then
		// is already row-locked, and a row created since is covered by the
		// advisory key this transaction still holds. Sorted anyway, so the
		// statements read in the same order the locks were taken and a reader
		// is not invited to conclude the order here is what makes it safe -- it
		// is not, and sorting these alone was exactly the incomplete fix.
		tokens := make([]string, 0, len(tokenDeltas))
		for token := range tokenDeltas {
			tokens = append(tokens, token)
		}
		sort.Strings(tokens)

		batch := &pgx.Batch{}
		for _, token := range tokens {
			d := tokenDeltas[token]
			batch.Queue(`
				UPDATE tokens SET
					transfer_count = COALESCE(transfer_count, 0) + $2,
					total_supply = CASE
						WHEN token_type = 'ERC20'
						THEN `+clampSupply(`COALESCE(total_supply, 0) + $3::NUMERIC`)+`
						ELSE total_supply
					END
				WHERE address = $1`, token, d.transferDelta, d.supplyDelta.String())
		}
		br := tx.SendBatch(ctx, batch)
		for _, token := range tokens {
			ct, err := br.Exec()
			if err != nil {
				_ = br.Close()
				return err
			}
			// No tokens row to carry the delta. Benign and self-healing: the
			// row does not exist yet because discovery's metadata fetch has not
			// succeeded, and whichever batch finally creates it seeds it from
			// the stored transfers -- these rows included. Logged rather than
			// dropped silently, because the same zero would otherwise be the
			// only trace of a genuine accounting loss.
			if ct.RowsAffected() == 0 {
				log.Debug("token counter delta found no tokens row; seed on creation will pick it up",
					"token", token, "transfers", tokenDeltas[token].transferDelta)
			}
		}
		if err := br.Close(); err != nil {
			return err
		}
	}

	if len(data.InternalTransactions) > 0 {
		batch := &pgx.Batch{}
		for _, it := range data.InternalTransactions {
			batch.Queue(`
				INSERT INTO internal_transactions (tx_hash, block_number, trace_address, from_address, to_address, value,
					gas, gas_used, input, output, call_type, error, timestamp)
				VALUES ($1, $2, $3, LOWER($4), LOWER($5), $6, $7, $8, $9, $10, $11, $12, $13)
				ON CONFLICT (tx_hash, trace_address) DO NOTHING`,
				it.TxHash, it.BlockNumber, it.TraceAddress, it.From, it.To, it.Value,
				it.Gas, it.GasUsed, it.Input, it.Output, it.CallType, it.Error, it.Timestamp)
		}
		br := tx.SendBatch(ctx, batch)
		if err := br.Close(); err != nil {
			return err
		}
	}

	if len(data.AddressStats) > 0 && !data.SkipAddressStats {
		batch := &pgx.Batch{}
		for _, s := range data.AddressStats {
			batch.Queue(`
				INSERT INTO address_stats (address, tx_count, internal_tx_count, token_transfer_count, first_seen, last_seen, is_contract, updated_at)
				VALUES (LOWER($1), $2, $3, $4, $5, $5, $6, NOW())
				ON CONFLICT (address) DO UPDATE SET
					tx_count = address_stats.tx_count + $2,
					internal_tx_count = address_stats.internal_tx_count + $3,
					token_transfer_count = address_stats.token_transfer_count + $4,
					last_seen = $5,
					is_contract = COALESCE(EXCLUDED.is_contract, address_stats.is_contract),
					updated_at = NOW()
				RETURNING (xmax = 0)`,
				s.Address, s.TxCountDelta, s.InternalTxCountDelta, s.TokenTransferDelta, s.BlockNumber, s.IsContract)
		}
		br := tx.SendBatch(ctx, batch)
		for range data.AddressStats {
			var wasNew bool
			if err := br.QueryRow().Scan(&wasNew); err != nil {
				_ = br.Close()
				return err
			}
			if wasNew {
				deltaAddresses++
			}
		}
		if err := br.Close(); err != nil {
			return err
		}
	}

	if deltaBlocks|deltaTxs|deltaTokens|deltaAddresses|deltaTransfers != 0 {
		_, err := tx.Exec(ctx, `
			INSERT INTO chain_counters (name, count, updated_at) VALUES
				('blocks_total',       $1, NOW()),
				('transactions_total', $2, NOW()),
				('tokens_total',       $3, NOW()),
				('addresses_total',    $4, NOW()),
				('transfers_total',    $5, NOW())
			ON CONFLICT (name) DO UPDATE
				SET count = chain_counters.count + EXCLUDED.count,
					updated_at = NOW()`,
			deltaBlocks, deltaTxs, deltaTokens, deltaAddresses, deltaTransfers)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// RebuildAddressStats rebuilds address_stats after catchup completes.
func (d *DB) RebuildAddressStats(ctx context.Context) error {
	// All address keys are lowercased so duplicate-case rows from past
	// writes (some checksum-cased, some lowercase) collapse to one row per
	// address. Ongoing delta writes already lowercase the key, so this
	// stays consistent going forward.
	//
	// tx_count is the count of distinct tx hashes involving the address
	// either as the EVM from/to OR as a Transfer-event participant in that
	// tx's logs. This matches the inclusive list returned by
	// GetTransactionsByAddress — a recipient who never sent a tx of their
	// own still gets credit for the mint/transfer txs that gave them tokens.
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL work_mem = '%s'", sanitizeWorkMem(d.RebuildWorkMem))); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		TRUNCATE address_stats;

		INSERT INTO address_stats (address, tx_count, internal_tx_count, token_transfer_count, first_seen, last_seen, is_contract, updated_at)
		SELECT
			addr,
			COALESCE(tx_count, 0),
			COALESCE(internal_tx_count, 0),
			COALESCE(token_transfer_count, 0),
			first_seen,
			last_seen,
			COALESCE(is_contract, false),
			NOW()
		FROM (
			SELECT
				a.address as addr,
				tx.tx_count,
				it.internal_tx_count,
				tt.token_transfer_count,
				LEAST(tx.first_seen, it.first_seen, tt.first_seen) as first_seen,
				GREATEST(tx.last_seen, it.last_seen, tt.last_seen) as last_seen,
				c.is_contract
			FROM (
				SELECT LOWER(from_address) as address FROM transactions
				UNION
				SELECT LOWER(to_address) as address FROM transactions WHERE to_address IS NOT NULL
				UNION
				SELECT LOWER(from_address) as address FROM internal_transactions
				UNION
				SELECT LOWER(to_address) as address FROM internal_transactions WHERE to_address IS NOT NULL
				UNION
				SELECT LOWER(from_address) as address FROM token_transfers
				UNION
				SELECT LOWER(to_address) as address FROM token_transfers
			) a
			LEFT JOIN (
				SELECT address, COUNT(DISTINCT tx_hash) as tx_count, MIN(block_number) as first_seen, MAX(block_number) as last_seen
				FROM (
					SELECT LOWER(from_address) as address, hash as tx_hash, block_number FROM transactions
					UNION ALL
					SELECT LOWER(to_address) as address, hash as tx_hash, block_number FROM transactions WHERE to_address IS NOT NULL
					UNION ALL
					SELECT LOWER(tt.from_address) as address, tt.tx_hash, tt.block_number FROM token_transfers tt
					UNION ALL
					SELECT LOWER(tt.to_address) as address, tt.tx_hash, tt.block_number FROM token_transfers tt
				) t
				GROUP BY address
			) tx ON a.address = tx.address
			LEFT JOIN (
				SELECT address, COUNT(*) as internal_tx_count, MIN(block_number) as first_seen, MAX(block_number) as last_seen
				FROM (
					SELECT LOWER(from_address) as address, block_number FROM internal_transactions
					UNION ALL
					SELECT LOWER(to_address) as address, block_number FROM internal_transactions WHERE to_address IS NOT NULL
				) t
				GROUP BY address
			) it ON a.address = it.address
			LEFT JOIN (
				SELECT address, COUNT(*) as token_transfer_count, MIN(block_number) as first_seen, MAX(block_number) as last_seen
				FROM (
					SELECT LOWER(from_address) as address, block_number FROM token_transfers
					UNION ALL
					SELECT LOWER(to_address) as address, block_number FROM token_transfers
				) t
				GROUP BY address
			) tt ON a.address = tt.address
			LEFT JOIN (
				SELECT LOWER(address) as address, true as is_contract FROM contracts
			) c ON a.address = c.address
		) stats
		WHERE addr IS NOT NULL
	`); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (d *DB) InsertBalancesBatch(ctx context.Context, balances []*types.Balance) error {
	if len(balances) == 0 {
		return nil
	}

	// Every token_balances_current upsert takes a row lock on (token, address)
	// and holds it to commit, which the balances insert alone never did: its key
	// carries block_number, so two workers at different blocks touched different
	// rows and never contended. Balance workers each flush their own unordered
	// batch -- 30 of them by default (config.go's balance_workers default, read
	// by indexer.New), 15 as the Tickr deployment sets BALANCE_WORKERS -- so on
	// either count, without a discipline two of them can hold the row the other
	// wants. flushBatch retries a 40P01 three times and then DROPS the
	// batch, so a deadlock costs balances outright, and a missing balance is a
	// wrong holder_count.
	//
	// Two rules make a cycle impossible. Every transaction takes its locks in
	// the same total order, and it takes all of its balances locks before any
	// current lock. Sorting alone is not enough: a batch holding two blocks for
	// one key would take current(k) between balances(k,5) and balances(k,7) and
	// could still cycle with a batch holding only (k,7). With the phases split,
	// a transaction waiting on a current row can only be waiting on one that has
	// finished phase one, and those wait on each other in sorted order.
	//
	// Stable, so rows sharing a key keep arrival order and the last-write-wins
	// tie-break on an identical (address, token, block) resolves as before. The
	// copy is because the caller reuses its slice after this returns.
	ordered := make([]*types.Balance, len(balances))
	copy(ordered, balances)
	sort.SliceStable(ordered, func(i, j int) bool {
		a, b := ordered[i], ordered[j]
		at, bt := strings.ToLower(a.TokenAddress), strings.ToLower(b.TokenAddress)
		if at != bt {
			return at < bt
		}
		aa, ba := strings.ToLower(a.Address), strings.ToLower(b.Address)
		if aa != ba {
			return aa < ba
		}
		return a.BlockNumber < b.BlockNumber
	})

	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	batch := &pgx.Batch{}
	for _, b := range ordered {
		batch.Queue(`
			INSERT INTO balances (address, token_address, block_number, balance)
			VALUES (LOWER($1), LOWER($2), $3, $4)
			ON CONFLICT (address, token_address, block_number) DO UPDATE SET balance = EXCLUDED.balance`,
			b.Address, b.TokenAddress, b.BlockNumber, b.Balance)
	}

	// token_balances_current caches the row that a DISTINCT ON (address) ...
	// ORDER BY block_number DESC over balances would have returned, so the guard
	// has to preserve exactly that: highest block_number wins. >= rather than >
	// because the insert above is last-write-wins for an identical block_number,
	// and the two must not disagree.
	//
	// The guard is also what keeps this correct across transactions: a second
	// transaction touching the same row blocks on the row lock, then re-evaluates
	// this WHERE against the updated row, so the highest block_number survives
	// whatever order the writes arrive in. Plain last-write-wins would have been
	// arrival-order dependent.
	//
	// One statement per distinct key rather than per row. The sort puts a key's
	// rows in one ascending run, so its last element is the row the guard would
	// have settled on anyway -- highest block, latest arrival on a tie -- and
	// collapsing the run takes the key's lock once instead of once per row.
	// Folding the run into a single multi-row INSERT instead is not available:
	// a VALUES list that conflicts with itself fails with "ON CONFLICT DO UPDATE
	// command cannot affect row a second time".
	//
	// block_number is the block that TRIGGERED the read, not the block the
	// balance was read at -- the balance workers read at latest. balances
	// carries the same defect, and this mirrors it rather than diverging from
	// the query it replaces. See PRST-4508.
	for i, b := range ordered {
		if next := i + 1; next < len(ordered) &&
			strings.EqualFold(ordered[next].TokenAddress, b.TokenAddress) &&
			strings.EqualFold(ordered[next].Address, b.Address) {
			continue
		}
		batch.Queue(`
			INSERT INTO token_balances_current (token_address, address, balance, block_number)
			VALUES (LOWER($1), LOWER($2), $3, $4)
			ON CONFLICT (token_address, address) DO UPDATE
				SET balance = EXCLUDED.balance,
					block_number = EXCLUDED.block_number
				WHERE EXCLUDED.block_number >= token_balances_current.block_number`,
			b.TokenAddress, b.Address, b.Balance, b.BlockNumber)
	}

	br := tx.SendBatch(ctx, batch)
	if err := br.Close(); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (d *DB) GetAllTokenAddresses(ctx context.Context) ([]string, error) {
	rows, err := d.pool.Query(ctx, `SELECT address FROM tokens`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var addresses []string
	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			return nil, err
		}
		addresses = append(addresses, addr)
	}
	return addresses, rows.Err()
}
