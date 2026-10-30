package db

import (
	"context"
	"sort"

	"github.com/jackc/pgx/v5"
)

// balanceKey is one (token, holder) pair, both lower-case.
type balanceKey struct {
	token, address string
}

// revertedBalances is what revertBalances leaves for the rest of DeleteBlock: every key the reverted block touched,
// sorted, for balance_rereads, and each token's holder_count movement.
type revertedBalances struct {
	keys        []balanceKey
	holderDelta map[string]int
}

// revertBalances takes a reverted block's balances off, in a flush's lock order (balance-key advisory locks, balances
// rows, token_balances_current rows), before the caller takes any tokens row.
//
// The keys are the block's balance history rows and the holders of its ERC-20 and ERC-721 transfers: balances are
// read asynchronously, so a read the block triggered may not have been written yet. The block's history rows are
// deleted. A key that had one is set back to its latest remaining row, or loses its current row when none remains
// (its only row was this block's, or older ones are pruned, or the stats backfill wrote it without history). A key
// without one keeps its current row. None of this is certain: every row was read at the node's latest state, so the
// remaining row may itself be fork-era, and a read queued before the revert can still land after it. The certain value
// is the node's at the new head, which the caller requests for every key (recordBalanceRereads).
func revertBalances(ctx context.Context, tx pgx.Tx, number uint64) (revertedBalances, error) {
	out := revertedBalances{holderDelta: map[string]int{}}
	rows, err := tx.Query(ctx, `
		SELECT token_address, address, bool_or(had_row) FROM (
			SELECT LOWER(token_address) AS token_address, LOWER(address) AS address, true AS had_row
			FROM balances WHERE block_number = $1
			UNION ALL
			SELECT LOWER(t.token_address), LOWER(x.address), false
			FROM token_transfers t CROSS JOIN LATERAL (VALUES (t.from_address), (t.to_address)) AS x(address)
			WHERE t.block_number = $1 AND t.token_type IN ('ERC20', 'ERC721') AND x.address IS NOT NULL
			  AND LOWER(x.address) <> '`+zeroAddress+`'
		) k
		GROUP BY token_address, address
		ORDER BY token_address, address`, int64(number))
	if err != nil {
		return out, err
	}
	var restore []balanceKey
	for rows.Next() {
		var k balanceKey
		var hadRow bool
		if err := rows.Scan(&k.token, &k.address, &hadRow); err != nil {
			rows.Close()
			return out, err
		}
		out.keys = append(out.keys, k)
		if hadRow {
			restore = append(restore, k)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, err
	}
	if len(out.keys) == 0 {
		return out, nil
	}

	tokens, addresses := balanceKeyColumns(out.keys)
	if _, err := tx.Exec(ctx, tbcLockKeysSQL, tbcLockKeys(tokens, addresses)); err != nil {
		return out, err
	}
	before, err := positiveBalances(ctx, tx, restore)
	if err != nil {
		return out, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM balances WHERE block_number = $1`, int64(number)); err != nil {
		return out, err
	}
	if len(restore) == 0 {
		return out, nil
	}
	rt, ra := balanceKeyColumns(restore)
	if _, err := tx.Exec(ctx, `
		WITH k AS (SELECT * FROM unnest($1::TEXT[], $2::TEXT[]) AS k(token_address, address)),
		prev AS (
			SELECT DISTINCT ON (b.token_address, b.address) b.token_address, b.address, b.balance, b.block_number
			FROM k JOIN balances b ON b.token_address = k.token_address AND b.address = k.address
			ORDER BY b.token_address, b.address, b.block_number DESC
		),
		gone AS (
			DELETE FROM token_balances_current c USING k
			WHERE c.token_address = k.token_address AND c.address = k.address
			  AND NOT EXISTS (SELECT 1 FROM prev p WHERE p.token_address = k.token_address AND p.address = k.address)
		)
		INSERT INTO token_balances_current AS c (token_address, address, balance, block_number)
		SELECT token_address, address, balance, block_number FROM prev
		ON CONFLICT (token_address, address) DO UPDATE SET balance = EXCLUDED.balance, block_number = EXCLUDED.block_number`,
		rt, ra); err != nil {
		return out, err
	}
	after, err := positiveBalances(ctx, tx, restore)
	if err != nil {
		return out, err
	}
	for _, k := range restore {
		if d := boolInt(after[k]) - boolInt(before[k]); d != 0 {
			out.holderDelta[k.token] += d
		}
	}
	return out, nil
}

// positiveBalances reports which keys hold a positive current balance.
func positiveBalances(ctx context.Context, tx pgx.Tx, keys []balanceKey) (map[balanceKey]bool, error) {
	pos := make(map[balanceKey]bool, len(keys))
	if len(keys) == 0 {
		return pos, nil
	}
	tokens, addresses := balanceKeyColumns(keys)
	rows, err := tx.Query(ctx, tbcOldPosSQL, tokens, addresses)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var k balanceKey
		var p bool
		if err := rows.Scan(&k.token, &k.address, &p); err != nil {
			return nil, err
		}
		pos[k] = p
	}
	return pos, rows.Err()
}

// recordBalanceRereads takes a new reorg epoch, records it on every key and opens each key's request to be read again
// at the new head. It is DeleteBlock's last write, after every row lock a balance flush takes, which takes
// balance_rereads last too.
func recordBalanceRereads(ctx context.Context, tx pgx.Tx, keys []balanceKey) error {
	if len(keys) == 0 {
		return nil
	}
	tokens, addresses := balanceKeyColumns(keys)
	_, err := tx.Exec(ctx, `
		WITH e AS (SELECT nextval('balance_reorg_epoch') AS epoch)
		INSERT INTO balance_rereads AS r (token_address, address, epoch, open, attempts)
		SELECT k.token_address, k.address, e.epoch, true, 0
		FROM unnest($1::TEXT[], $2::TEXT[]) WITH ORDINALITY AS k(token_address, address, n), e ORDER BY k.n
		ON CONFLICT (token_address, address) DO UPDATE SET epoch = EXCLUDED.epoch, open = true, attempts = 0`,
		tokens, addresses)
	return err
}

// CurrentBalanceEpoch is the epoch of the latest reorg revert, 0 before the first. A balance read decided now carries
// it.
func (d *DB) CurrentBalanceEpoch(ctx context.Context) (int64, error) {
	var e int64
	err := d.pool.QueryRow(ctx, `SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM balance_reorg_epoch`).Scan(&e)
	return e, err
}

func balanceKeyColumns(keys []balanceKey) (tokens, addresses []string) {
	tokens, addresses = make([]string, len(keys)), make([]string, len(keys))
	for i, k := range keys {
		tokens[i], addresses[i] = k.token, k.address
	}
	return tokens, addresses
}

func sortedTokens(set map[string]bool) []string {
	tokens := make([]string, 0, len(set))
	for t := range set {
		tokens = append(tokens, t)
	}
	sort.Strings(tokens)
	return tokens
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// revertEpoch is a key's latest reorg epoch and whether its request to be read again is still open.
type revertEpoch struct {
	epoch int64
	open  bool
}

// revertEpochs returns the reorg epoch of each key a revert has touched.
func revertEpochs(ctx context.Context, tx pgx.Tx, tokens, addresses []string) (map[balanceKey]revertEpoch, error) {
	rows, err := tx.Query(ctx, `
		SELECT r.token_address, r.address, r.epoch, r.open FROM balance_rereads r
		JOIN unnest($1::TEXT[], $2::TEXT[]) AS k(token_address, address)
		  ON r.token_address = k.token_address AND r.address = k.address`, tokens, addresses)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[balanceKey]revertEpoch{}
	for rows.Next() {
		var k balanceKey
		var e revertEpoch
		if err := rows.Scan(&k.token, &k.address, &e.epoch, &e.open); err != nil {
			return nil, err
		}
		out[k] = e
	}
	return out, rows.Err()
}

// closeRereads closes the requests a flush answered, in key order. The rows stay: their epoch keeps dropping reads
// decided before the revert.
func closeRereads(ctx context.Context, tx pgx.Tx, keys []balanceKey) error {
	if len(keys) == 0 {
		return nil
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].token != keys[j].token {
			return keys[i].token < keys[j].token
		}
		return keys[i].address < keys[j].address
	})
	tokens, addresses := balanceKeyColumns(keys)
	_, err := tx.Exec(ctx, `
		UPDATE balance_rereads r SET open = false FROM unnest($1::TEXT[], $2::TEXT[]) AS k(token_address, address)
		WHERE r.token_address = k.token_address AND r.address = k.address`, tokens, addresses)
	return err
}

// BalanceReread is one open request: read the holder's balance of the token again at the head, at Epoch or later.
type BalanceReread struct {
	Token, Address string
	Epoch          int64
}

// MaxBalanceRereadAttempts is how many times a request is handed out before it is dropped: a token whose
// balanceOf(holder) never answers would otherwise be asked about forever.
const MaxBalanceRereadAttempts = 20

// ClaimBalanceRereads hands out up to limit open requests, counting an attempt on each, and closes the ones that ran
// out of attempts, which it returns separately (their epoch still drops reads decided before the revert).
func (d *DB) ClaimBalanceRereads(ctx context.Context, limit int) (claimed, dropped []BalanceReread, err error) {
	err = pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		collect := func(rows pgx.Rows, err error) ([]BalanceReread, error) {
			if err != nil {
				return nil, err
			}
			return pgx.CollectRows(rows, func(r pgx.CollectableRow) (BalanceReread, error) {
				var b BalanceReread
				return b, r.Scan(&b.Token, &b.Address, &b.Epoch)
			})
		}
		if dropped, err = collect(tx.Query(ctx, `
			UPDATE balance_rereads SET open = false WHERE open AND attempts >= $1
			RETURNING token_address, address, epoch`, MaxBalanceRereadAttempts)); err != nil {
			return err
		}
		claimed, err = collect(tx.Query(ctx, `
			UPDATE balance_rereads r SET attempts = r.attempts + 1
			FROM (SELECT token_address, address FROM balance_rereads WHERE open
			      ORDER BY epoch LIMIT $1 FOR UPDATE SKIP LOCKED) o
			WHERE r.token_address = o.token_address AND r.address = o.address
			RETURNING r.token_address, r.address, r.epoch`, limit))
		return err
	})
	return claimed, dropped, err
}
