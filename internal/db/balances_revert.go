package db

import (
	"context"
	"math"
	"sort"

	"github.com/jackc/pgx/v5"
)

// balanceRevertLock is the advisory lock a balance revert holds exclusively from before it reads its block's keys to
// its commit, and every balance history write (InsertBalancesBatch) holds shared from before its key locks to its
// commit.
const balanceRevertLock = 4675

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
//
// A flush can write a row at the block between the read of its keys and their locks: a read at the head, which is
// this block, of a key the block did not touch. The keys are read again under the locks until no new one appears. A
// new key is waited for only when its lock comes after every lock held, as a flush takes them; one that comes before
// is taken if free, and otherwise left to the reread alone, so the revert never waits out of order. The re-read finds
// such a key by its history row: every writer of token_balances_current at a head block also writes one, in the same
// transaction (InsertBalancesBatch, from the balance workers' flush and the reread loop). The others do not write at a
// head block: the start's reconcile copies history rows, and the stats backfill (InsertCurrentBalances, current rows
// without history) writes below START_BLOCK. A new writer of current rows without history at the head breaks this.
func revertBalances(ctx context.Context, tx pgx.Tx, number uint64) (revertedBalances, error) {
	out := revertedBalances{holderDelta: map[string]int{}}
	// Before any key: a flush holding the lock shared commits first, so the history delete below sees its rows, and
	// none starts until the revert commits its new epoch, which fences every read decided before it.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, 1)`, balanceRevertLock); err != nil {
		return out, err
	}
	keys, err := blockBalanceKeys(ctx, tx, number)
	if err != nil || len(keys) == 0 {
		return out, err
	}
	var held []int32
	seen := map[balanceKey]bool{}
	hadRow := map[balanceKey]bool{}
	locked := map[balanceKey]bool{}
	for len(keys) > 0 {
		var fresh []balanceKey
		for k, had := range keys {
			hadRow[k] = hadRow[k] || had
			if !seen[k] {
				seen[k] = true
				fresh = append(fresh, k)
			}
		}
		if len(fresh) == 0 {
			break
		}
		got, err := lockBalanceKeys(ctx, tx, fresh, held)
		if err != nil {
			return out, err
		}
		for _, k := range got {
			locked[k] = true
			held = append(held, tbcLockKey(k.token, k.address))
		}
		if keys, err = blockBalanceKeys(ctx, tx, number); err != nil {
			return out, err
		}
	}
	var restore []balanceKey
	for k := range seen {
		out.keys = append(out.keys, k)
		if hadRow[k] && locked[k] {
			restore = append(restore, k)
		}
	}
	sortBalanceKeys(out.keys)
	sortBalanceKeys(restore)

	before, err := positiveBalances(ctx, tx, restore)
	if err != nil {
		return out, err
	}
	if beforeBalanceHistoryDelete != nil {
		beforeBalanceHistoryDelete()
	}
	// A flush can still commit a row at the block, for a key the last read did not see, before the delete: the delete
	// returns its key, which is asked to be read again like the others.
	deleted, err := tx.Query(ctx, `
		DELETE FROM balances WHERE block_number = $1 RETURNING LOWER(token_address), LOWER(address)`, int64(number))
	if err != nil {
		return out, err
	}
	for deleted.Next() {
		var k balanceKey
		if err := deleted.Scan(&k.token, &k.address); err != nil {
			deleted.Close()
			return out, err
		}
		if !seen[k] {
			seen[k] = true
			out.keys = append(out.keys, k)
		}
	}
	deleted.Close()
	if err := deleted.Err(); err != nil {
		return out, err
	}
	if afterBalanceHistoryDelete != nil {
		afterBalanceHistoryDelete()
	}
	sortBalanceKeys(out.keys)
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

// beforeBalanceHistoryDelete, when set, runs just before a revert deletes its block's balance history rows: a test
// commits a flush there, which no read of the revert can see.
var beforeBalanceHistoryDelete func()

// afterBalanceHistoryDelete, when set, runs once a revert has deleted its block's balance history rows: a test starts
// a flush there, which the delete could not see.
var afterBalanceHistoryDelete func()

// afterBalanceRereadsRecorded, when set, runs once a revert has taken its reorg epoch and before it commits: the epoch
// is a sequence, which others see at once, while the revert's rows are not yet theirs.
var afterBalanceRereadsRecorded func()

// blockBalanceKeys reads the keys of a block's balance history rows and of its ERC-20 and ERC-721 transfers' holders,
// each with whether it had a history row at the block.
func blockBalanceKeys(ctx context.Context, tx pgx.Tx, number uint64) (map[balanceKey]bool, error) {
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
		GROUP BY token_address, address`, int64(number))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	keys := map[balanceKey]bool{}
	for rows.Next() {
		var k balanceKey
		var had bool
		if err := rows.Scan(&k.token, &k.address, &had); err != nil {
			return nil, err
		}
		keys[k] = had
	}
	return keys, rows.Err()
}

// lockBalanceKeys takes the balance-key locks of keys, given the locks the transaction holds, and returns the keys
// it holds afterwards. Keys whose lock comes after every held one are waited for, in order, as a flush takes them; a
// key whose lock is held already is held; one whose lock comes before is taken only if it is free.
func lockBalanceKeys(ctx context.Context, tx pgx.Tx, keys []balanceKey, held []int32) ([]balanceKey, error) {
	top, holding := int32(math.MinInt32), map[int32]bool{}
	for _, h := range held {
		top, holding[h] = max(top, h), true
	}
	var got, wait []balanceKey
	var lower []int32
	lowerKeys := map[int32][]balanceKey{}
	for _, k := range keys {
		switch h := tbcLockKey(k.token, k.address); {
		case holding[h]:
			got = append(got, k)
		case len(held) == 0 || h > top:
			wait = append(wait, k)
		default:
			if lowerKeys[h] == nil {
				lower = append(lower, h)
			}
			lowerKeys[h] = append(lowerKeys[h], k)
		}
	}
	if len(wait) > 0 {
		tokens, addresses := balanceKeyColumns(wait)
		if _, err := tx.Exec(ctx, tbcLockKeysSQL, tbcLockKeys(tokens, addresses)); err != nil {
			return nil, err
		}
		got = append(got, wait...)
	}
	if len(lower) == 0 {
		return got, nil
	}
	rows, err := tx.Query(ctx, `SELECT k, pg_try_advisory_xact_lock(4669, k) FROM unnest($1::int4[]) AS k`, lower)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var h int32
		var ok bool
		if err := rows.Scan(&h, &ok); err != nil {
			return nil, err
		}
		if ok {
			got = append(got, lowerKeys[h]...)
		}
	}
	return got, rows.Err()
}

func sortBalanceKeys(keys []balanceKey) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].token != keys[j].token {
			return keys[i].token < keys[j].token
		}
		return keys[i].address < keys[j].address
	})
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
// balance_rereads last too. A revert with no key still takes the epoch: a flush that waited for it then writes no read
// decided before it.
func recordBalanceRereads(ctx context.Context, tx pgx.Tx, keys []balanceKey) error {
	if len(keys) == 0 {
		_, err := tx.Exec(ctx, `SELECT nextval('balance_reorg_epoch')`)
		return err
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

// reopenRereads opens the request to read each key again at epoch or later: a flush's reads of them were decided
// before a revert.
func reopenRereads(ctx context.Context, tx pgx.Tx, keys []balanceKey, epoch int64) error {
	if len(keys) == 0 {
		return nil
	}
	sortBalanceKeys(keys)
	tokens, addresses := balanceKeyColumns(keys)
	_, err := tx.Exec(ctx, `
		INSERT INTO balance_rereads AS r (token_address, address, epoch, open, attempts)
		SELECT k.token_address, k.address, $3, true, 0
		FROM unnest($1::TEXT[], $2::TEXT[]) WITH ORDINALITY AS k(token_address, address, n) ORDER BY k.n
		ON CONFLICT (token_address, address) DO UPDATE SET epoch = GREATEST(r.epoch, EXCLUDED.epoch), open = true, attempts = 0`,
		tokens, addresses, epoch)
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
// out of attempts, which it returns separately (their epoch still drops reads decided before the revert). A request
// handed out carries the latest reorg epoch, or its own if later: its read is decided now, after every revert so far,
// and a balance write drops a read decided before the latest one. The claim holds the balance revert lock shared, as
// its first statement: the epoch is a sequence, which a revert moves before it commits, and a claim between the two
// would stamp a read at the head still being reverted with the revert's epoch.
func (d *DB) ClaimBalanceRereads(ctx context.Context, limit int) (claimed, dropped []BalanceReread, err error) {
	err = pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared($1, 1)`, balanceRevertLock); err != nil {
			return err
		}
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
			RETURNING r.token_address, r.address,
			          GREATEST(r.epoch, (SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM balance_reorg_epoch))`, limit))
		return err
	})
	return claimed, dropped, err
}
