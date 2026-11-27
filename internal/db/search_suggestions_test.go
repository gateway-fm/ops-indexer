package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestHexPrefixRange(t *testing.T) {
	cases := []struct {
		q      string
		max    int
		lo, hi string
		ok     bool
	}{
		{"0x071E11", 40, "0x071e11", "0x071e11g", true},
		{"0X071e11", 40, "0x071e11", "0x071e11g", true},
		{"0x07", 40, "", "", false},                         // too short
		{"0x" + strings.Repeat("a", 41), 40, "", "", false}, // longer than an address
		{"0x07%", 40, "", "", false},                        // LIKE wildcard is not hex
		{"0x07_e", 40, "", "", false},
		{"071e11", 40, "", "", false}, // no 0x
		{"Tick", 40, "", "", false},
	}
	for _, c := range cases {
		lo, hi, ok := hexPrefixRange(c.q, c.max)
		require.Equal(t, c.ok, ok, c.q)
		require.Equal(t, c.lo, lo, c.q)
		require.Equal(t, c.hi, hi, c.q)
	}
}

func TestBlockPrefixRanges(t *testing.T) {
	los, his := blockPrefixRanges("18")
	require.Equal(t, []int64{18, 180, 1800}, los[:3])
	require.Equal(t, []int64{19, 190, 1900}, his[:3])
	require.LessOrEqual(t, los[len(los)-1], int64(1)<<53)

	los, his = blockPrefixRanges("0")
	require.Equal(t, []int64{0}, los)
	require.Equal(t, []int64{1}, his)

	for _, q := range []string{"", "012", "abc", "0x12", "1234567890123456789", "+15", "-15", " 15", "1_5"} {
		los, _ = blockPrefixRanges(q)
		require.Empty(t, los, q)
	}
}

func TestSearchSuggestions(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	_, err := d.pool.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		SELECT g, '0xblock' || g, '0xblock' || (g - 1), 1700000000 + g, 0, 30000000, 1 FROM generate_series(1, 2000) g`)
	require.NoError(t, err)

	target := "0x071e11a4403dc38a35b600c6775766f6df404c30"
	txHash := "0x071e11" + strings.Repeat("ab", 29)
	_, err = d.pool.Exec(ctx, `
		INSERT INTO transactions (hash, block_number, tx_index, from_address, to_address, value, gas_used, gas_price, input_data, status, tx_type)
		VALUES ($1, 1500, 0, $2, $2, '0', 0, 0, '0x', 1, 0),
		       ('0x071f' || repeat('cd', 30), 1501, 0, $2, $2, '0', 0, 0, '0x', 1, 0)`, txHash, target)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `
		INSERT INTO address_stats (address, tx_count) VALUES ($1, 57), ('0x071e12' || repeat('0', 34), 3), ('0x071f' || repeat('0', 36), 9)`, target)
	require.NoError(t, err)

	byType := func(q string) map[string][]string {
		t.Helper()
		got, err := d.SearchSuggestions(ctx, q, 10)
		require.NoError(t, err, q)
		m := map[string][]string{}
		for _, s := range got {
			m[s.Type] = append(m[s.Type], s.Value)
		}
		return m
	}

	// A checksummed (mixed-case) prefix finds the lowercase-stored rows.
	got := byType("0x071E11a44")
	require.Equal(t, []string{target}, got["address"])
	require.Empty(t, got["transaction"], "tx hash diverges after 0x071e11")

	got = byType("0x071e1")
	require.Equal(t, []string{target, "0x071e12" + strings.Repeat("0", 34)}, got["address"], "ordered by tx_count")
	require.Equal(t, []string{txHash}, got["transaction"])

	// The full address is an exact hit; the full hash is found as a transaction.
	require.Equal(t, []string{target}, byType(target)["address"])
	require.Equal(t, []string{txHash}, byType(txHash)["transaction"])

	// Wildcards are rejected rather than matching everything.
	require.Empty(t, byType("0x0%"))

	// Block prefixes: newest first across every digit length.
	got = byType("15")
	require.Equal(t, "1599", got["block"][0])
	for _, v := range got["block"] {
		require.True(t, strings.HasPrefix(v, "15"), v)
	}
	require.Equal(t, []string{"2000"}, byType("2000")["block"])
	require.Equal(t, fmt.Sprint(799), byType("7")["block"][0], "longest matching numbers come first")
}

// A collation that does not order hex bytewise (ICU Danish sorts "aa" as "å",
// after "f") must not make prefix lookups miss rows: the lookup falls back to a
// "C"-collated comparison.
func TestSearchSuggestionsNonBytewiseCollation(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	var has bool
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_collation WHERE collname = 'da-x-icu')`).Scan(&has))
	if !has {
		t.Skip("server has no ICU da collation")
	}
	_, err := d.pool.Exec(ctx, `ALTER TABLE address_stats ALTER COLUMN address TYPE text COLLATE "da-x-icu"`)
	require.NoError(t, err)

	target := "0x12aa" + strings.Repeat("0", 36)
	_, err = d.pool.Exec(ctx, `INSERT INTO address_stats (address, tx_count) VALUES ($1, 1)`, target)
	require.NoError(t, err)

	// The bytewise range [0x12a, 0x12ag) misses it under this collation.
	var inRange int
	require.NoError(t, d.pool.QueryRow(ctx, `SELECT count(*) FROM address_stats WHERE address >= '0x12a' AND address < '0x12ag'`).Scan(&inRange))
	require.Zero(t, inRange, "fixture no longer reproduces the collation gap")

	c, err := d.hexCollation(ctx, "address_stats", "address")
	require.NoError(t, err)
	require.False(t, c.bytewise())

	got, err := d.SearchSuggestions(ctx, "0x12a", 10)
	require.NoError(t, err)
	var addrs []string
	for _, s := range got {
		if s.Type == "address" {
			addrs = append(addrs, s.Value)
		}
	}
	require.Equal(t, []string{target}, addrs)
}

// suggestionsByType runs SearchSuggestions and groups the values by type.
func suggestionsByType(t *testing.T, d *DB, q string) map[string][]string {
	t.Helper()
	got, err := d.SearchSuggestions(context.Background(), q, 10)
	require.NoError(t, err, q)
	m := map[string][]string{}
	for _, s := range got {
		m[s.Type] = append(m[s.Type], s.Value)
	}
	return m
}

// An ICU collation in another schema that is merely named "C" must not pass for
// pg_catalog."C", neither in the catalog check nor in the fallback's COLLATE
// when that schema comes before pg_catalog on search_path: the lookup has to
// find the rows that the Danish ordering puts outside the range.
func TestSearchSuggestionsCollationNamedC(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	_, err := d.pool.Exec(ctx, `
		CREATE SCHEMA spoof;
		CREATE COLLATION spoof."C" (provider = icu, locale = 'da');
		ALTER TABLE address_stats ALTER COLUMN address TYPE text COLLATE spoof."C"`)
	require.NoError(t, err)
	target := "0x12aa" + strings.Repeat("0", 36)
	txHash := "0x12aa" + strings.Repeat("0", 60)
	insertBlock(t, d, 1)
	_, err = d.pool.Exec(ctx, `INSERT INTO address_stats (address, tx_count) VALUES ($1, 1)`, target)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `
		INSERT INTO transactions (hash, block_number, tx_index, from_address, to_address, value, gas_used, gas_price, input_data, status, tx_type)
		VALUES ($1, 1, 0, $2, $2, '0', 0, 0, '0x', 1, 0)`, txHash, target)
	require.NoError(t, err)

	got := suggestionsByType(t, d, "0x12a")
	require.Equal(t, []string{target}, got["address"])
	require.Equal(t, []string{txHash}, got["transaction"])

	cfg := d.pool.Config()
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = "spoof, pg_catalog, public"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	defer pool.Close()
	got = suggestionsByType(t, &DB{pool: pool}, "0x12a")
	require.Equal(t, []string{target}, got["address"], "spoof.\"C\" ahead of pg_catalog on search_path")
	require.Equal(t, []string{txHash}, got["transaction"], "spoof.\"C\" ahead of pg_catalog on search_path")
}

// The bytewise fast path, which C-collated deployments take: both columns
// classify as bytewise and the range finds the rows, including one just inside
// the prefix boundary, for a short (unfenced) and a long (fenced) prefix.
func TestSearchSuggestionsBytewise(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	_, err := d.pool.Exec(ctx, `
		ALTER TABLE transactions ALTER COLUMN hash TYPE text COLLATE "C";
		ALTER TABLE address_stats ALTER COLUMN address TYPE text COLLATE "C"`)
	require.NoError(t, err)
	require.True(t, d.txHashOrderIsBytewise(ctx))
	require.True(t, d.addressOrderIsBytewise(ctx))

	for b := uint64(1); b <= 3; b++ {
		insertBlock(t, d, b)
	}
	from := "0x" + strings.Repeat("1", 40)
	tx := func(prefix string) string { return prefix + strings.Repeat("0", 66-len(prefix)) }
	addr := func(prefix string) string { return prefix + strings.Repeat("0", 42-len(prefix)) }
	_, err = d.pool.Exec(ctx, `
		INSERT INTO transactions (hash, block_number, tx_index, from_address, to_address, value, gas_used, gas_price, input_data, status, tx_type)
		VALUES ($1, 1, 0, $5, $5, '0', 0, 0, '0x', 1, 0), ($2, 2, 0, $5, $5, '0', 0, 0, '0x', 1, 0),
		       ($3, 3, 0, $5, $5, '0', 0, 0, '0x', 1, 0), ($4, 3, 1, $5, $5, '0', 0, 0, '0x', 1, 0)`,
		tx("0x12aa"), tx("0x12af"), tx("0x12b"), tx("0x129f"), from)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `INSERT INTO address_stats (address, tx_count) VALUES ($1, 5), ($2, 7), ($3, 9), ($4, 11)`,
		addr("0x12aa"), addr("0x12af"), addr("0x12b"), addr("0x129f"))
	require.NoError(t, err)

	got := suggestionsByType(t, d, "0x12A")
	require.Equal(t, []string{tx("0x12af"), tx("0x12aa")}, got["transaction"], "newest first")
	require.Equal(t, []string{addr("0x12af"), addr("0x12aa")}, got["address"], "by tx_count")

	got = suggestionsByType(t, d, "0x12aa000")
	require.Equal(t, []string{tx("0x12aa")}, got["transaction"])
	require.Equal(t, []string{addr("0x12aa")}, got["address"])
}

func TestBytewiseCollation(t *testing.T) {
	cases := []struct {
		name string
		c    hexCollation
		want bool
	}{
		{"libc C", hexCollation{provider: "c", collate: "C"}, true},
		{"libc POSIX", hexCollation{provider: "c", collate: "POSIX"}, true},
		{"libc en_US", hexCollation{provider: "c", collate: "en_US.utf8"}, false},
		{"libc C.UTF-8", hexCollation{provider: "c", collate: "C.UTF-8"}, false},
		{"ICU", hexCollation{provider: "i", locale: "da"}, false},
		{"ICU named C", hexCollation{provider: "i", locale: "C"}, false},
		{"builtin C", hexCollation{provider: "b", locale: "C"}, true},
		{"builtin C.UTF-8", hexCollation{provider: "b", locale: "C.UTF-8"}, false},
		{"default, C database", hexCollation{isDefault: true, provider: "d", dbProvider: "c", dbCollate: "C"}, true},
		{"default, POSIX database", hexCollation{isDefault: true, provider: "d", dbProvider: "c", dbCollate: "POSIX"}, true},
		{"default, en_US database", hexCollation{isDefault: true, provider: "d", dbProvider: "c", dbCollate: "en_US.utf8"}, false},
		{"default, ICU database with lc_collate C", hexCollation{isDefault: true, provider: "d", dbProvider: "i", dbCollate: "C", dbLocale: "und"}, false},
		{"default, builtin C database", hexCollation{isDefault: true, provider: "d", dbProvider: "b", dbCollate: "C", dbLocale: "C"}, true},
		{"default, builtin C.UTF-8 database", hexCollation{isDefault: true, provider: "d", dbProvider: "b", dbCollate: "C", dbLocale: "C.UTF-8"}, false},
		{"no collation", hexCollation{provider: "c"}, false},
	}
	for _, c := range cases {
		require.Equal(t, c.want, c.c.bytewise(), c.name)
	}
}

// The catalog lookup classifies each kind of column collation, including the
// database default under a C, a non-C and an ICU database.
func TestHexCollationFromCatalog(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	_, err := d.pool.Exec(ctx, `
		CREATE SCHEMA spoof;
		CREATE COLLATION spoof."C" (provider = icu, locale = 'da');
		CREATE TABLE colls (c text COLLATE "C", posix text COLLATE "POSIX", icu text COLLATE "und-x-icu",
			spoofed text COLLATE spoof."C", dflt text)`)
	require.NoError(t, err)
	bytewise := func(d *DB, table, col string) bool {
		t.Helper()
		c, err := d.hexCollation(ctx, table, col)
		require.NoError(t, err, col)
		return c.bytewise()
	}
	require.True(t, bytewise(d, "colls", "c"))
	require.True(t, bytewise(d, "colls", "posix"))
	require.False(t, bytewise(d, "colls", "icu"))
	require.False(t, bytewise(d, "colls", "spoofed"))
	// The test server's default locale is en_US.utf8.
	require.False(t, bytewise(d, "colls", "dflt"))
	require.False(t, bytewise(d, "transactions", "hash"))

	_, err = d.hexCollation(ctx, "no_such_table", "x")
	require.Error(t, err)

	for _, c := range []struct {
		opts string
		want bool
	}{
		{"LC_COLLATE 'C' LC_CTYPE 'C'", true},
		{"LC_COLLATE 'POSIX' LC_CTYPE 'POSIX'", true},
		{"LC_COLLATE 'en_US.utf8' LC_CTYPE 'en_US.utf8'", false},
		{"LOCALE_PROVIDER icu ICU_LOCALE 'und' LC_COLLATE 'C' LC_CTYPE 'C'", false},
	} {
		ld, drop := localeDB(t, c.opts)
		_, err := ld.pool.Exec(ctx, `CREATE TABLE address_stats (address text PRIMARY KEY)`)
		require.NoError(t, err)
		require.Equal(t, c.want, bytewise(ld, "address_stats", "address"), c.opts)
		drop()
	}
}

// localeDB creates an empty database from template0 with the given locale
// options. setupTestDB must have started the shared server.
func localeDB(t *testing.T, opts string) (*DB, func()) {
	t.Helper()
	ctx := context.Background()
	name := fmt.Sprintf("locale_%d", testDBSeq.Add(1))
	admin := *sharedPGURL
	admin.Path = "/postgres"
	conn, err := pgx.Connect(ctx, admin.String())
	require.NoError(t, err)
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "CREATE DATABASE "+name+" TEMPLATE template0 "+opts)
	require.NoError(t, err)
	own := *sharedPGURL
	own.Path = "/" + name
	pool, err := pgxpool.New(ctx, own.String())
	require.NoError(t, err)
	return &DB{pool: pool}, func() {
		pool.Close()
		conn, err := pgx.Connect(ctx, admin.String())
		require.NoError(t, err)
		defer conn.Close(ctx)
		_, err = conn.Exec(ctx, "DROP DATABASE "+name+" WITH (FORCE)")
		require.NoError(t, err)
	}
}

// A failed check is not cached: the next call checks again. Each column is
// classified on its own, so one failing leaves the other's fast path.
func TestHexOrderRetriesAfterError(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	_, err := d.pool.Exec(ctx, `
		ALTER TABLE transactions ALTER COLUMN hash TYPE text COLLATE "C";
		ALTER TABLE address_stats ALTER COLUMN address TYPE text COLLATE "C";
		ALTER TABLE address_stats RENAME TO address_stats_away`)
	require.NoError(t, err)

	require.False(t, d.addressOrderIsBytewise(ctx), "the check fails while the table is missing")
	require.True(t, d.txHashOrderIsBytewise(ctx), "the hash column is classified on its own")

	_, err = d.pool.Exec(ctx, `ALTER TABLE address_stats_away RENAME TO address_stats`)
	require.NoError(t, err)
	require.True(t, d.addressOrderIsBytewise(ctx), "the failed check is retried")

	// A successful classification is cached.
	_, err = d.pool.Exec(ctx, `ALTER TABLE address_stats ALTER COLUMN address TYPE text COLLATE "und-x-icu"`)
	require.NoError(t, err)
	require.True(t, d.addressOrderIsBytewise(ctx))
}

// seedPrefixTxs inserts 20,000 blocks of 10 transactions. The blocks matching
// the SQL condition on b hold hashes starting with prefix; the rest start 0x0.
func seedPrefixTxs(t *testing.T, d *DB, matching, prefix string) {
	t.Helper()
	ctx := context.Background()
	_, err := d.pool.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		SELECT g, '0xblock' || g, '0xblock' || (g - 1), 1700000000 + g, 0, 30000000, 10 FROM generate_series(1, 20000) g`)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `
		INSERT INTO transactions (hash, block_number, tx_index, from_address, to_address, value, gas_used, gas_price, input_data, status, tx_type)
		SELECT CASE WHEN `+matching+` THEN $1 || substr(md5(b || ':' || i) || md5(i || ':' || b), 1, 64 - length($1) + 2)
		            ELSE '0x0' || substr(md5(b || ':' || i) || md5(i || ':' || b), 1, 63) END,
		       b, i, '0x' || repeat('1', 40), '0x' || repeat('2', 40), '0', 0, 0, '0x', 1, 0
		FROM generate_series(1, 20000) b, generate_series(0, 9) i`, prefix)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `ANALYZE transactions`)
	require.NoError(t, err)
}

// prefixPlan runs q for prefix under EXPLAIN ANALYZE and returns the rows its
// filters discarded and the plan.
func prefixPlan(t *testing.T, d *DB, q, prefix string) (removed int, plan string) {
	t.Helper()
	require.NoError(t, d.pool.QueryRow(context.Background(), `EXPLAIN (ANALYZE, FORMAT JSON) `+q, prefix, prefix+"g", 10).Scan(&plan))
	for _, part := range strings.Split(plan, `"Rows Removed by Filter": `)[1:] {
		n := 0
		fmt.Sscanf(part, "%d", &n)
		removed += n
	}
	return removed, plan
}

// prefixBlocks runs q for prefix and returns the block of each hash it
// returns, checking that every hash has the prefix.
func prefixBlocks(t *testing.T, d *DB, q, prefix string) []int64 {
	t.Helper()
	ctx := context.Background()
	rows, err := d.pool.Query(ctx, q, prefix, prefix+"g", 10)
	require.NoError(t, err)
	hashes, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	blocks := make([]int64, len(hashes))
	for i, h := range hashes {
		require.True(t, strings.HasPrefix(h, prefix), h)
		require.NoError(t, d.pool.QueryRow(ctx, `SELECT block_number FROM transactions WHERE hash = $1`, h).Scan(&blocks[i]))
	}
	return blocks
}

func TestTxPrefixQueryShape(t *testing.T) {
	require.Contains(t, txPrefixQuery(true, minFencedHexPrefix), "MATERIALIZED")
	require.Contains(t, txPrefixQuery(true, 64), "MATERIALIZED")
	require.NotContains(t, txPrefixQuery(true, minFencedHexPrefix-1), "MATERIALIZED")
	require.NotContains(t, txPrefixQuery(false, minFencedHexPrefix), "MATERIALIZED")
	require.NotContains(t, txPrefixQuery(false, 64), "MATERIALIZED")
	require.Contains(t, txPrefixQuery(false, 3), `COLLATE pg_catalog."C"`)
}

// A long prefix on the bytewise path must read the hash matches, not walk the
// block-number index filtering on hash: with the matches in old blocks that
// walk reads almost every row.
func TestTxPrefixQueryReadsOnlyMatches(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	// Blocks 1-2000 (10%, the oldest) hold hashes starting 0xabcde. A large, old
	// match set is what makes the planner prefer walking the block index for
	// ORDER BY block_number DESC LIMIT n.
	seedPrefixTxs(t, d, "b <= 2000", "0xabcde")
	_, err := d.pool.Exec(ctx, `ALTER TABLE transactions ALTER COLUMN hash TYPE text COLLATE "C"`)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `ANALYZE transactions`)
	require.NoError(t, err)

	q := txPrefixQuery(true, 5)
	removed, _ := prefixPlan(t, d, q, "0xabcde")
	require.Less(t, removed, 1000)
	require.Equal(t, []int64{2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000}, prefixBlocks(t, d, q, "0xabcde"),
		"the newest ten matches")

	got := suggestionsByType(t, d, "0xabcde")
	require.Len(t, got["transaction"], 10)
	for _, h := range got["transaction"] {
		require.True(t, strings.HasPrefix(h, "0xabcde"), h)
	}
}

// The non-bytewise fallback has no index on the hash range, so it must walk the
// block-number index newest first and stop at the limit, not collect every
// match (a full scan) before ordering.
func TestTxPrefixFallbackStopsEarly(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()

	// The newest 2000 blocks hold hashes starting 0xabc.
	seedPrefixTxs(t, d, "b > 18000", "0xabc")

	q := txPrefixQuery(false, 3)
	removed, plan := prefixPlan(t, d, q, "0xabc")
	require.NotContains(t, plan, `"Seq Scan"`)
	require.Less(t, removed, 1000)
	require.Equal(t, []int64{20000, 20000, 20000, 20000, 20000, 20000, 20000, 20000, 20000, 20000}, prefixBlocks(t, d, q, "0xabc"))
}

// While the collation check keeps failing, concurrent lookups each probe at
// once rather than queueing on one another, and the failure is logged once,
// not per lookup.
func TestHexOrderProbesDoNotSerialize(t *testing.T) {
	var buf bytes.Buffer
	prev := log.Default()
	log.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer log.SetDefault(prev)

	const n = 8
	var d DB
	var entered atomic.Int32
	all := make(chan struct{})
	d.addressOrder.probe = func(ctx context.Context) (hexCollation, error) {
		if entered.Add(1) == n {
			close(all)
		}
		select {
		case <-all:
		case <-time.After(5 * time.Second):
		}
		return hexCollation{}, errors.New("injected")
	}
	start := time.Now()
	var wg sync.WaitGroup
	results := make([]bool, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = d.addressOrderIsBytewise(context.Background())
		}()
	}
	wg.Wait()
	require.Less(t, time.Since(start), 4*time.Second, "the probes ran one at a time")
	require.EqualValues(t, n, entered.Load())
	require.Equal(t, make([]bool, n), results)
	require.Equal(t, 1, strings.Count(buf.String(), "could not check the collation"), buf.String())
	require.Contains(t, buf.String(), "error=injected")
}
