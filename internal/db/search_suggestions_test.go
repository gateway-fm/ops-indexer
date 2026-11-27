package db

import (
	"context"
	"fmt"
	"strings"
	"testing"

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

// An ICU collation in another schema that is merely named "C" must not pass for
// pg_catalog."C": the lookup has to find the Danish-ordered row.
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
	_, err = d.pool.Exec(ctx, `INSERT INTO address_stats (address, tx_count) VALUES ($1, 1)`, target)
	require.NoError(t, err)

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

// The transaction prefix lookup must read the hash matches, not walk the
// block-number index filtering on hash: with the matches in old blocks that
// walk reads almost every row.
func TestTxPrefixQueryReadsOnlyMatches(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()

	_, err := d.pool.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		SELECT g, '0xblock' || g, '0xblock' || (g - 1), 1700000000 + g, 0, 30000000, 10 FROM generate_series(1, 20000) g`)
	require.NoError(t, err)
	// 200k transactions. Blocks 1-2000 (10%, the oldest) hold hashes starting
	// 0xabc; the rest start 0x0. A large, old match set is what makes the planner
	// prefer walking the block index for ORDER BY block_number DESC LIMIT n.
	_, err = d.pool.Exec(ctx, `
		INSERT INTO transactions (hash, block_number, tx_index, from_address, to_address, value, gas_used, gas_price, input_data, status, tx_type)
		SELECT CASE WHEN b <= 2000 THEN '0xabc' || substr(md5(b || ':' || i) || md5(i || ':' || b), 1, 61)
		            ELSE '0x0' || substr(md5(b || ':' || i) || md5(i || ':' || b), 1, 63) END,
		       b, i, '0x' || repeat('1', 40), '0x' || repeat('2', 40), '0', 0, 0, '0x', 1, 0
		FROM generate_series(1, 20000) b, generate_series(0, 9) i`)
	require.NoError(t, err)
	_, err = d.pool.Exec(ctx, `ANALYZE transactions`)
	require.NoError(t, err)

	removed := func(q string) int {
		t.Helper()
		var plan string
		require.NoError(t, d.pool.QueryRow(ctx, `EXPLAIN (ANALYZE, FORMAT JSON) `+q, "0xabc", "0xabcg", 10).Scan(&plan))
		total := 0
		for _, part := range strings.Split(plan, `"Rows Removed by Filter": `)[1:] {
			n := 0
			fmt.Sscanf(part, "%d", &n)
			total += n
		}
		return total
	}
	require.Less(t, removed(txPrefixQuery(true)), 1000)

	got, err := d.SearchSuggestions(ctx, "0xabc", 10)
	require.NoError(t, err)
	n := 0
	for _, s := range got {
		if s.Type == "transaction" {
			require.True(t, strings.HasPrefix(s.Value, "0xabc"), s.Value)
			n++
		}
	}
	require.Equal(t, 10, n)
}
