package db

import (
	"context"
	"fmt"
	"strings"
	"testing"

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

	for _, q := range []string{"", "012", "abc", "0x12", "1234567890123456789"} {
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
