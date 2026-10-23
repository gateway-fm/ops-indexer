package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// With history partitioned, the tx_hash index lives on the parent: the legacy partition keeps the index an
// earlier startup built there instead of getting a second one, and every partition created later has one.
func TestTxHashIndexesCoverEveryHistoryPartition(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	_, err := d.pool.Exec(ctx, `CREATE INDEX ON logs_legacy (tx_hash)`)
	require.NoError(t, err)

	everyPartitionIndexed := func(stage string, minPartitions int) {
		t.Helper()
		for _, table := range txHashLookupTables {
			rows, err := d.pool.Query(ctx, `
				SELECT c.relname, (SELECT count(*) FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
				                   WHERE i.indrelid = c.oid AND i.indnatts = 1 AND a.attname = 'tx_hash')
				FROM pg_inherits h JOIN pg_class c ON c.oid = h.inhrelid
				WHERE h.inhparent = $1::regclass`, table)
			require.NoError(t, err)
			indexes := map[string]int{}
			for rows.Next() {
				var name string
				var n int
				if err := rows.Scan(&name, &n); err != nil {
					rows.Close()
					t.Fatal(err)
				}
				indexes[name] = n
			}
			rows.Close()
			require.NoError(t, rows.Err())
			for name, n := range indexes {
				require.Equal(t, 1, n, "%s %s: tx_hash indexes", stage, name)
			}
			require.GreaterOrEqual(t, len(indexes), minPartitions, "%s %s: partitions", stage, table)
		}
	}

	require.NoError(t, d.EnsureTxHashIndexes(ctx))
	everyPartitionIndexed("before the cut", 1)
	cfg := RetentionConfig{RetainBlocks: 20_000, PartitionBlocks: 2_000}
	require.NoError(t, d.retentionPass(ctx, cfg))
	ensurePartitions(t, d, 10_000, cfg)
	everyPartitionIndexed("after the cut", 3)
}
