package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHighestBlockAtOrBelow(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	for _, n := range []uint64{1, 2, 3, 4, 5, 8} {
		insertBlock(t, d, n)
	}
	for at, want := range map[uint64]uint64{8: 8, 7: 5, 5: 5, 100: 8} {
		b, err := d.HighestBlockAtOrBelow(ctx, at)
		require.NoError(t, err)
		require.NotNil(t, b, "at or below %d", at)
		require.Equal(t, want, b.Number, "at or below %d", at)
		require.Equal(t, "0xhash"+map[uint64]string{8: "8", 5: "5"}[want], b.Hash)
	}
	b, err := d.HighestBlockAtOrBelow(ctx, 0)
	require.NoError(t, err)
	require.Nil(t, b, "nothing is stored at or below 0")
}
