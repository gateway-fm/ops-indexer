package db

import (
	"context"
	"reflect"
	"sync"
	"testing"
)

func missingSpanPairs(t *testing.T, d *DB) [][2]uint64 {
	t.Helper()
	rows, err := d.pool.Query(context.Background(), `SELECT from_number,to_number FROM missing_block_ranges ORDER BY from_number,to_number`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	pairs := [][2]uint64{}
	for rows.Next() {
		var pair [2]uint64
		if err := rows.Scan(&pair[0], &pair[1]); err != nil {
			t.Fatal(err)
		}
		pairs = append(pairs, pair)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return pairs
}

func TestPartialCompletionKeepsUnfilledBlockRanges(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	if err := d.SaveMissingRanges(ctx, []BlockRange{{FromNumber: 10, ToNumber: 19}, {FromNumber: 30, ToNumber: 34}}); err != nil {
		t.Fatal(err)
	}
	if err := d.DeleteMissingRangeByBlock(ctx, 15); err != nil {
		t.Fatal(err)
	}
	want := [][2]uint64{{10, 14}, {16, 19}, {30, 34}}
	if got := missingSpanPairs(t, d); !reflect.DeepEqual(got, want) {
		t.Fatalf("partial completion lost unfilled blocks: got%v want%v", got, want)
	}
}

func TestConcurrentPartialCompletionKeepsUnfilledSiblings(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	if err := d.SaveMissingRanges(ctx, []BlockRange{{FromNumber: 10, ToNumber: 30}}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for _, number := range []uint64{12, 17, 21, 29} {
		wg.Go(func() { errs <- d.DeleteMissingRangeByBlock(ctx, number) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	got := map[uint64]bool{}
	for _, span := range missingSpanPairs(t, d) {
		for n := span[0]; n <= span[1]; n++ {
			got[n] = true
		}
	}
	done := map[uint64]bool{12: true, 17: true, 21: true, 29: true}
	for n := uint64(10); n <= 30; n++ {
		if got[n] == done[n] {
			t.Fatalf("block%d missing=%v processed=%v", n, got[n], done[n])
		}
	}
}

func TestPartialCompletionSplitFailureRollsBack(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	if err := d.SaveMissingRanges(ctx, []BlockRange{{FromNumber: 10, ToNumber: 20}}); err != nil {
		t.Fatal(err)
	}
	_, err := d.pool.Exec(ctx, `CREATE FUNCTION reject_completion_split() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'injected split failure';END$$;CREATE TRIGGER reject_completion_split BEFORE INSERT ON missing_block_ranges FOR EACH ROW EXECUTE FUNCTION reject_completion_split()`)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.DeleteMissingRangeByBlock(ctx, 15); err == nil {
		t.Fatal("expected injected split failure")
	}
	if got := missingSpanPairs(t, d); !reflect.DeepEqual(got, [][2]uint64{{10, 20}}) {
		t.Fatalf("failed split changed coverage: %v", got)
	}
}
