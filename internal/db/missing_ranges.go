package db

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

type BlockRange struct {
	ID         int
	FromNumber uint64
	ToNumber   uint64
	CreatedAt  time.Time
}

type IndexerProgress struct {
	ID               int
	MinFetchedBlock  uint64
	MaxFetchedBlock  uint64
	BackfillComplete bool
	UpdatedAt        time.Time
}

func (d *DB) GetIndexerProgress(ctx context.Context) (*IndexerProgress, error) {
	var p IndexerProgress
	err := d.pool.QueryRow(ctx, `
		SELECT id, min_fetched_block, max_fetched_block, backfill_complete, updated_at
		FROM indexer_progress ORDER BY id LIMIT 1`).Scan(
		&p.ID, &p.MinFetchedBlock, &p.MaxFetchedBlock, &p.BackfillComplete, &p.UpdatedAt)
	if err == pgx.ErrNoRows {
		return &IndexerProgress{}, nil
	}
	return &p, err
}

func (d *DB) UpdateIndexerProgress(ctx context.Context, minBlock, maxBlock uint64, backfillComplete bool) error {
	_, err := d.pool.Exec(ctx, `
		UPDATE indexer_progress SET
			min_fetched_block = $1,
			max_fetched_block = $2,
			backfill_complete = $3,
			updated_at = NOW()
		WHERE id = (SELECT id FROM indexer_progress ORDER BY id LIMIT 1)`,
		minBlock, maxBlock, backfillComplete)
	return err
}

func (d *DB) SaveMissingRanges(ctx context.Context, ranges []BlockRange) error {
	if len(ranges) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, r := range ranges {
		batch.Queue(`
			INSERT INTO missing_block_ranges (from_number, to_number)
			VALUES ($1, $2)
			ON CONFLICT (from_number, to_number) DO NOTHING`,
			r.FromNumber, r.ToNumber)
	}

	br := d.pool.SendBatch(ctx, batch)
	defer br.Close()

	for range ranges {
		_, err := br.Exec()
		if err != nil {
			return err
		}
	}
	return nil
}

// Returns ranges ordered by from_number descending (process newest first)
func (d *DB) GetMissingRangesBatch(ctx context.Context, batchSize int) ([]BlockRange, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT id, from_number, to_number, created_at
		FROM missing_block_ranges
		ORDER BY from_number DESC
		LIMIT $1`, batchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ranges []BlockRange
	for rows.Next() {
		var r BlockRange
		if err := rows.Scan(&r.ID, &r.FromNumber, &r.ToNumber, &r.CreatedAt); err != nil {
			return nil, err
		}
		ranges = append(ranges, r)
	}
	return ranges, rows.Err()
}

func (d *DB) DeleteMissingRange(ctx context.Context, id int) error {
	_, err := d.pool.Exec(ctx, `DELETE FROM missing_block_ranges WHERE id = $1`, id)
	return err
}

func (d *DB) DeleteMissingRangeByBlock(ctx context.Context, blockNumber uint64) error {
	_, err := d.pool.Exec(ctx, `
		DELETE FROM missing_block_ranges
		WHERE from_number <= $1 AND to_number >= $1`, blockNumber)
	return err
}

// ShrinkMissingRange shrinks a range after processing a block.
// If the processed block is in the middle, the range is split into two.
func (d *DB) ShrinkMissingRange(ctx context.Context, id int, processedBlock uint64) error {
	var fromNum, toNum uint64
	err := d.pool.QueryRow(ctx, `
		SELECT from_number, to_number FROM missing_block_ranges WHERE id = $1`,
		id).Scan(&fromNum, &toNum)
	if err != nil {
		return err
	}

	_, err = d.pool.Exec(ctx, `DELETE FROM missing_block_ranges WHERE id = $1`, id)
	if err != nil {
		return err
	}

	if processedBlock == fromNum {
		if fromNum < toNum {
			_, err = d.pool.Exec(ctx, `
				INSERT INTO missing_block_ranges (from_number, to_number)
				VALUES ($1, $2)`, processedBlock+1, toNum)
		}
	} else if processedBlock == toNum {
		if fromNum < toNum {
			_, err = d.pool.Exec(ctx, `
				INSERT INTO missing_block_ranges (from_number, to_number)
				VALUES ($1, $2)`, fromNum, processedBlock-1)
		}
	} else if processedBlock > fromNum && processedBlock < toNum {
		_, err = d.pool.Exec(ctx, `
			INSERT INTO missing_block_ranges (from_number, to_number)
			VALUES ($1, $2), ($3, $4)`,
			fromNum, processedBlock-1, processedBlock+1, toNum)
	}
	return err
}

func (d *DB) GetMissingRangesCount(ctx context.Context) (int64, error) {
	var count int64
	err := d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM missing_block_ranges`).Scan(&count)
	return count, err
}

func (d *DB) GetTotalMissingBlocks(ctx context.Context) (int64, error) {
	var total *int64
	err := d.pool.QueryRow(ctx, `
		SELECT SUM(to_number - from_number + 1) FROM missing_block_ranges`).Scan(&total)
	if total == nil {
		return 0, err
	}
	return *total, err
}

func (d *DB) FindMissingBlocksInRange(ctx context.Context, fromBlock, toBlock uint64) ([]BlockRange, error) {
	rows, err := d.pool.Query(ctx, `
		WITH block_series AS (
			SELECT generate_series($1::bigint, $2::bigint) AS block_num
		),
		existing_blocks AS (
			SELECT number FROM blocks WHERE number >= $1 AND number <= $2
		),
		missing AS (
			SELECT block_num FROM block_series
			WHERE block_num NOT IN (SELECT number FROM existing_blocks)
		),
		gaps AS (
			SELECT
				block_num,
				block_num - ROW_NUMBER() OVER (ORDER BY block_num) AS grp
			FROM missing
		)
		SELECT MIN(block_num) as from_num, MAX(block_num) as to_num
		FROM gaps
		GROUP BY grp
		ORDER BY from_num DESC`,
		fromBlock, toBlock)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ranges []BlockRange
	for rows.Next() {
		var r BlockRange
		if err := rows.Scan(&r.FromNumber, &r.ToNumber); err != nil {
			return nil, err
		}
		ranges = append(ranges, r)
	}
	return ranges, rows.Err()
}

func (d *DB) GetMinMaxIndexedBlocks(ctx context.Context) (min uint64, max uint64, err error) {
	var minPtr, maxPtr *uint64
	err = d.pool.QueryRow(ctx, `
		SELECT MIN(number), MAX(number) FROM blocks`).Scan(&minPtr, &maxPtr)
	if minPtr != nil {
		min = *minPtr
	}
	if maxPtr != nil {
		max = *maxPtr
	}
	return
}

func (d *DB) GetBlockCount(ctx context.Context) (int64, error) {
	var count int64
	err := d.pool.QueryRow(ctx, `SELECT COUNT(*) FROM blocks`).Scan(&count)
	return count, err
}

// RequeueMissingBlock re-inserts a single block as a missing range so it will
// be retried by the catchup worker. Used when processBlock fails — without this,
// the block is permanently lost because the parent range was already deleted from
// missing_block_ranges when an earlier sibling block was successfully processed.
func (d *DB) RequeueMissingBlock(ctx context.Context, blockNum uint64) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO missing_block_ranges (from_number, to_number)
		VALUES ($1, $1)
		ON CONFLICT (from_number, to_number) DO NOTHING`,
		blockNum)
	return err
}

func (d *DB) ClearMissingRanges(ctx context.Context) error {
	_, err := d.pool.Exec(ctx, `TRUNCATE missing_block_ranges`)
	return err
}

func (d *DB) HasBlock(ctx context.Context, number uint64) (bool, error) {
	var exists bool
	err := d.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM blocks WHERE number = $1)`, number).Scan(&exists)
	return exists, err
}
