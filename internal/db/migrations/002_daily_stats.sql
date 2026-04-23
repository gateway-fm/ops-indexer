-- Daily Stats for chart data
CREATE TABLE IF NOT EXISTS daily_stats (
    date DATE PRIMARY KEY,
    total_blocks INT NOT NULL DEFAULT 0,
    total_transactions INT NOT NULL DEFAULT 0,
    total_gas_used BIGINT NOT NULL DEFAULT 0,
    avg_gas_price BIGINT NOT NULL DEFAULT 0,
    successful_txs INT NOT NULL DEFAULT 0,
    failed_txs INT NOT NULL DEFAULT 0,
    active_addresses INT NOT NULL DEFAULT 0,
    new_addresses INT NOT NULL DEFAULT 0,
    avg_block_time FLOAT NOT NULL DEFAULT 0,
    avg_block_size BIGINT NOT NULL DEFAULT 0,
    new_contracts INT NOT NULL DEFAULT 0,
    token_transfer_count INT NOT NULL DEFAULT 0,
    cumulative_transactions BIGINT NOT NULL DEFAULT 0,
    cumulative_addresses BIGINT NOT NULL DEFAULT 0,
    cumulative_contracts BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_daily_stats_date ON daily_stats(date);

---- create above / drop below ----

DROP TABLE IF EXISTS daily_stats;
