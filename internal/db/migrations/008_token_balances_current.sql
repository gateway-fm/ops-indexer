-- One row per (token, address) holding that token's CURRENT balance.
--
-- `balances` is append-only: PK (address, token_address, block_number), one row
-- per address per block in which that balance changed. Answering "how many
-- addresses currently hold > 0" from it therefore means reading the token's
-- whole history, sorting it, and keeping the newest row per address --
-- DISTINCT ON (address) ... ORDER BY address, block_number DESC. That cost
-- grows with stored history, forever, because the row count only ever
-- increases. At multi-million-row scale it runs for seconds per call and
-- becomes the most expensive statement in the database.
--
-- This table holds exactly the row that query would have returned, so the same
-- question becomes a filtered count whose cost grows with the number of current
-- holders instead -- far slower growth, and bounded by how many accounts
-- actually hold the token. Same answer, well over an order of magnitude
-- cheaper; figures on PRST-4493.
--
-- `balances` is kept: it is the history, and GetBalanceHistory reads it.
CREATE TABLE IF NOT EXISTS token_balances_current (
    token_address TEXT          NOT NULL,
    address       TEXT          NOT NULL,
    balance       NUMERIC(78, 0) NOT NULL,
    block_number  BIGINT        NOT NULL,
    PRIMARY KEY (token_address, address)
);

-- Backfilled before the index exists: maintaining an index across the whole
-- backfill costs materially more than building it once afterwards.
--
-- No LOWER() here. 005_lowercase_addresses.sql already lowercased balances and
-- deleted the mixed-case collisions, and the writer lowercases, so the source
-- is already canonical -- while adding LOWER() could itself collide two rows
-- onto one primary key if that ever stopped being true.
INSERT INTO token_balances_current (token_address, address, balance, block_number)
SELECT DISTINCT ON (token_address, address) token_address, address, balance, block_number
FROM balances
ORDER BY token_address, address, block_number DESC
ON CONFLICT (token_address, address) DO NOTHING;

-- Serves both the holder count and a balance-ordered holder listing.
--
-- Partial on balance > 0 because every consumer asks about current holders, and
-- addresses that have gone to zero are a growing dead weight otherwise. Note
-- the planner will still choose a sequential scan when a single token owns
-- nearly all the rows, where scanning is genuinely cheaper -- this earns its
-- keep on chains with many tokens rather than one dominant one.
CREATE INDEX IF NOT EXISTS idx_tbc_holders
    ON token_balances_current (token_address, balance DESC) WHERE balance > 0;

ANALYZE token_balances_current;

---- create above / drop below ----

DROP TABLE IF EXISTS token_balances_current;
