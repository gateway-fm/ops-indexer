-- idx_balances_address (address, block_number DESC) is served, for every query that reads
-- balances by address, by the primary key (address, token_address, block_number): each of those
-- queries filters on address and either fixes token_address or partitions by it, so the key
-- delivers the rows in the order they ask for. Nothing orders one address's history across
-- tokens, which is the only shape this index adds.
--
-- It is randomly keyed by address, and every balance row a flush writes (two per ERC-20
-- transfer) inserted into it as well as into the primary key: on a 6.8M-transaction database
-- it was 1.5 GB with 15 scans over the whole fill.
DROP INDEX IF EXISTS idx_balances_address;

---- create above / drop below ----

CREATE INDEX IF NOT EXISTS idx_balances_address ON balances(address, block_number DESC);
