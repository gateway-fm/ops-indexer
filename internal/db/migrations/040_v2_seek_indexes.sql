-- Exact V2 seek orders, including the wire contract's C text collation.
-- Expand only: these indexes do not change retained facts or V1 behavior.
--
-- This is a transactional startup migration, like the existing keyset index
-- migration. Drain indexing writers for the build on a populated database:
-- lock_timeout bounds acquisition, NOT the time a successful build holds its
-- write-blocking lock. CREATE INDEX CONCURRENTLY cannot target a partitioned
-- parent. For a large deployment, prebuild ordinary-table indexes concurrently
-- and arrange partition-by-partition concurrent builds/attachment before this
-- migration; existing valid named indexes are then reused.
SET LOCAL lock_timeout = '1s';

CREATE INDEX IF NOT EXISTS idx_v2_transactions_seek
 ON transactions(block_number DESC,tx_index DESC,hash COLLATE "C" DESC);
CREATE INDEX IF NOT EXISTS idx_v2_transactions_search
 ON transactions(block_number DESC,hash COLLATE "C" DESC);
CREATE INDEX IF NOT EXISTS idx_v2_internal_seek
 ON internal_transactions(block_number DESC,tx_hash COLLATE "C" DESC,trace_address COLLATE "C" DESC);
CREATE INDEX IF NOT EXISTS idx_v2_contracts_seek
 ON contracts(block_number DESC,address COLLATE "C" DESC);
CREATE INDEX IF NOT EXISTS idx_v2_tokens_seek
 ON tokens(block_number DESC,address COLLATE "C" DESC);
CREATE INDEX IF NOT EXISTS idx_v2_addresses_seek
 ON address_stats(address COLLATE "C" DESC);
CREATE INDEX IF NOT EXISTS idx_v2_balances_seek
 ON token_balances_current(token_address COLLATE "C" DESC,address COLLATE "C" DESC);
CREATE INDEX IF NOT EXISTS idx_v2_holders_seek
 ON token_balances_current(token_address,balance DESC,address COLLATE "C" DESC) WHERE balance>0;
CREATE INDEX IF NOT EXISTS idx_v2_contract_creation
 ON contracts(creation_tx,block_number,creator,address COLLATE "C");

-- Blocks already have their unique number key; logs and transfers have the
-- unique (block_number,log_index) keys established by migration013.

---- create above / drop below ----
SET LOCAL lock_timeout = '1s';
DROP INDEX IF EXISTS idx_v2_contract_creation;
DROP INDEX IF EXISTS idx_v2_holders_seek;
DROP INDEX IF EXISTS idx_v2_balances_seek;
DROP INDEX IF EXISTS idx_v2_addresses_seek;
DROP INDEX IF EXISTS idx_v2_tokens_seek;
DROP INDEX IF EXISTS idx_v2_contracts_seek;
DROP INDEX IF EXISTS idx_v2_internal_seek;
DROP INDEX IF EXISTS idx_v2_transactions_search;
DROP INDEX IF EXISTS idx_v2_transactions_seek;
