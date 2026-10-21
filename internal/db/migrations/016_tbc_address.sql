-- Balance reads by address (ListTokenBalances, BatchGetTokenBalances) come from token_balances_current instead
-- of the balances history, which retention drops by range (migration 014). Its key leads with token_address.
CREATE INDEX IF NOT EXISTS idx_tbc_address ON token_balances_current (address);

---- create above / drop below ----

DROP INDEX IF EXISTS idx_tbc_address;
