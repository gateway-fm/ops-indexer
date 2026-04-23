-- Blocks
CREATE TABLE IF NOT EXISTS blocks (
    number BIGINT PRIMARY KEY,
    hash TEXT NOT NULL UNIQUE,
    parent_hash TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    gas_used BIGINT NOT NULL,
    gas_limit BIGINT NOT NULL,
    base_fee_per_gas BIGINT,
    transaction_count INT NOT NULL,
    size BIGINT DEFAULT 0,
    difficulty TEXT DEFAULT '0',
    total_difficulty TEXT DEFAULT '0',
    nonce TEXT DEFAULT '0x0000000000000000',
    miner TEXT,
    extra_data TEXT,
    state_root TEXT,
    transactions_root TEXT,
    receipts_root TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_blocks_hash ON blocks(hash);
CREATE INDEX IF NOT EXISTS idx_blocks_miner ON blocks(miner);

-- Transactions
CREATE TABLE IF NOT EXISTS transactions (
    hash TEXT PRIMARY KEY,
    block_number BIGINT NOT NULL REFERENCES blocks(number) ON DELETE CASCADE,
    tx_index INT NOT NULL,
    from_address TEXT NOT NULL,
    to_address TEXT,
    value NUMERIC(78, 0) NOT NULL,
    gas_used BIGINT NOT NULL,
    gas_price BIGINT NOT NULL,
    gas_limit BIGINT,
    max_fee_per_gas BIGINT,
    max_priority_fee_per_gas BIGINT,
    nonce BIGINT,
    tx_type SMALLINT DEFAULT 0,
    input_data TEXT,
    status SMALLINT NOT NULL,
    error TEXT,
    revert_reason TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_tx_block ON transactions(block_number DESC);
CREATE INDEX IF NOT EXISTS idx_tx_from ON transactions(from_address, block_number DESC);
CREATE INDEX IF NOT EXISTS idx_tx_to ON transactions(to_address, block_number DESC);
CREATE INDEX IF NOT EXISTS idx_tx_from_nonce ON transactions(from_address, nonce DESC);
CREATE INDEX IF NOT EXISTS idx_tx_status ON transactions(status, block_number DESC);

-- Tokens (ERC20, ERC721 metadata)
CREATE TABLE IF NOT EXISTS tokens (
    address TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    name TEXT,
    decimals INT NOT NULL DEFAULT 18,
    token_type TEXT NOT NULL DEFAULT 'ERC20',
    total_supply NUMERIC(78, 0),
    holder_count INT DEFAULT 0,
    transfer_count INT DEFAULT 0,
    usd_price DECIMAL(20, 8),
    icon_url TEXT,
    l1_address TEXT,
    block_number BIGINT NOT NULL,
    creation_tx TEXT REFERENCES transactions(hash) ON DELETE CASCADE,
    off_chain_updated_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_tokens_symbol ON tokens(symbol);
CREATE INDEX IF NOT EXISTS idx_tokens_type ON tokens(token_type);
CREATE INDEX IF NOT EXISTS idx_tokens_holder_count ON tokens(holder_count DESC);

-- Token Transfers
CREATE TABLE IF NOT EXISTS token_transfers (
    id SERIAL PRIMARY KEY,
    tx_hash TEXT NOT NULL REFERENCES transactions(hash) ON DELETE CASCADE,
    log_index INT NOT NULL,
    token_address TEXT NOT NULL,
    from_address TEXT NOT NULL,
    to_address TEXT NOT NULL,
    value NUMERIC(78, 0) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp BIGINT,
    transfer_type TEXT DEFAULT 'transfer',
    token_type TEXT DEFAULT 'ERC20',
    token_id NUMERIC(78, 0),
    is_internal BOOLEAN DEFAULT false,
    UNIQUE(tx_hash, log_index)
);
CREATE INDEX IF NOT EXISTS idx_transfer_token ON token_transfers(token_address, block_number DESC);
CREATE INDEX IF NOT EXISTS idx_transfer_from ON token_transfers(from_address, block_number DESC);
CREATE INDEX IF NOT EXISTS idx_transfer_to ON token_transfers(to_address, block_number DESC);
CREATE INDEX IF NOT EXISTS idx_transfer_type ON token_transfers(transfer_type, block_number DESC);
CREATE INDEX IF NOT EXISTS idx_transfer_timestamp ON token_transfers(timestamp DESC);

-- Balances
CREATE TABLE IF NOT EXISTS balances (
    address TEXT NOT NULL,
    token_address TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    balance NUMERIC(78, 0) NOT NULL,
    PRIMARY KEY (address, token_address, block_number)
);
CREATE INDEX IF NOT EXISTS idx_balances_address ON balances(address, block_number DESC);
CREATE INDEX IF NOT EXISTS idx_balances_token ON balances(token_address, block_number DESC);
CREATE INDEX IF NOT EXISTS idx_balances_block ON balances(block_number DESC);

-- Counters
CREATE TABLE IF NOT EXISTS counters (
    address TEXT NOT NULL,
    counter_type TEXT NOT NULL,
    count BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (address, counter_type)
);
CREATE INDEX IF NOT EXISTS idx_counters_type ON counters(counter_type, count DESC);

-- Address Stats
CREATE TABLE IF NOT EXISTS address_stats (
    address TEXT PRIMARY KEY,
    tx_count INT DEFAULT 0,
    internal_tx_count INT DEFAULT 0,
    token_transfer_count INT DEFAULT 0,
    first_seen BIGINT,
    last_seen BIGINT,
    is_contract BOOLEAN DEFAULT false,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Contracts
CREATE TABLE IF NOT EXISTS contracts (
    address TEXT PRIMARY KEY,
    bytecode TEXT NOT NULL,
    bytecode_hash TEXT,
    creator TEXT NOT NULL,
    creation_tx TEXT NOT NULL REFERENCES transactions(hash) ON DELETE CASCADE,
    block_number BIGINT NOT NULL,
    is_verified BOOLEAN DEFAULT false,
    contract_name TEXT,
    compiler_version TEXT,
    optimization_used BOOLEAN,
    optimization_runs INT,
    evm_version TEXT,
    source_code TEXT,
    abi JSONB,
    license_type TEXT,
    constructor_args TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_contracts_creator ON contracts(creator);
CREATE INDEX IF NOT EXISTS idx_contracts_verified ON contracts(is_verified);

-- Logs
CREATE TABLE IF NOT EXISTS logs (
    id SERIAL PRIMARY KEY,
    tx_hash TEXT NOT NULL REFERENCES transactions(hash) ON DELETE CASCADE,
    log_index INT NOT NULL,
    address TEXT NOT NULL,
    topic0 TEXT,
    topic1 TEXT,
    topic2 TEXT,
    topic3 TEXT,
    data TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp BIGINT,
    removed BOOLEAN DEFAULT false,
    UNIQUE(tx_hash, log_index)
);
CREATE INDEX IF NOT EXISTS idx_logs_tx_hash ON logs(tx_hash);
CREATE INDEX IF NOT EXISTS idx_logs_address ON logs(address, block_number DESC);
CREATE INDEX IF NOT EXISTS idx_logs_topic0 ON logs(topic0, block_number DESC);
CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_address_topic0 ON logs(address, topic0, block_number DESC);

-- Internal Transactions
CREATE TABLE IF NOT EXISTS internal_transactions (
    id SERIAL PRIMARY KEY,
    tx_hash TEXT NOT NULL REFERENCES transactions(hash) ON DELETE CASCADE,
    block_number BIGINT NOT NULL,
    trace_address TEXT NOT NULL,
    from_address TEXT NOT NULL,
    to_address TEXT,
    value NUMERIC(78, 0) NOT NULL DEFAULT 0,
    gas BIGINT,
    gas_used BIGINT,
    input TEXT,
    output TEXT,
    call_type TEXT,
    error TEXT,
    timestamp BIGINT,
    UNIQUE(tx_hash, trace_address)
);
CREATE INDEX IF NOT EXISTS idx_internal_tx_hash ON internal_transactions(tx_hash);
CREATE INDEX IF NOT EXISTS idx_internal_tx_from ON internal_transactions(from_address, block_number DESC);
CREATE INDEX IF NOT EXISTS idx_internal_tx_to ON internal_transactions(to_address, block_number DESC);
CREATE INDEX IF NOT EXISTS idx_internal_tx_block ON internal_transactions(block_number DESC);

-- Sync Status
CREATE TABLE IF NOT EXISTS sync_status (
    id SERIAL PRIMARY KEY,
    last_indexed_block BIGINT NOT NULL,
    last_verified_block BIGINT,
    last_finalized_block BIGINT,
    is_syncing BOOLEAN DEFAULT false,
    updated_at TIMESTAMP DEFAULT NOW()
);
INSERT INTO sync_status (last_indexed_block, is_syncing) VALUES (0, false) ON CONFLICT DO NOTHING;

-- Missing Block Ranges
CREATE TABLE IF NOT EXISTS missing_block_ranges (
    id SERIAL PRIMARY KEY,
    from_number BIGINT NOT NULL,
    to_number BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT valid_range CHECK (from_number <= to_number),
    CONSTRAINT unique_range UNIQUE (from_number, to_number)
);
CREATE INDEX IF NOT EXISTS idx_missing_ranges_from ON missing_block_ranges(from_number);
CREATE INDEX IF NOT EXISTS idx_missing_ranges_to ON missing_block_ranges(to_number);

-- Indexer Progress
CREATE TABLE IF NOT EXISTS indexer_progress (
    id SERIAL PRIMARY KEY,
    min_fetched_block BIGINT DEFAULT 0,
    max_fetched_block BIGINT DEFAULT 0,
    backfill_complete BOOLEAN DEFAULT false,
    updated_at TIMESTAMP DEFAULT NOW()
);
INSERT INTO indexer_progress (min_fetched_block, max_fetched_block, backfill_complete)
VALUES (0, 0, false) ON CONFLICT DO NOTHING;

-- OP Stack Deposit Metadata
CREATE TABLE IF NOT EXISTS op_deposits (
    l2_tx_hash TEXT PRIMARY KEY REFERENCES transactions(hash) ON DELETE CASCADE,
    l1_block_number BIGINT NOT NULL,
    l1_block_timestamp BIGINT,
    l1_tx_hash TEXT NOT NULL,
    l1_tx_origin TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_op_deposits_l1_block ON op_deposits(l1_block_number DESC);
CREATE INDEX IF NOT EXISTS idx_op_deposits_l1_tx ON op_deposits(l1_tx_hash);
