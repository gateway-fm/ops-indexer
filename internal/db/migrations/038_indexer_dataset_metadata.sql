-- One durable identity for the indexed dataset. Reindex/wipe and an operator
-- restoring a backup rotate the epoch before API traffic resumes. Restarts do
-- not rotate it. Identity is initialized from the trusted configured node;
-- retained history need not contain the genesis block.
CREATE TABLE indexer_dataset_metadata (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    dataset_epoch UUID NOT NULL DEFAULT gen_random_uuid(),
    chain_id TEXT,
    genesis_hash BYTEA,
    CHECK ((chain_id IS NULL) = (genesis_hash IS NULL)),
    CHECK (chain_id IS NULL OR chain_id ~ '^(0|[1-9][0-9]*)$'),
    CHECK (genesis_hash IS NULL OR octet_length(genesis_hash) = 32)
);
INSERT INTO indexer_dataset_metadata (id) VALUES (1);

---- create above / drop below ----
DROP TABLE indexer_dataset_metadata;
