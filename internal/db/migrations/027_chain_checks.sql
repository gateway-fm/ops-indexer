-- Chain checks against the node: which span's transaction total was last compared with the node's
-- per-block counts, and how many full running-counter cycles have completed (the nonce sample waits for one, since
-- sent_count is recounted there on databases older than migration 024).

ALTER TABLE ledger_spans
    ADD COLUMN chain_checked_at TIMESTAMPTZ,
    ADD COLUMN chain_state TEXT CHECK (chain_state IN ('ok', 'drift'));
CREATE INDEX ledger_spans_chain_checked ON ledger_spans (chain_checked_at NULLS FIRST, lo);

CREATE TABLE ledger_verify_state (
    name TEXT PRIMARY KEY,
    value BIGINT NOT NULL
);
INSERT INTO ledger_verify_state (name, value) VALUES ('running_cycles', 0);

---- create above / drop below ----

DROP TABLE ledger_verify_state;
DROP INDEX ledger_spans_chain_checked;
ALTER TABLE ledger_spans DROP COLUMN chain_state, DROP COLUMN chain_checked_at;
