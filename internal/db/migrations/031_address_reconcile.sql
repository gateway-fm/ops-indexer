-- Progress of the one-off reconciliation of address_stats against the ledger:
-- the next address bucket (0-255) to repair; 256 once every bucket is done.

INSERT INTO ledger_verify_state (name, value) VALUES ('address_reconcile_next', 0) ON CONFLICT (name) DO NOTHING;

---- create above / drop below ----

DELETE FROM ledger_verify_state WHERE name = 'address_reconcile_next';
