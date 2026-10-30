-- Balances a reorg revert could not restore with certainty, to be read again from the node at the new head. Balances
-- are read at the node's latest state and stamped with the block that triggered the read, so a reverted block's rows
-- and the rows before it may hold fork-era values, and a read queued before the revert can still land after it. The
-- revert records every key its block touched; a read the balance loop makes for the request, carrying its seq,
-- clears it. A later revert of the same key renews seq, so an older read never clears the newer request.
CREATE SEQUENCE balance_reread_seq;

CREATE TABLE balance_rereads (
    token_address TEXT   NOT NULL,
    address       TEXT   NOT NULL,
    seq           BIGINT NOT NULL,
    attempts      INT    NOT NULL DEFAULT 0,
    PRIMARY KEY (token_address, address)
);

---- create above / drop below ----

DROP TABLE balance_rereads;
DROP SEQUENCE balance_reread_seq;
