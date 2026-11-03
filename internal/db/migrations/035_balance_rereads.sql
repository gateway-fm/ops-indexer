-- Balances after a reorg revert. Balances are read at the node's latest state, asynchronously, and stamped with the
-- block that triggered the read, so a read made on the fork before a revert can land after it. Every revert takes a
-- new reorg epoch and records it on each key its block touched; every balance read carries the epoch current when it
-- was queued. A read whose epoch is below its key's was decided before the key's last revert and is dropped, while
-- the request is open and after it closed; any other read goes through the highest-block guard. The revert also opens
-- a request to read the key again at the new head, which the first read at the key's epoch or later closes.
CREATE SEQUENCE balance_reorg_epoch;

CREATE TABLE balance_rereads (
    token_address TEXT    NOT NULL,
    address       TEXT    NOT NULL,
    epoch         BIGINT  NOT NULL,
    open          BOOLEAN NOT NULL DEFAULT true,
    attempts      INT     NOT NULL DEFAULT 0,
    PRIMARY KEY (token_address, address)
);

---- create above / drop below ----

DROP TABLE balance_rereads;
DROP SEQUENCE balance_reorg_epoch;
