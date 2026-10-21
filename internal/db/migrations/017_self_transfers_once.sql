-- address_stats counts a self-transfer and a self-call once. Rows written before
-- this counted both sides; subtract one side. The scan needs every history row, so it refuses once any is pruned.

DO $m$
BEGIN
    IF pruned_through() > 0 THEN
        RAISE EXCEPTION 'migration 017 (self-transfers counted once) needs every token_transfers and internal_transactions row, but history at or below block % is pruned', pruned_through();
    END IF;
END
$m$;

UPDATE address_stats a SET token_transfer_count = a.token_transfer_count - s.n
FROM (SELECT from_address AS address, count(*) AS n FROM token_transfers
      WHERE to_address = from_address AND from_address <> '0x0000000000000000000000000000000000000000'
      GROUP BY 1) s
WHERE a.address = s.address;

UPDATE address_stats a SET internal_tx_count = a.internal_tx_count - s.n
FROM (SELECT from_address AS address, count(*) AS n FROM internal_transactions
      WHERE to_address = from_address
      GROUP BY 1) s
WHERE a.address = s.address;

---- create above / drop below ----

DO $m$
BEGIN
    IF pruned_through() > 0 THEN
        RAISE EXCEPTION 'migration 017 cannot be reverted: history at or below block % is pruned', pruned_through();
    END IF;
END
$m$;

UPDATE address_stats a SET token_transfer_count = a.token_transfer_count + s.n
FROM (SELECT from_address AS address, count(*) AS n FROM token_transfers
      WHERE to_address = from_address AND from_address <> '0x0000000000000000000000000000000000000000'
      GROUP BY 1) s
WHERE a.address = s.address;

UPDATE address_stats a SET internal_tx_count = a.internal_tx_count + s.n
FROM (SELECT from_address AS address, count(*) AS n FROM internal_transactions
      WHERE to_address = from_address
      GROUP BY 1) s
WHERE a.address = s.address;
