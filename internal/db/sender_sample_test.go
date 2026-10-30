package db

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The nonce sample takes only senders that sent nothing but the types sent_count counts (0-4): a sender with a
// deposit or system transaction (here OP Stack's 126) has a nonce above its sent_count by design.
func TestTheNonceSampleTakesOnlySendersOfCountedTypes(t *testing.T) {
	d, cleanup := setupTestDB(t)
	defer cleanup()
	ctx := context.Background()
	_, err := d.pool.Exec(ctx, `
		INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count)
		VALUES (1, 'b1', 'b0', 1700000001, 0, 1, 2), (2, 'b2', 'b1', 1700000002, 0, 1, 2);
		INSERT INTO transactions (hash, block_number, tx_index, from_address, to_address, value, gas_used, gas_price, input_data, status, tx_type)
		VALUES ('t1', 1, 0, '0xc1ea', '0xbeef', 0, 21000, 0, '0x', 1, 2),
		       ('t2', 1, 1, '0xd3b0', '0xbeef', 0, 21000, 0, '0x', 1, 126),
		       ('t3', 2, 0, '0xd3b0', '0xbeef', 0, 21000, 0, '0x', 1, 2),
		       ('t4', 2, 1, '0xc1ea', '0xbeef', 0, 21000, 0, '0x', 1, 0);
		INSERT INTO address_stats (address, tx_count, internal_tx_count, token_transfer_count, sent_count, first_seen, last_seen, is_contract, updated_at)
		VALUES ('0xc1ea', 2, 0, 0, 2, 1, 2, false, NOW()), ('0xd3b0', 2, 0, 0, 1, 1, 2, false, NOW());
		UPDATE ledger_verify_state SET value = 1 WHERE name = 'running_cycles'`)
	require.NoError(t, err)

	got, err := d.SampleSenders(ctx, 50)
	require.NoError(t, err)
	var sampled []string
	for _, s := range got {
		sampled = append(sampled, s.Address)
	}
	require.Equal(t, []string{"0xc1ea"}, sampled, "the sender of a type-126 transaction is not sampled")

	conn, err := d.pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()
	_, err = conn.Exec(ctx, `SET enable_seqscan = off; SET enable_hashjoin = off; SET enable_mergejoin = off`)
	require.NoError(t, err, "the fixture is tiny: show the plan a large table gets")
	rows, err := conn.Query(ctx, `EXPLAIN `+senderSampleSQL, 50, senderProbeTxs)
	require.NoError(t, err)
	var plan []string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		plan = append(plan, line)
	}
	rows.Close()
	t.Logf("sample plan:\n%s", strings.Join(plan, "\n"))
	require.Contains(t, strings.Join(plan, "\n"), "idx_tx_from", "the probe reads a sender's transactions through the from_address index")
}

// The API docs claim no more for tx_count_out than sent_count counts: types 0-4, below eth_getTransactionCount where a
// chain's system or deposit types raise a nonce.
func TestTheAPIDocsStateWhatSentCountCounts(t *testing.T) {
	doc, err := os.ReadFile("../../docs/API.md")
	require.NoError(t, err)
	text := strings.Join(strings.Fields(string(doc)), " ")
	require.Contains(t, text, "transactions of types 0-4 sent by the address. On chains with system or deposit "+
		"transaction types, such as OP Stack (126) and Arbitrum (100-106), it can be below `eth_getTransactionCount`")
	require.NotContains(t, text, "it equals `eth_getTransactionCount`")
}
