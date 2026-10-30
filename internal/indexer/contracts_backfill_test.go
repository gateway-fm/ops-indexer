package indexer

import (
	"context"
	"errors"
	"math/big"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gateway-fm/chain-indexer/internal/rpc"
	"github.com/gateway-fm/chain-indexer/internal/types"
	"github.com/gateway-fm/chain-indexer/pkg/eth/common"
	"github.com/gateway-fm/chain-indexer/pkg/eth/rpclient"
)

type backfillDB struct{ contracts map[string]*types.Contract }

func (f *backfillDB) BackfillContractsFromInternalTransactions(context.Context) (int64, error) {
	return 0, nil
}

func (f *backfillDB) ExistingContracts(_ context.Context, addresses []string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, a := range addresses {
		if f.contracts[a] != nil {
			out[a] = true
		}
	}
	return out, nil
}

func (f *backfillDB) InsertContract(_ context.Context, c *types.Contract) error {
	if f.contracts[c.Address] == nil {
		f.contracts[c.Address] = c
	}
	return nil
}

type backfillNode struct {
	traces     map[common.Hash]*rpc.CallFrame
	blocks     map[common.Hash]uint64
	fail       map[common.Hash]bool
	calls      int
	traceCalls int
}

func (f *backfillNode) TraceTransaction(_ context.Context, h common.Hash) (*rpc.CallFrame, error) {
	f.calls++
	f.traceCalls++
	if f.fail[h] {
		return nil, errors.New("node timeout")
	}
	return f.traces[h], nil
}

func (f *backfillNode) TransactionReceipt(_ context.Context, h common.Hash) (*rpclient.Receipt, error) {
	f.calls++
	return &rpclient.Receipt{Status: 1, TxHash: h, BlockNumber: new(big.Int).SetUint64(f.blocks[h])}, nil
}

func (f *backfillNode) TransactionByHash(_ context.Context, h common.Hash) (*rpclient.Transaction, bool, error) {
	f.calls++
	to := common.HexToAddress("0x00000000000000000000000000000000000FAC70")
	return &rpclient.Transaction{Hash: h, To: &to}, false, nil
}

func (f *backfillNode) GetCode(context.Context, common.Address) ([]byte, error) {
	f.calls++
	return []byte{0x60, 0x80, 0xdd}, nil
}

func addr(n int64) string { return strings.ToLower(common.BytesToAddress(big.NewInt(n).Bytes()).Hex()) }

// The pruned range's factory-deployed contracts come from tracing each listed transaction once on the node: rows as
// ingest writes them, a listed contract the trace does not create reported (and not written), an unlisted one the
// trace creates written, a failed trace left for the rerun, and a transaction already complete never traced again.
func TestContractsBackfillTracesListedTransactionsAndResumes(t *testing.T) {
	ctx := context.Background()
	factory := common.HexToAddress("0x00000000000000000000000000000000000FAC70")
	txA, txB, txC, txD := common.HexToHash("0xa"), common.HexToHash("0xb"), common.HexToHash("0xc"), common.HexToHash("0xd")
	create := func(callType string, to int64, output, errMsg string, calls ...rpc.CallFrame) rpc.CallFrame {
		return rpc.CallFrame{Type: callType, From: factory.Hex(), To: common.BytesToAddress(big.NewInt(to).Bytes()).Hex(), Output: output, Error: errMsg, Calls: calls}
	}
	root := func(errMsg string, calls ...rpc.CallFrame) *rpc.CallFrame {
		return &rpc.CallFrame{Type: "CALL", From: "0x00000000000000000000000000000000000000a1", To: factory.Hex(), Error: errMsg, Calls: calls}
	}
	node := &backfillNode{
		traces: map[common.Hash]*rpc.CallFrame{
			txA: root("", create("CREATE2", 0xc1, "0x6080aa", ""), create("CREATE", 0xc2, "", ""), create("CREATE", 0xc3, "0x6080cc", "")),
			txC: root("", create("CALL", 0xb2, "", "execution reverted", create("CREATE", 0xc5, "0x6080ee", ""))),
			txD: root("", create("CREATE2", 0xc6, "0x6080ff", "")),
		},
		blocks: map[common.Hash]uint64{txA: 10, txC: 12, txD: 13},
		fail:   map[common.Hash]bool{txD: true},
	}
	d := &backfillDB{contracts: map[string]*types.Contract{addr(0xc4): {Address: addr(0xc4)}}}
	listed := []Creation{
		{Address: addr(0xc1), TxHash: txA.Hex(), Block: 10}, {Address: addr(0xc2), TxHash: txA.Hex(), Block: 10},
		{Address: addr(0xc4), TxHash: txB.Hex(), Block: 11},
		{Address: addr(0xc5), TxHash: txC.Hex(), Block: 12},
		{Address: addr(0xc6), TxHash: txD.Hex(), Block: 13},
	}

	report, err := BackfillContracts(ctx, d, node, listed, ContractsBackfillOptions{PerSecond: 1_000})
	require.NoError(t, err)
	assert.Equal(t, 2, report.TxsTraced, "A and C; D failed")
	assert.Equal(t, 1, report.TxsSkipped, "B's contract exists")
	assert.Equal(t, 3, report.Written, "c1, c2, and the unlisted c3")
	assert.Equal(t, []string{addr(0xc5)}, report.NotCreated, "c5 was created under a reverted call")
	assert.Equal(t, []string{addr(0xc3)}, report.Unlisted)
	assert.Contains(t, report.Failed, txD.Hex())

	creator := strings.ToLower(factory.Hex())
	c1 := d.contracts[addr(0xc1)]
	require.NotNil(t, c1)
	assert.Equal(t, [4]string{"6080aa", "0x00000000000000000000000000000000000000000000000000000000006080aa", creator, txA.Hex()},
		[4]string{c1.Bytecode, *c1.BytecodeHash, c1.Creator, c1.CreationTx}, "the row the SQL backfill writes for the same frame")
	assert.EqualValues(t, 10, c1.BlockNumber)
	assert.Equal(t, "6080dd", d.contracts[addr(0xc2)].Bytecode, "no output in the frame: the node's code")

	node.fail = nil
	calls := node.calls
	report, err = BackfillContracts(ctx, d, node, listed, ContractsBackfillOptions{PerSecond: 1_000})
	require.NoError(t, err)
	assert.Equal(t, 2, report.TxsSkipped, "A and B are complete")
	assert.Equal(t, 2, report.TxsTraced, "C again (c5 is still missing), and D")
	assert.Equal(t, 4, node.calls-calls, "a receipt and a trace each for C and D, nothing for A or B")
	keys := make([]string, 0, len(d.contracts))
	for k := range d.contracts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	assert.Equal(t, []string{addr(0xc1), addr(0xc2), addr(0xc3), addr(0xc4), addr(0xc6)}, keys)
}

func TestReadCreationsCSV(t *testing.T) {
	got, err := ReadCreationsCSV(strings.NewReader("address,creation_tx_hash,block_number\n0xABC,0xDEF,123\n"))
	require.NoError(t, err)
	assert.Equal(t, []Creation{{Address: "0xabc", TxHash: "0xdef", Block: 123}}, got)
	got, err = ReadCreationsCSV(strings.NewReader("0xABC,0xDEF,123\n"))
	require.NoError(t, err, "a Blockscout export has no header row")
	assert.Equal(t, []Creation{{Address: "0xabc", TxHash: "0xdef", Block: 123}}, got)
	_, err = ReadCreationsCSV(strings.NewReader("0xabc,0xdef\n"))
	assert.Error(t, err)
}

// A node that cannot trace old blocks: rows from the receipt, the current code and the creator from the list (else
// the transaction's to), with no trace call at all.
func TestContractsBackfillWithoutTraces(t *testing.T) {
	ctx := context.Background()
	txE := common.HexToHash("0xe")
	node := &backfillNode{blocks: map[common.Hash]uint64{txE: 480}}
	d := &backfillDB{contracts: map[string]*types.Contract{}}
	listed := []Creation{
		{Address: addr(0xc7), TxHash: txE.Hex(), Block: 480, Creator: addr(0xfa)},
		{Address: addr(0xc8), TxHash: txE.Hex(), Block: 480},
	}
	report, err := BackfillContracts(ctx, d, node, listed, ContractsBackfillOptions{PerSecond: 1_000, WithoutTraces: true})
	require.NoError(t, err)
	assert.Zero(t, node.traceCalls)
	assert.Equal(t, 2, report.Written)
	assert.Empty(t, report.NotCreated)
	c7, c8 := d.contracts[addr(0xc7)], d.contracts[addr(0xc8)]
	require.NotNil(t, c7)
	require.NotNil(t, c8)
	assert.Equal(t, addr(0xfa), c7.Creator, "the listed creator")
	assert.Equal(t, "0x00000000000000000000000000000000000fac70", c8.Creator, "the transaction's to")
	assert.Equal(t, [3]string{"6080dd", "0x00000000000000000000000000000000000000000000000000000000006080dd", txE.Hex()},
		[3]string{c8.Bytecode, *c8.BytecodeHash, c8.CreationTx}, "the same row shape as the traced path")
	assert.EqualValues(t, 480, c8.BlockNumber)

	got, err := ReadCreationsCSV(strings.NewReader("0xABC,0xDEF,123,0xFAC\n"))
	require.NoError(t, err)
	assert.Equal(t, "0xfac", got[0].Creator)
}
