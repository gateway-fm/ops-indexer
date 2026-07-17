package grpcserver

import (
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"github.com/gateway-fm/chain-indexer/internal/types"
)

// unixToProto converts a Unix seconds timestamp to google.protobuf.Timestamp.
// Returns nil when ts is zero (no timestamp).
func unixToProto(ts uint64) *timestamppb.Timestamp {
	if ts == 0 {
		return nil
	}
	return timestamppb.New(time.Unix(int64(ts), 0).UTC())
}

func unixPtrToProto(ts *uint64) *timestamppb.Timestamp {
	if ts == nil {
		return nil
	}
	return unixToProto(*ts)
}

func timeToProto(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t.UTC())
}

// strOrEmpty returns the string the pointer refers to, or "" if nil.
func strOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// bigIntFromString wraps a decimal-string big integer in the BigInt proto.
// Empty string means "unknown / not populated" per docs/API.md.
func bigIntFromString(s string) *indexerv1.BigInt {
	return &indexerv1.BigInt{Value: s}
}

func bigIntFromPtrString(s *string) *indexerv1.BigInt {
	if s == nil {
		return &indexerv1.BigInt{}
	}
	return &indexerv1.BigInt{Value: *s}
}

func bigIntFromFloat(f *float64) *indexerv1.BigInt {
	if f == nil {
		return &indexerv1.BigInt{}
	}
	return &indexerv1.BigInt{Value: strconv.FormatFloat(*f, 'f', -1, 64)}
}

func bigIntFromUint64Ptr(u *uint64) *indexerv1.BigInt {
	if u == nil {
		return &indexerv1.BigInt{}
	}
	return &indexerv1.BigInt{Value: strconv.FormatUint(*u, 10)}
}

// ----- Block -----

func mapBlock(b *types.Block) *indexerv1.Block {
	if b == nil {
		return nil
	}
	var baseFee *indexerv1.BigInt
	if b.BaseFeePerGas != nil {
		baseFee = &indexerv1.BigInt{Value: strconv.FormatUint(*b.BaseFeePerGas, 10)}
	} else {
		baseFee = &indexerv1.BigInt{}
	}
	return &indexerv1.Block{
		Number:           b.Number,
		Hash:             b.Hash,
		ParentHash:       b.ParentHash,
		Timestamp:        unixToProto(b.Timestamp),
		Miner:            b.Miner,
		GasUsed:          b.GasUsed,
		GasLimit:         b.GasLimit,
		BaseFeePerGas:    baseFee,
		Difficulty:       bigIntFromString(b.Difficulty),
		TotalDifficulty:  bigIntFromString(b.TotalDifficulty),
		StateRoot:        b.StateRoot,
		TransactionsRoot: b.TransactionsRoot,
		ReceiptsRoot:     b.ReceiptsRoot,
		ExtraData:        b.ExtraData,
		Nonce:            b.Nonce,
		Size:             b.Size,
		TransactionCount: uint32(b.TransactionCount),
	}
}

func mapBlocks(rows []types.Block) []*indexerv1.Block {
	out := make([]*indexerv1.Block, len(rows))
	for i := range rows {
		out[i] = mapBlock(&rows[i])
	}
	return out
}

// ----- Transaction -----

// mapTxCategories translates the free-form string list used in the existing
// db types to the typed enum used in the proto.
func mapTxCategories(cats []string) []indexerv1.TransactionCategory {
	if len(cats) == 0 {
		return nil
	}
	out := make([]indexerv1.TransactionCategory, 0, len(cats))
	for _, c := range cats {
		switch c {
		case "coin_transfer":
			out = append(out, indexerv1.TransactionCategory_TRANSACTION_CATEGORY_COIN_TRANSFER)
		case "contract_creation":
			out = append(out, indexerv1.TransactionCategory_TRANSACTION_CATEGORY_CONTRACT_CREATION)
		case "contract_call":
			out = append(out, indexerv1.TransactionCategory_TRANSACTION_CATEGORY_CONTRACT_CALL)
		case "token_transfer":
			out = append(out, indexerv1.TransactionCategory_TRANSACTION_CATEGORY_TOKEN_TRANSFER)
		}
	}
	return out
}

func mapTxStatus(s int) indexerv1.TransactionStatus {
	switch s {
	case 1:
		return indexerv1.TransactionStatus_TRANSACTION_STATUS_SUCCESS
	case 0:
		return indexerv1.TransactionStatus_TRANSACTION_STATUS_FAILED
	default:
		return indexerv1.TransactionStatus_TRANSACTION_STATUS_UNSPECIFIED
	}
}

func mapTransaction(tx *types.Transaction) *indexerv1.Transaction {
	if tx == nil {
		return nil
	}
	methodID := ""
	if len(tx.InputData) >= 10 { // "0x" + 8 hex chars
		methodID = tx.InputData[:10]
	}
	return &indexerv1.Transaction{
		Hash:                  tx.Hash,
		BlockNumber:           tx.BlockNumber,
		BlockTimestamp:        unixToProto(tx.BlockTimestamp),
		TransactionIndex:      uint32(tx.TxIndex),
		From:                  tx.From,
		To:                    strOrEmpty(tx.To),
		Value:                 bigIntFromString(string(tx.Value)),
		Gas:                   uint64(0), // gas field removed from types.Transaction; populate if extended
		GasUsed:               tx.GasUsed,
		GasPrice:              bigIntFromString(strconv.FormatUint(tx.GasPrice, 10)),
		EffectiveGasPrice:     &indexerv1.BigInt{}, // not tracked on types.Transaction
		MaxFeePerGas:          bigIntFromUint64Ptr(tx.MaxFeePerGas),
		MaxPriorityFeePerGas:  bigIntFromUint64Ptr(tx.MaxPriorityFeePerGas),
		Nonce:                 ptrUint64OrZero(tx.Nonce),
		Input:                 tx.InputData,
		MethodId:              methodID,
		Status:                mapTxStatus(tx.Status),
		TxType:                uint32(tx.TxType),
		CumulativeGasUsed:     0, // not tracked on types.Transaction
		ContractAddress:       strOrEmpty(tx.ContractAddress),
		Categories:            mapTxCategories(tx.TxCategories),
	}
}

func mapTransactions(rows []types.Transaction) []*indexerv1.Transaction {
	out := make([]*indexerv1.Transaction, len(rows))
	for i := range rows {
		out[i] = mapTransaction(&rows[i])
	}
	return out
}

func ptrUint64OrZero(p *uint64) uint64 {
	if p == nil {
		return 0
	}
	return *p
}

// ----- Log -----

func mapLog(l *types.Log) *indexerv1.Log {
	if l == nil {
		return nil
	}
	topics := []string{}
	for _, t := range []*string{l.Topic0, l.Topic1, l.Topic2, l.Topic3} {
		if t != nil && *t != "" {
			topics = append(topics, *t)
		}
	}
	return &indexerv1.Log{
		BlockNumber:      l.BlockNumber,
		BlockTimestamp:   unixPtrToProto(l.Timestamp),
		TransactionHash:  l.TxHash,
		LogIndex:         uint32(l.LogIndex),
		Address:          l.Address,
		Topics:           topics,
		Data:             l.Data,
		Removed:          l.Removed,
	}
}

func mapLogs(rows []types.Log) []*indexerv1.Log {
	out := make([]*indexerv1.Log, len(rows))
	for i := range rows {
		out[i] = mapLog(&rows[i])
	}
	return out
}

// ----- Token -----

func mapTokenType(s string) indexerv1.TokenType {
	switch s {
	case "ERC20", "erc20":
		return indexerv1.TokenType_TOKEN_TYPE_ERC20
	case "ERC721", "erc721":
		return indexerv1.TokenType_TOKEN_TYPE_ERC721
	case "ERC1155", "erc1155":
		return indexerv1.TokenType_TOKEN_TYPE_ERC1155
	default:
		return indexerv1.TokenType_TOKEN_TYPE_UNSPECIFIED
	}
}

func mapToken(t *types.Token) *indexerv1.Token {
	if t == nil {
		return nil
	}
	name := ""
	if t.Name != nil {
		name = *t.Name
	}
	priceUSD := ""
	if t.USDPrice != nil {
		priceUSD = strconv.FormatFloat(*t.USDPrice, 'f', -1, 64)
	}
	return &indexerv1.Token{
		Address:       t.Address,
		Name:          name,
		Symbol:        t.Symbol,
		Decimals:      uint32(t.Decimals),
		TotalSupply:   bigIntFromPtrString(t.TotalSupply),
		TokenType:     mapTokenType(t.TokenType),
		HolderCount:   uint64(t.HolderCount),
		TransferCount: uint64(t.TransferCount),
		IconUrl:       strOrEmpty(t.IconURL),
		PriceUsd:      priceUSD,
	}
}

func mapTokens(rows []types.Token) []*indexerv1.Token {
	out := make([]*indexerv1.Token, len(rows))
	for i := range rows {
		out[i] = mapToken(&rows[i])
	}
	return out
}

// ----- TokenTransfer -----

func mapTokenTransfer(t *types.TokenTransfer) *indexerv1.TokenTransfer {
	if t == nil {
		return nil
	}
	var tokenID *indexerv1.BigInt
	if t.TokenID != nil {
		tokenID = &indexerv1.BigInt{Value: *t.TokenID}
	} else {
		tokenID = &indexerv1.BigInt{}
	}
	return &indexerv1.TokenTransfer{
		TransactionHash: t.TxHash,
		LogIndex:        uint32(t.LogIndex),
		BlockNumber:     t.BlockNumber,
		BlockTimestamp:  unixPtrToProto(t.Timestamp),
		TokenAddress:    t.TokenAddress,
		TokenType:       mapTokenType(t.TokenType),
		From:            t.From,
		To:              t.To,
		Value:           bigIntFromString(string(t.Value)),
		TokenId:         tokenID,
	}
}

func mapTokenTransfers(rows []types.TokenTransfer) []*indexerv1.TokenTransfer {
	out := make([]*indexerv1.TokenTransfer, len(rows))
	for i := range rows {
		out[i] = mapTokenTransfer(&rows[i])
	}
	return out
}

// ----- Balance / Holder -----

func mapBalance(b *types.Balance) *indexerv1.TokenBalance {
	if b == nil {
		return nil
	}
	return &indexerv1.TokenBalance{
		Address:      b.Address,
		TokenAddress: b.TokenAddress,
		Balance:      bigIntFromString(string(b.Balance)),
		// TokenType unknown from types.Balance; left UNSPECIFIED. Callers that need
		// it should fetch GetToken(token_address) alongside.
	}
}

func mapBalances(rows []types.Balance) []*indexerv1.TokenBalance {
	out := make([]*indexerv1.TokenBalance, len(rows))
	for i := range rows {
		out[i] = mapBalance(&rows[i])
	}
	return out
}

func mapHolder(h *types.TokenHolder) *indexerv1.TokenBalance {
	if h == nil {
		return nil
	}
	return &indexerv1.TokenBalance{
		Address:      h.Address,
		TokenAddress: "", // caller knows the token address from request context
		Balance:      bigIntFromString(string(h.Balance)),
	}
}

func mapHolders(rows []types.TokenHolder) []*indexerv1.TokenBalance {
	out := make([]*indexerv1.TokenBalance, len(rows))
	for i := range rows {
		out[i] = mapHolder(&rows[i])
	}
	return out
}

// ----- AddressStats -----

func mapAddressStats(a *types.AddressStats) *indexerv1.Address {
	if a == nil {
		return nil
	}
	return &indexerv1.Address{
		Address:          a.Address,
		IsContract:       a.IsContract,
		TxCountIn:        uint64(a.TxCount), // schema doesn't split in/out; full total stored as TxCount
		TxCountOut:       0,
		TokenCount:       uint64(a.TokenTransferCount), // closest available proxy
		FirstSeenAt:      unixPtrToProto(a.FirstSeen),
		LastSeenAt:       unixPtrToProto(a.LastSeen),
		FirstSeenBlock:   ptrUint64OrZero(a.FirstSeen),
		LastSeenBlock:    ptrUint64OrZero(a.LastSeen),
		NativeBalance:    &indexerv1.BigInt{}, // not tracked by this store; consumers can fetch balance via separate mechanism
	}
}

func mapAddressStatsList(rows []types.AddressStats) []*indexerv1.Address {
	out := make([]*indexerv1.Address, len(rows))
	for i := range rows {
		out[i] = mapAddressStats(&rows[i])
	}
	return out
}

// ----- Contract -----

func mapContract(c *types.Contract) *indexerv1.Contract {
	if c == nil {
		return nil
	}
	// Chain-facts only: ABI, source, verified flag are NOT exposed. See docs/API.md.
	return &indexerv1.Contract{
		Address:              c.Address,
		Deployer:             c.Creator,
		DeploymentTxHash:     c.CreationTx,
		DeploymentBlock:      c.BlockNumber,
		DeployedAt:           timeToProto(c.CreatedAt),
		Bytecode:             c.Bytecode,
		// Proxy fields not tracked in the existing schema; left zero. A future
		// migration adds them from the proxy detection already performed in
		// Open Privacy Suite's bytecode analyzer.
	}
}

// ----- InternalTransaction -----

func mapInternalTx(it *types.InternalTransaction) *indexerv1.InternalTransaction {
	if it == nil {
		return nil
	}
	return &indexerv1.InternalTransaction{
		TransactionHash: it.TxHash,
		BlockNumber:     it.BlockNumber,
		BlockTimestamp:  unixPtrToProto(it.Timestamp),
		TraceAddress:    it.TraceAddress,
		CallType:        it.CallType,
		From:            it.From,
		To:              strOrEmpty(it.To),
		Value:           bigIntFromString(string(it.Value)),
		Gas:             ptrUint64OrZero(it.Gas),
		GasUsed:         ptrUint64OrZero(it.GasUsed),
		Input:           strOrEmpty(it.Input),
		Output:          strOrEmpty(it.Output),
		Error:           strOrEmpty(it.Error),
	}
}

func mapInternalTxs(rows []types.InternalTransaction) []*indexerv1.InternalTransaction {
	out := make([]*indexerv1.InternalTransaction, len(rows))
	for i := range rows {
		out[i] = mapInternalTx(&rows[i])
	}
	return out
}

// ----- Stats -----

func mapSyncStatus(s *types.SyncStatus) *indexerv1.SyncStatus {
	if s == nil {
		return nil
	}
	return &indexerv1.SyncStatus{
		LatestIndexedBlock: s.LastIndexedBlock,
		IsSyncing:          s.IsSyncing,
		LastUpdatedAt:      timeToProto(s.UpdatedAt),
		// LatestChainBlock and GapCount populated by handler if relevant.
	}
}

func mapChainStats(c *types.ChainStats) *indexerv1.ChainStats {
	if c == nil {
		return nil
	}
	return &indexerv1.ChainStats{
		TotalBlocks:        uint64(c.TotalBlocks),
		TotalTransactions:  uint64(c.TotalTransactions),
		TotalAddresses:     uint64(c.TotalAddresses),
		TotalContracts:     0, // not in types.ChainStats; populate if extended
		AvgBlockTimeSeconds: float32(c.AvgBlockTime),
	}
}

func mapTxHistory(points []types.TxHistoryPoint) *indexerv1.TransactionHistory {
	buckets := make([]*indexerv1.TransactionHistoryBucket, len(points))
	for i, p := range points {
		buckets[i] = &indexerv1.TransactionHistoryBucket{
			BucketStart:      unixToProto(p.Timestamp),
			TransactionCount: uint64(p.Count),
		}
	}
	return &indexerv1.TransactionHistory{Buckets: buckets}
}

func mapDailyStats(rows []types.DailyStats) []*indexerv1.DailyStats {
	out := make([]*indexerv1.DailyStats, len(rows))
	for i := range rows {
		r := rows[i]
		out[i] = &indexerv1.DailyStats{
			Date:            r.Date,
			Transactions:    uint64(r.TotalTransactions),
			NewAddresses:    uint64(r.NewAddresses),
			ActiveAddresses: uint64(r.ActiveAddresses),
			Blocks:          uint64(r.TotalBlocks),
			GasUsed:         uint64(r.TotalGasUsed),
			TotalFees:       &indexerv1.BigInt{}, // schema has avg_gas_price only; total fees not stored directly

			CumulativeTransactions: uint64(r.CumulativeTransactions),
			CumulativeAddresses:    uint64(r.CumulativeAddresses),
			CumulativeContracts:    uint64(r.CumulativeContracts),
			SuccessCount:           uint64(r.SuccessfulTxs),
			FailedCount:            uint64(r.FailedTxs),
			NewContracts:           uint64(r.NewContracts),
			TokenTransferCount:     uint64(r.TokenTransferCount),
			AvgBlockTime:           r.AvgBlockTime,
			AvgBlockSize:           uint64(r.AvgBlockSize),
		}
	}
	return out
}

// ----- OP Deposits -----

func mapOPDeposit(d *types.OPDeposit) *indexerv1.OPDeposit {
	if d == nil {
		return nil
	}
	return &indexerv1.OPDeposit{
		L1TransactionHash:  d.L1TxHash,
		L2TransactionHash:  d.L2TxHash,
		L1BlockNumber:      d.L1BlockNumber,
		L1BlockTimestamp:   unixPtrToProto(d.L1BlockTimestamp),
		From:               d.L1TxOrigin,
		// L2 block, value, data, gas_limit, is_creation not tracked in current
		// types.OPDeposit; left zero. Extend types.OPDeposit + schema when those
		// are added.
	}
}
