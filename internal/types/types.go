package types

import (
	"encoding/json"
	"math/big"
	"strings"
	"time"
)

// BigInt wraps big.Int to serialize as a string in JSON
type BigInt struct {
	i *big.Int
}

func NewBigInt(i *big.Int) BigInt {
	if i == nil {
		return BigInt{i: big.NewInt(0)}
	}
	return BigInt{i: i}
}

func (b BigInt) MarshalJSON() ([]byte, error) {
	if b.i == nil {
		return json.Marshal("0")
	}
	return json.Marshal(b.i.String())
}

func (b BigInt) String() string {
	if b.i == nil {
		return "0"
	}
	return b.i.String()
}

// JSONString is a string that always serializes as a JSON string
type JSONString string

func (s JSONString) MarshalJSON() ([]byte, error) {
	escaped := string(s)
	escaped = strings.ReplaceAll(escaped, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `"`, `\"`)
	return []byte(`"` + escaped + `"`), nil
}

type Block struct {
	Number           uint64    `json:"number"`
	Hash             string    `json:"hash"`
	ParentHash       string    `json:"parentHash"`
	Timestamp        uint64    `json:"timestamp"`
	GasUsed          uint64    `json:"gasUsed"`
	GasLimit         uint64    `json:"gasLimit"`
	BaseFeePerGas    *uint64   `json:"baseFeePerGas,omitempty"`
	TransactionCount int       `json:"transactionCount"`
	Size             uint64    `json:"size"`
	Difficulty       string    `json:"difficulty"`
	TotalDifficulty  string    `json:"totalDifficulty"`
	Nonce            string    `json:"nonce"`
	Miner            string    `json:"miner"`
	ExtraData        string    `json:"extraData"`
	StateRoot        string    `json:"stateRoot"`
	TransactionsRoot string    `json:"transactionsRoot"`
	ReceiptsRoot     string    `json:"receiptsRoot"`
	CreatedAt        time.Time `json:"createdAt"`
}

type Transaction struct {
	Hash                  string     `json:"hash"`
	BlockNumber           uint64     `json:"blockNumber"`
	BlockTimestamp        uint64     `json:"blockTimestamp,omitempty"`
	TxIndex               int        `json:"txIndex"`
	From                  string     `json:"from"`
	To                    *string    `json:"to"`
	ContractAddress       *string    `json:"contractAddress,omitempty"`
	Value                 JSONString `json:"value"`
	GasUsed               uint64     `json:"gasUsed"`
	GasPrice              uint64     `json:"gasPrice"`
	GasLimit              *uint64    `json:"gasLimit,omitempty"`
	MaxFeePerGas          *uint64    `json:"maxFeePerGas,omitempty"`
	MaxPriorityFeePerGas  *uint64    `json:"maxPriorityFeePerGas,omitempty"`
	Nonce                 *uint64    `json:"nonce,omitempty"`
	TxType                int        `json:"txType"`
	InputData             string     `json:"inputData"`
	Status                int        `json:"status"`
	Error                 *string    `json:"error,omitempty"`
	RevertReason          *string    `json:"revertReason,omitempty"`
	CreatedAt             time.Time  `json:"createdAt"`
	TxCategories          []string          `json:"txCategories,omitempty"`
	TokenTransferCount    int               `json:"tokenTransferCount,omitempty"`
	AddressMetadata       map[string]string `json:"addressMetadata,omitempty"`
}

type Token struct {
	Address            string     `json:"address"`
	Symbol             string     `json:"symbol"`
	Name               *string    `json:"name,omitempty"`
	Decimals           int        `json:"decimals"`
	TokenType          string     `json:"tokenType"`
	TotalSupply        *string    `json:"totalSupply,omitempty"`
	HolderCount        int        `json:"holderCount"`
	TransferCount      int        `json:"transferCount"`
	USDPrice           *float64   `json:"usdPrice,omitempty"`
	IconURL            *string    `json:"iconUrl,omitempty"`
	L1Address          *string    `json:"l1Address,omitempty"`
	BlockNumber        uint64     `json:"blockNumber"`
	CreationTx         *string    `json:"creationTx,omitempty"`
	OffChainUpdatedAt  *time.Time `json:"offChainUpdatedAt,omitempty"`
	CreatedAt          time.Time  `json:"createdAt"`
}

type TokenTransfer struct {
	ID           int64      `json:"id"`
	TxHash       string     `json:"txHash"`
	LogIndex     int        `json:"logIndex"`
	TokenAddress string     `json:"tokenAddress"`
	From         string     `json:"from"`
	To           string     `json:"to"`
	Value        JSONString `json:"value"`
	BlockNumber  uint64     `json:"blockNumber"`
	Timestamp    *uint64    `json:"timestamp,omitempty"`
	TransferType string     `json:"transferType"`
	TokenType    string     `json:"tokenType"`
	TokenID      *string    `json:"tokenId,omitempty"`
	IsInternal      bool              `json:"isInternal"`
	AddressMetadata map[string]string `json:"addressMetadata,omitempty"`
}

type Balance struct {
	Address      string     `json:"address"`
	TokenAddress string     `json:"tokenAddress"`
	BlockNumber  uint64     `json:"blockNumber"`
	Balance      JSONString `json:"balance"`
}

type Counter struct {
	Address     string    `json:"address"`
	CounterType string    `json:"counterType"`
	Count       int64     `json:"count"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

type AddressStats struct {
	Address            string    `json:"address"`
	TxCount            int       `json:"txCount"`
	InternalTxCount    int       `json:"internalTxCount"`
	TokenTransferCount int       `json:"tokenTransferCount"`
	FirstSeen          *uint64   `json:"firstSeen"`
	LastSeen           *uint64   `json:"lastSeen"`
	IsContract         bool      `json:"isContract"`
	UpdatedAt          time.Time `json:"updatedAt"`
}

type Contract struct {
	Address          string          `json:"address"`
	Bytecode         string          `json:"bytecode"`
	BytecodeHash     *string         `json:"bytecodeHash,omitempty"`
	Creator          string          `json:"creator"`
	CreationTx       string          `json:"creationTx"`
	BlockNumber      uint64          `json:"blockNumber"`
	IsVerified       bool            `json:"isVerified"`
	ContractName     *string         `json:"contractName,omitempty"`
	CompilerVersion  *string         `json:"compilerVersion,omitempty"`
	OptimizationUsed *bool           `json:"optimizationUsed,omitempty"`
	EVMVersion       *string         `json:"evmVersion,omitempty"`
	SourceCode       *string         `json:"sourceCode,omitempty"`
	ABI              json.RawMessage `json:"abi,omitempty"`
	CreatedAt        time.Time       `json:"createdAt"`
	LicenseType      *string         `json:"licenseType,omitempty"`
	ConstructorArgs  *string         `json:"constructorArgs,omitempty"`
	OptimizationRuns *int            `json:"optimizationRuns,omitempty"`
}

type Log struct {
	ID          int64   `json:"id"`
	TxHash      string  `json:"txHash"`
	LogIndex    int     `json:"logIndex"`
	Address     string  `json:"address"`
	Topic0      *string `json:"topic0"`
	Topic1      *string `json:"topic1"`
	Topic2      *string `json:"topic2"`
	Topic3      *string `json:"topic3"`
	Data        string  `json:"data"`
	BlockNumber uint64  `json:"blockNumber"`
	Timestamp   *uint64 `json:"timestamp,omitempty"`
	Removed         bool              `json:"removed"`
	AddressMetadata map[string]string `json:"addressMetadata,omitempty"`
}

type InternalTransaction struct {
	ID           int64      `json:"id"`
	TxHash       string     `json:"txHash"`
	BlockNumber  uint64     `json:"blockNumber"`
	TraceAddress string     `json:"traceAddress"`
	From         string     `json:"from"`
	To           *string    `json:"to"`
	Value        JSONString `json:"value"`
	Gas          *uint64    `json:"gas,omitempty"`
	GasUsed      *uint64    `json:"gasUsed,omitempty"`
	Input        *string    `json:"input,omitempty"`
	Output       *string    `json:"output,omitempty"`
	CallType     string     `json:"callType"`
	Error        *string    `json:"error,omitempty"`
	Timestamp       *uint64           `json:"timestamp,omitempty"`
	AddressMetadata map[string]string `json:"addressMetadata,omitempty"`
}

type SyncStatus struct {
	ID                 int64     `json:"id"`
	LastIndexedBlock   uint64    `json:"lastIndexedBlock"`
	LastVerifiedBlock  *uint64   `json:"lastVerifiedBlock,omitempty"`
	LastFinalizedBlock *uint64   `json:"lastFinalizedBlock,omitempty"`
	IsSyncing          bool      `json:"isSyncing"`
	UpdatedAt          time.Time `json:"updatedAt"`
}

type AddressInfo struct {
	Address            string     `json:"address"`
	Balance            JSONString `json:"balance"`
	TxCount            int        `json:"txCount"`
	InternalTxCount    int        `json:"internalTxCount"`
	TokenTransferCount int        `json:"tokenTransferCount"`
	IsContract         bool       `json:"isContract"`
}

type ChainStats struct {
	TotalBlocks       int64   `json:"totalBlocks"`
	TotalTransactions int64   `json:"totalTransactions"`
	TotalAddresses    int64   `json:"totalAddresses"`
	TotalTokens       int64   `json:"totalTokens"`
	AvgBlockTime      float64 `json:"avgBlockTime"`
	PrivacyEnabled    bool    `json:"privacyEnabled"`
}

type PaginatedResponse[T any] struct {
	Data       []T     `json:"data"`
	NextCursor *string `json:"nextCursor,omitempty"`
	HasMore    bool    `json:"hasMore"`
}

type OffsetPaginatedResponse[T any] struct {
	Data       []T   `json:"data"`
	Total      int64 `json:"total"`
	Page       int   `json:"page"`
	PageSize   int   `json:"pageSize"`
	TotalPages int   `json:"totalPages"`
}

type AccountListItem struct {
	Address    string     `json:"address"`
	Balance    JSONString `json:"balance"`
	TxCount    int        `json:"txCount"`
	IsContract bool       `json:"isContract"`
}

type SearchSuggestion struct {
	Type  string `json:"type"`
	Value string `json:"value"`
	Label string `json:"label"`
}

type SearchSuggestionsResponse struct {
	Query       string             `json:"query"`
	Suggestions []SearchSuggestion `json:"suggestions"`
}

type TxHistoryPoint struct {
	Timestamp uint64 `json:"timestamp"`
	Count     int64  `json:"count"`
}

type TokenHolder struct {
	Address     string     `json:"address"`
	Balance     JSONString `json:"balance"`
	Percentage  float64    `json:"percentage"`
	IsContract      bool              `json:"isContract"`
	AddressMetadata map[string]string `json:"addressMetadata,omitempty"`
}

const (
	TransferTypeTransfer   = "transfer"
	TransferTypeMint       = "mint"
	TransferTypeBurn       = "burn"
	TransferTypeDeposit    = "deposit"
	TransferTypeWithdrawal = "withdrawal"
)

const (
	TokenTypeERC20  = "ERC20"
	TokenTypeERC721 = "ERC721"
	TokenTypeNative = "NATIVE"
)

const (
	CallTypeCall         = "call"
	CallTypeDelegateCall = "delegatecall"
	CallTypeStaticCall   = "staticcall"
	CallTypeCreate       = "create"
	CallTypeCreate2      = "create2"
)

const (
	TxCategoryCoinTransfer      = "coin_transfer"
	TxCategoryContractCall      = "contract_call"
	TxCategoryContractCreation  = "contract_creation"
	TxCategoryTokenTransfer     = "token_transfer"
	TxCategorySystemTransaction = "system_transaction"
)

const (
	TxTypeDeposit = 126 // 0x7E
)

type OPDeposit struct {
	L2TxHash        string  `json:"l2TxHash"`
	L1BlockNumber   uint64  `json:"l1BlockNumber"`
	L1BlockTimestamp *uint64 `json:"l1BlockTimestamp,omitempty"`
	L1TxHash        string  `json:"l1TxHash"`
	L1TxOrigin      string  `json:"l1TxOrigin"`
	CreatedAt       time.Time `json:"createdAt"`
}

type TransactionWithDeposit struct {
	Transaction
	OPDeposit *OPDeposit `json:"opDeposit,omitempty"`
}

type DailyStats struct {
	Date                   string  `json:"date"`
	TotalBlocks            int     `json:"totalBlocks"`
	TotalTransactions      int     `json:"totalTransactions"`
	TotalGasUsed           int64   `json:"totalGasUsed"`
	AvgGasPrice            int64   `json:"avgGasPrice"`
	SuccessfulTxs          int     `json:"successfulTxs"`
	FailedTxs              int     `json:"failedTxs"`
	ActiveAddresses        int     `json:"activeAddresses"`
	NewAddresses           int     `json:"newAddresses"`
	AvgBlockTime           float64 `json:"avgBlockTime"`
	AvgBlockSize           int64   `json:"avgBlockSize"`
	NewContracts           int     `json:"newContracts"`
	TokenTransferCount     int     `json:"tokenTransferCount"`
	CumulativeTransactions int64   `json:"cumulativeTransactions"`
	CumulativeAddresses    int64   `json:"cumulativeAddresses"`
	CumulativeContracts    int64   `json:"cumulativeContracts"`
}

type ChartLineInfo struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Units       string `json:"units,omitempty"`
	Section     string `json:"section"`
}

type ChartDataPoint struct {
	Date  string  `json:"date"`
	Value float64 `json:"value"`
}

type ChartLineResponse struct {
	Info  ChartLineInfo    `json:"info"`
	Chart []ChartDataPoint `json:"chart"`
}
