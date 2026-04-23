package chaininfo

import (
	"context"
	"math/big"
	"sync"
	"time"

	"github.com/gateway-fm/chain-indexer/internal/log"
	"github.com/gateway-fm/chain-indexer/internal/rpc"
)

// Info holds cached chain metadata fetched from the node.
type Info struct {
	ChainID         string `json:"chainId"`
	ChainIDDecimal  uint64 `json:"chainIdDecimal"`
	NetworkID       string `json:"networkId"`
	ClientVersion   string `json:"clientVersion"`
	ProtocolVersion string `json:"protocolVersion"`
	LatestBlock     uint64 `json:"latestBlock"`
	GasPrice        string `json:"gasPrice"`
	PeerCount       int    `json:"peerCount"`
	IsSyncing       bool   `json:"isSyncing"`
	GenesisHash     string `json:"genesisHash"`
	UpdatedAt       string `json:"updatedAt"`
}

// Service caches chain info in memory and refreshes periodically.
type Service struct {
	rpc      *rpc.Client
	mu       sync.RWMutex
	cached   *Info
	interval time.Duration
}

// NewService creates a chain info service that refreshes at the given interval.
func NewService(rpcClient *rpc.Client, interval time.Duration) *Service {
	return &Service{
		rpc:      rpcClient,
		interval: interval,
	}
}

// Start fetches chain info immediately, then refreshes on a timer until ctx is cancelled.
func (s *Service) Start(ctx context.Context) {
	s.refresh(ctx)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refresh(ctx)
		}
	}
}

// Get returns the most recently cached chain info.
func (s *Service) Get() *Info {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.cached == nil {
		return &Info{}
	}
	return s.cached
}

func (s *Service) refresh(ctx context.Context) {
	info := &Info{
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	raw := s.rpc.Raw()

	// Chain ID
	if chainID, err := s.rpc.ChainID(ctx); err == nil {
		info.ChainID = "0x" + chainID.Text(16)
		info.ChainIDDecimal = chainID.Uint64()
	}

	// Client version (web3_clientVersion)
	var clientVersion string
	if err := raw.CallContext(ctx, &clientVersion, "web3_clientVersion"); err == nil {
		info.ClientVersion = clientVersion
	}

	// Network ID (net_version)
	var netVersion string
	if err := raw.CallContext(ctx, &netVersion, "net_version"); err == nil {
		info.NetworkID = netVersion
	}

	// Protocol version (eth_protocolVersion)
	var protocolVersion string
	if err := raw.CallContext(ctx, &protocolVersion, "eth_protocolVersion"); err == nil {
		info.ProtocolVersion = protocolVersion
	}

	// Latest block
	if blockNum, err := s.rpc.BlockNumber(ctx); err == nil {
		info.LatestBlock = blockNum
	}

	// Gas price (eth_gasPrice)
	var gasPriceHex string
	if err := raw.CallContext(ctx, &gasPriceHex, "eth_gasPrice"); err == nil {
		if gp, ok := new(big.Int).SetString(gasPriceHex, 0); ok {
			info.GasPrice = gp.String()
		}
	}

	// Peer count (net_peerCount)
	var peerCountHex string
	if err := raw.CallContext(ctx, &peerCountHex, "net_peerCount"); err == nil {
		if pc, ok := new(big.Int).SetString(peerCountHex, 0); ok {
			info.PeerCount = int(pc.Int64())
		}
	}

	// Syncing (eth_syncing) — returns false or an object
	var syncing any
	if err := raw.CallContext(ctx, &syncing, "eth_syncing"); err == nil {
		if b, ok := syncing.(bool); ok {
			info.IsSyncing = b
		} else {
			info.IsSyncing = true // non-false means syncing
		}
	}

	// Genesis block hash
	var genesisBlock struct {
		Hash string `json:"hash"`
	}
	if err := raw.CallContext(ctx, &genesisBlock, "eth_getBlockByNumber", "0x0", false); err == nil {
		info.GenesisHash = genesisBlock.Hash
	}

	s.mu.Lock()
	s.cached = info
	s.mu.Unlock()

	log.Info("chain info refreshed",
		"chainId", info.ChainIDDecimal,
		"client", info.ClientVersion,
		"block", info.LatestBlock,
	)
}
