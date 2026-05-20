package grpcserver

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	indexerv1 "github.com/gateway-fm/chain-indexer/gen/go/chain_indexer/v1"
	"github.com/gateway-fm/chain-indexer/internal/db"
)

// testHarness boots a postgres testcontainer, runs migrations, seeds a tiny
// chain, and exposes an in-process gRPC client talking to a freshly-built
// grpcserver.Server via bufconn.
type testHarness struct {
	t       *testing.T
	db      *db.DB
	client  indexerv1.IndexerServiceClient
	conn    *grpc.ClientConn
	cleanup func()
}

func newTestHarness(t *testing.T) *testHarness {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pgC, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Skipf("skipping: postgres testcontainer unavailable (is Docker running?): %v", err)
	}

	connStr, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = pgC.Terminate(context.Background())
		t.Fatalf("failed to get conn string: %v", err)
	}
	database, err := db.New(connStr)
	if err != nil {
		_ = pgC.Terminate(context.Background())
		t.Fatalf("failed to open db: %v", err)
	}
	if err := database.Migrate(); err != nil {
		database.Close()
		_ = pgC.Terminate(context.Background())
		t.Fatalf("failed to migrate: %v", err)
	}
	// Separate pool for direct seeding SQL; the db package only exposes
	// high-level read helpers, not raw Exec.
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		database.Close()
		_ = pgC.Terminate(context.Background())
		t.Fatalf("failed to create seed pool: %v", err)
	}
	seedChain(ctx, t, pool)

	srv, err := New(Config{
		ListenAddr: "bufconn",
	}, database)
	if err != nil {
		pool.Close()
		_ = pgC.Terminate(context.Background())
		t.Fatalf("failed to build grpc server: %v", err)
	}

	listener := bufconn.Listen(1024 * 1024)
	grpcSrv := grpc.NewServer()
	indexerv1.RegisterIndexerServiceServer(grpcSrv, srv)
	go func() { _ = grpcSrv.Serve(listener) }()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		grpcSrv.Stop()
		pool.Close()
		_ = pgC.Terminate(context.Background())
		t.Fatalf("failed to dial bufconn: %v", err)
	}

	cleanup := func() {
		conn.Close()
		grpcSrv.Stop()
		pool.Close()
		database.Close()
		_ = pgC.Terminate(context.Background())
	}

	return &testHarness{
		t:       t,
		db:      database,
		client:  indexerv1.NewIndexerServiceClient(conn),
		conn:    conn,
		cleanup: cleanup,
	}
}

// seedChain inserts a handful of blocks and transactions so read RPCs have
// something to return. Kept minimal; extend as tests require.
func seedChain(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	for i := uint64(1); i <= 5; i++ {
		_, err := pool.Exec(ctx, `
			INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_used, gas_limit, transaction_count, miner, size, difficulty, total_difficulty, nonce, extra_data, state_root, transactions_root, receipts_root)
			VALUES ($1, $2, $3, $4, 21000, 30000000, 1, '0xabc0000000000000000000000000000000000001', 500, '0', '0', '0x0', '', '', '', '')
		`,
			i,
			fmt.Sprintf("0x%064x", i),
			fmt.Sprintf("0x%064x", i-1),
			1_700_000_000+int64(i),
		)
		if err != nil {
			t.Fatalf("seed block %d: %v", i, err)
		}
		_, err = pool.Exec(ctx, `
			INSERT INTO transactions (hash, block_number, block_timestamp, tx_index, from_address, to_address, value, gas_used, gas_price, nonce, tx_type, input_data, status)
			VALUES ($1, $2, $3, 0, '0xaaa0000000000000000000000000000000000001', '0xbbb0000000000000000000000000000000000002', 1000000000000000000, 21000, 1000000000, 0, 0, '0x', 1)
		`,
			fmt.Sprintf("0x%064x", 0x1000+i),
			i,
			1_700_000_000+int64(i),
		)
		if err != nil {
			t.Fatalf("seed tx %d: %v", i, err)
		}
	}
	// sync_status row so GetSyncStatus returns something
	_, err := pool.Exec(ctx, `
		INSERT INTO sync_status (id, last_indexed_block, is_syncing, updated_at)
		VALUES (1, 5, false, NOW())
		ON CONFLICT (id) DO UPDATE SET last_indexed_block = 5, is_syncing = false
	`)
	if err != nil {
		t.Fatalf("seed sync_status: %v", err)
	}
}

func TestGetLatestBlockNumber(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	resp, err := h.client.GetLatestBlockNumber(context.Background(), &indexerv1.Empty{})
	if err != nil {
		t.Fatalf("GetLatestBlockNumber: %v", err)
	}
	if resp.GetNumber() != 5 {
		t.Fatalf("expected latest block 5, got %d", resp.GetNumber())
	}
}

func TestGetBlock_ByNumber(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	resp, err := h.client.GetBlock(context.Background(), &indexerv1.GetBlockRequest{
		Selector: &indexerv1.GetBlockRequest_Number{Number: 3},
	})
	if err != nil {
		t.Fatalf("GetBlock: %v", err)
	}
	if resp.GetNumber() != 3 {
		t.Fatalf("expected block 3, got %d", resp.GetNumber())
	}
	if resp.GetTransactionCount() != 1 {
		t.Fatalf("expected transaction_count=1, got %d", resp.GetTransactionCount())
	}
}

func TestGetBlock_NotFound(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	_, err := h.client.GetBlock(context.Background(), &indexerv1.GetBlockRequest{
		Selector: &indexerv1.GetBlockRequest_Number{Number: 999},
	})
	if err == nil {
		t.Fatal("expected NotFound, got nil")
	}
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", status.Code(err))
	}
}

func TestListBlocks(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	resp, err := h.client.ListBlocks(context.Background(), &indexerv1.ListBlocksRequest{
		Page: &indexerv1.PageRequest{PageSize: 10},
	})
	if err != nil {
		t.Fatalf("ListBlocks: %v", err)
	}
	if got := len(resp.GetBlocks()); got != 5 {
		t.Fatalf("expected 5 blocks, got %d", got)
	}
	// Feed is descending by number.
	if resp.GetBlocks()[0].GetNumber() != 5 {
		t.Fatalf("expected first block=5, got %d", resp.GetBlocks()[0].GetNumber())
	}
}

func TestGetTransaction(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	hash := fmt.Sprintf("0x%064x", 0x1001)
	resp, err := h.client.GetTransaction(context.Background(), &indexerv1.GetTransactionRequest{Hash: hash})
	if err != nil {
		t.Fatalf("GetTransaction: %v", err)
	}
	if resp.GetHash() != hash {
		t.Fatalf("hash mismatch: %q vs %q", resp.GetHash(), hash)
	}
	if resp.GetBlockNumber() != 1 {
		t.Fatalf("expected block_number=1, got %d", resp.GetBlockNumber())
	}
}

func TestListLogs_RequiresFilter(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	_, err := h.client.ListLogs(context.Background(), &indexerv1.ListLogsRequest{})
	if err == nil {
		t.Fatal("expected InvalidArgument for empty filter")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", status.Code(err))
	}
}

func TestOPStackDisabled(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	_, err := h.client.GetOPDeposit(context.Background(), &indexerv1.GetOPDepositRequest{
		Selector: &indexerv1.GetOPDepositRequest_L2TransactionHash{L2TransactionHash: "0xdeadbeef"},
	})
	if err == nil {
		t.Fatal("expected Unavailable when OP-Stack disabled")
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", status.Code(err))
	}
}

func TestGetSyncStatus(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	resp, err := h.client.GetSyncStatus(context.Background(), &indexerv1.Empty{})
	if err != nil {
		t.Fatalf("GetSyncStatus: %v", err)
	}
	if resp.GetLatestIndexedBlock() != 5 {
		t.Fatalf("expected latest_indexed_block=5, got %d", resp.GetLatestIndexedBlock())
	}
}
