package config

import (
	"strings"
	"testing"
)

func pipelineConfig(depth int, poolMaxConns string) *Config {
	return &Config{
		RPCURL:              "http://node",
		DatabaseURL:         "postgres://db/indexer?pool_max_conns=" + poolMaxConns,
		APIPort:             8080,
		RPCWorkers:          1,
		RPCRateLimit:        1,
		CommitPipelineDepth: depth,
		EnableAsyncBalance:  true,
		BalanceWorkers:      4,
	}
}

// COMMIT_PIPELINE open block writes each hold a connection, beside what the rest of the indexer holds; a pool too
// small for both stops ingest silently, so the setting is refused at startup.
func TestCommitPipelineIsCheckedAgainstThePool(t *testing.T) {
	beside := pipelineConfig(1, "10").ConnectionsBesidePipeline()
	if beside != 4+3 {
		t.Fatalf("connections beside the pipeline = %d, want 4 balance flushes + the daily stats advance + the progress reads + the ledger closer", beside)
	}

	err := pipelineConfig(4, "10").CheckCommitPipeline()
	if err == nil {
		t.Fatal("COMMIT_PIPELINE=4 with pool_max_conns=10 and 7 connections beside it was accepted")
	}
	for _, want := range []string{"COMMIT_PIPELINE=4", "pool_max_conns", "10", "11", "BALANCE_WORKERS=4", "at most 3"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}

	if err := pipelineConfig(3, "10").CheckCommitPipeline(); err != nil {
		t.Fatalf("COMMIT_PIPELINE=3 fits pool_max_conns=10 exactly: %v", err)
	}

	if err := pipelineConfig(1, "1").CheckCommitPipeline(); err != nil {
		t.Fatalf("COMMIT_PIPELINE=1 must pass with any pool: %v", err)
	}

	noAsync := pipelineConfig(7, "10")
	noAsync.EnableAsyncBalance = false
	if err := noAsync.CheckCommitPipeline(); err != nil {
		t.Fatalf("without async balance workers, COMMIT_PIPELINE=7 fits pool_max_conns=10: %v", err)
	}
	noAsync.CommitPipelineDepth = 8
	if err := noAsync.CheckCommitPipeline(); err == nil || strings.Contains(err.Error(), "BALANCE_WORKERS") {
		t.Fatalf("without async balance workers, COMMIT_PIPELINE=8 with pool_max_conns=10 must be refused without "+
			"naming BALANCE_WORKERS, which does not apply: %v", err)
	}
}

// A negative BALANCE_WORKERS would lower the connections kept beside the pipeline, and so let a deeper pipeline pass.
func TestNegativeBalanceWorkersIsRefused(t *testing.T) {
	c := pipelineConfig(1, "10")
	c.BalanceWorkers = -4
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "BALANCE_WORKERS") {
		t.Fatalf("BALANCE_WORKERS=-4 was not refused: %v", err)
	}
}

// Beside the pipeline, the catch-up workers write while realtime pipelines, the ledger closer holds a connection
// for a whole span close, and retention holds one for the statements under the history-DDL gate (the gate's own
// connection is outside the pool).
func TestConnectionsBesidePipelineCountsCatchupLedgerAndRetention(t *testing.T) {
	c := pipelineConfig(1, "10")
	base := c.ConnectionsBesidePipeline()
	if base != 4+2+1 {
		t.Fatalf("connections beside the pipeline = %d, want 4 balance flushes + the daily stats advance + the progress reads + the ledger closer", base)
	}
	c.CatchupEnabled, c.CatchupWorkers = true, 10
	if got := c.ConnectionsBesidePipeline(); got != base+10 {
		t.Fatalf("with 10 catch-up workers: %d, want %d", got, base+10)
	}
	c.PruneRetainBlocks = 100_000
	if got := c.ConnectionsBesidePipeline(); got != base+10+1 {
		t.Fatalf("with retention on: %d, want %d", got, base+11)
	}

	c.CommitPipelineDepth = 8
	err := c.CheckCommitPipeline()
	if err == nil {
		t.Fatal("COMMIT_PIPELINE=8 with pool_max_conns=10 and 18 connections beside it was accepted")
	}
	for _, want := range []string{"CATCHUP_WORKERS=10", "ledger closer", "retention"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}
}

// A production deployment's settings still pass: pool 64, COMMIT_PIPELINE 32, BALANCE_WORKERS 15, CATCHUP_WORKERS 5,
// retention and the ledger on (32 + 15 + 2 + 5 + 1 + 1 = 56 of 64).
func TestCommitPipelineCheckAcceptsAProductionDeployment(t *testing.T) {
	c := pipelineConfig(32, "64")
	c.BalanceWorkers, c.CatchupEnabled, c.CatchupWorkers, c.PruneRetainBlocks = 15, true, 5, 9_000_000
	if got := c.ConnectionsBesidePipeline(); got != 24 {
		t.Fatalf("connections beside the pipeline = %d, want 24", got)
	}
	if err := c.CheckCommitPipeline(); err != nil {
		t.Fatalf("a production deployment's settings were refused: %v", err)
	}
}
