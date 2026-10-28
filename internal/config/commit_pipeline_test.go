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
	if beside != 4+2 {
		t.Fatalf("connections beside the pipeline = %d, want 4 balance flushes + the daily stats advance + the progress reads", beside)
	}

	err := pipelineConfig(5, "10").CheckCommitPipeline()
	if err == nil {
		t.Fatal("COMMIT_PIPELINE=5 with pool_max_conns=10 and 6 connections beside it was accepted")
	}
	for _, want := range []string{"COMMIT_PIPELINE=5", "pool_max_conns", "10", "11", "BALANCE_WORKERS=4", "at most 4"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}

	if err := pipelineConfig(4, "10").CheckCommitPipeline(); err != nil {
		t.Fatalf("COMMIT_PIPELINE=4 fits pool_max_conns=10 exactly: %v", err)
	}

	if err := pipelineConfig(1, "1").CheckCommitPipeline(); err != nil {
		t.Fatalf("COMMIT_PIPELINE=1 must pass with any pool: %v", err)
	}

	noAsync := pipelineConfig(8, "10")
	noAsync.EnableAsyncBalance = false
	if err := noAsync.CheckCommitPipeline(); err != nil {
		t.Fatalf("without async balance workers, COMMIT_PIPELINE=8 fits pool_max_conns=10: %v", err)
	}
	noAsync.CommitPipelineDepth = 9
	if err := noAsync.CheckCommitPipeline(); err == nil || strings.Contains(err.Error(), "BALANCE_WORKERS") {
		t.Fatalf("without async balance workers, COMMIT_PIPELINE=9 with pool_max_conns=10 must be refused without "+
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
