package config

import "testing"

// With no setting, realtime walks a backlog as 0.4.0 did. Catch-up fills the blocks a jump skips with
// every row, address_stats included, but the jump is unproven at scale, so it is opt-in.
func TestRealtimeJumpIsOptIn(t *testing.T) {
	t.Setenv("RPC_URL", "http://node")
	t.Setenv("DATABASE_URL", "postgres://db")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RealtimeMaxBehind != 0 {
		t.Fatalf("REALTIME_MAX_BEHIND_BLOCKS unset: got %d, want 0 (no jump)", cfg.RealtimeMaxBehind)
	}
	t.Setenv("REALTIME_MAX_BEHIND_BLOCKS", "1000")
	if cfg, err = Load(); err != nil {
		t.Fatal(err)
	}
	if cfg.RealtimeMaxBehind != 1000 {
		t.Fatalf("REALTIME_MAX_BEHIND_BLOCKS=1000: got %d", cfg.RealtimeMaxBehind)
	}
}
