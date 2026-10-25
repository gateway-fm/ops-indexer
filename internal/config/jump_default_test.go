package config

import "testing"

// In this version address_stats follows the ledger, so the blocks a jump skips are counted from their
// rows when their span closes, and realtime jumps to the head after a stall of 1,000 blocks by default. 0 turns it off.
func TestRealtimeJumpsByDefault(t *testing.T) {
	t.Setenv("RPC_URL", "http://node")
	t.Setenv("DATABASE_URL", "postgres://db")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RealtimeMaxBehind != 1000 {
		t.Fatalf("REALTIME_MAX_BEHIND_BLOCKS unset: got %d, want 1000", cfg.RealtimeMaxBehind)
	}
	t.Setenv("REALTIME_MAX_BEHIND_BLOCKS", "0")
	if cfg, err = Load(); err != nil {
		t.Fatal(err)
	}
	if cfg.RealtimeMaxBehind != 0 {
		t.Fatalf("REALTIME_MAX_BEHIND_BLOCKS=0: got %d, want 0 (no jump)", cfg.RealtimeMaxBehind)
	}
}
