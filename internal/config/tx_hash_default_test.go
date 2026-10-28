package config

import "testing"

// With no setting, the tx_hash lookup indexes are kept, as 0.4.0 had them. Consumers that read logs and
// token_transfers by transaction hash depend on it.
func TestTxHashIndexesAreOnByDefault(t *testing.T) {
	t.Setenv("RPC_URL", "http://node")
	t.Setenv("DATABASE_URL", "postgres://db")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.TxHashIndexes {
		t.Fatal("TX_HASH_INDEXES unset must keep the indexes")
	}
}
