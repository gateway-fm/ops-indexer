package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func gaugeValue(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	var m dto.Metric
	if err := g.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetGauge().GetValue()
}

func TestBackfillGaugesSayWhatHoldsTheBackfillAndRetention(t *testing.T) {
	for _, on := range []bool{true, false} {
		SetStatsBackfillWaiting(on)
		SetPruneBlockedByBackfill(on)
		if got, want := gaugeValue(t, statsBackfillWaiting), boolGauge(on); got != want {
			t.Errorf("indexer_stats_backfill_waiting_for_catchup = %v, want %v", got, want)
		}
		if got, want := gaugeValue(t, pruneBlockedByBackfill), boolGauge(on); got != want {
			t.Errorf("indexer_prune_blocked_by_stats_backfill = %v, want %v", got, want)
		}
	}
}
