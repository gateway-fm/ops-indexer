package metrics

import (
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var registerPoolOnce sync.Once

// RegisterPoolStats wires postgres connection-pool statistics into the registry.
// It is a call rather than a package-level var because the pool does not exist
// at init time. Only the first call takes effect.
func RegisterPoolStats(stat func() *pgxpool.Stat) {
	if stat == nil {
		return
	}
	registerPoolOnce.Do(func() {
		promauto.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "indexer_db_pool_max_conns",
			Help: "Configured maximum size of the postgres connection pool.",
		}, func() float64 { return float64(stat().MaxConns()) })

		promauto.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "indexer_db_pool_total_conns",
			Help: "Postgres connections in the pool, idle and in use.",
		}, func() float64 { return float64(stat().TotalConns()) })

		promauto.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "indexer_db_pool_acquired_conns",
			Help: "Postgres connections currently checked out.",
		}, func() float64 { return float64(stat().AcquiredConns()) })

		promauto.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "indexer_db_pool_idle_conns",
			Help: "Postgres connections currently idle in the pool.",
		}, func() float64 { return float64(stat().IdleConns()) })

		promauto.NewCounterFunc(prometheus.CounterOpts{
			Name: "indexer_db_pool_acquire_total",
			Help: "Successful postgres connection acquisitions.",
		}, func() float64 { return float64(stat().AcquireCount()) })

		// A rising rate here means the pool is the constraint, not the database.
		promauto.NewCounterFunc(prometheus.CounterOpts{
			Name: "indexer_db_pool_acquire_duration_seconds_total",
			Help: "Time spent waiting to acquire a postgres connection.",
		}, func() float64 { return stat().AcquireDuration().Seconds() })

		promauto.NewCounterFunc(prometheus.CounterOpts{
			Name: "indexer_db_pool_empty_acquire_total",
			Help: "Acquisitions that had to wait because the pool was empty.",
		}, func() float64 { return float64(stat().EmptyAcquireCount()) })
	})
}
