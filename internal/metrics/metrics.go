package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Registry is the global Prometheus registry.
var Registry = prometheus.NewRegistry()

// IngestorMetrics holds all metrics for the ingestor service.
type IngestorMetrics struct {
	BlocksProcessed prometheus.Counter
	ReorgsDetected  prometheus.Counter
	LatestBlock     prometheus.Gauge
}

// NewIngestorMetrics creates and registers the ingestor's metrics.
func NewIngestorMetrics(reg prometheus.Registerer) *IngestorMetrics {
	return &IngestorMetrics{
		BlocksProcessed: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "forkguard_ingestor_blocks_processed_total",
			Help: "The total number of blocks processed by the ingestor.",
		}),
		ReorgsDetected: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "forkguard_ingestor_reorgs_detected_total",
			Help: "The total number of reorgs detected by the ingestor.",
		}),
		LatestBlock: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "forkguard_ingestor_latest_block_processed",
			Help: "The block number of the latest block processed by the ingestor.",
		}),
	}
}

// DispatcherMetrics holds all metrics for the dispatcher service.
type DispatcherMetrics struct {
	WebhooksDelivered *prometheus.CounterVec
}

// NewDispatcherMetrics creates and registers the dispatcher's metrics.
func NewDispatcherMetrics(reg prometheus.Registerer) *DispatcherMetrics {
	return &DispatcherMetrics{
		WebhooksDelivered: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "forkguard_dispatcher_webhook_deliveries_total",
			Help: "The total number of webhook deliveries, labeled by status (success/failed).",
		}, []string{"status"}),
	}
}
