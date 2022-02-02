package feedservice

import (
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

var registry = prometheus.NewRegistry()

func init() {
	registry.MustRegister(prometheus.NewGoCollector())
	registry.MustRegister(grpc_prometheus.NewClientMetrics())
}
