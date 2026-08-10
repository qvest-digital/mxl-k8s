// Package exporter is the mxl-k8s flow exporter: a per-node DaemonSet
// that reads the node's MXL domain and publishes Prometheus metrics for
// every flow materialized on it.
//
// The metric set and the entry-lifetime model are ported from
// github.com/jonasohland/mxl-exporter (Apache-2.0), which predates the
// public go-mxl bindings and read the domain's shared-memory layout by
// parsing the structures itself. Here the same values come from go-mxl,
// so the layout stays libmxl's to define.
package exporter
