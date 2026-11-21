package main

import (
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/mxl-k8s/exporter/internal/numa"
	"github.com/qvest-digital/mxl-k8s/exporter/internal/wanted"
)

// The waits for missing flows concern how flows are found, not where
// their pages are: they are scanned whether or not the NUMA counters
// are asked for.
func TestWantedCountersAreScannedWithoutNumaMetrics(t *testing.T) {
	off := mxlfsCounters(false, logr.Discard())
	require.Len(t, off, 1)
	require.IsType(t, &wanted.Collector{}, off[0])

	on := mxlfsCounters(true, logr.Discard())
	require.Len(t, on, 2)
	require.IsType(t, &wanted.Collector{}, on[0])
	require.IsType(t, &numa.Collector{}, on[1])
}
