package collector

import (
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/qvest-digital/mxl-k8s/exporter/internal/domain"
)

type fakeDomain struct {
	path string
	obs  []domain.Observation
}

func (f fakeDomain) Observe() []domain.Observation { return f.obs }
func (f fakeDomain) Path() string                  { return f.path }
func (f fakeDomain) SizeBytes() uint64             { return 0 }
func (f fakeDomain) Stat() (domain.FSStat, error)  { return domain.FSStat{}, nil }

const flowA = "5d0c4a9e-0000-4000-8000-0000000000a1"
const flowB = "5d0c4a9e-0000-4000-8000-0000000000b1"

// Every domain the list returns reports its flows under its own domain
// label, and the primary's series keep the label they always had.
func TestCollectorsReportEveryDomain(t *testing.T) {
	doms := []fakeDomain{
		{path: "/run/mxl/domain", obs: []domain.Observation{{ID: flowA, Present: true}}},
		{path: "/run/mxl/domains/studio", obs: []domain.Observation{{ID: flowB, Present: true}}},
	}
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(
		NewFlowCollector(func() []Source {
			return []Source{doms[0], doms[1]}
		}),
		NewDomainCollector(func() []DomainSource {
			return []DomainSource{doms[0], doms[1]}
		}, logr.Discard()),
	)

	want := `
# HELP mxl_flow_present 1 while the flow's directory exists in the domain.
# TYPE mxl_flow_present gauge
mxl_flow_present{domain="/run/mxl/domain",flow_id="` + flowA + `"} 1
mxl_flow_present{domain="/run/mxl/domains/studio",flow_id="` + flowB + `"} 1
# HELP mxl_domain_num_flows Flows currently present in the domain.
# TYPE mxl_domain_num_flows gauge
mxl_domain_num_flows{domain="/run/mxl/domain"} 1
mxl_domain_num_flows{domain="/run/mxl/domains/studio"} 1
`
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(want),
		"mxl_flow_present", "mxl_domain_num_flows"))
}
