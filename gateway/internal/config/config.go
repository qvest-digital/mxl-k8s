package config

import (
	"flag"
	"fmt"
	"net/netip"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/qvest-digital/go-mxl/fabrics"

	"github.com/qvest-digital/mxl-k8s/gateway/internal/domaingc"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/fabric"
)

// Config holds the gateway's runtime configuration.
type Config struct {
	// NodeName is the Kubernetes node hosting this gateway. Sourced
	// from --node-name or the NODE_NAME env var.
	NodeName string

	// DomainPath is the absolute path of the MXL domain directory this
	// gateway operates on. One mxl.Instance is opened against this
	// directory at boot and shared across all flows.
	DomainPath string

	// BindAddress is the libmxl-fabrics endpoint Node passed to each
	// Target/Initiator Setup. Empty lets libmxl-fabrics bind to all
	// interfaces; for in-cluster use this is typically $POD_IP.
	BindAddress string

	// Providers is the upper bound on the libmxl-fabrics providers
	// this gateway may advertise and use. What it actually advertises
	// is the intersection of this list with what libmxl-fabrics
	// enumerates on the node, so a provider named here whose hardware
	// is absent is published with a zero device count rather than
	// claimed. Naming fabrics.ProviderAny imposes no bound.
	Providers []fabrics.Provider

	// FabricCIDRs restricts the interfaces this gateway may advertise
	// and bind to those whose address falls inside one of these
	// prefixes. Empty imposes no restriction.
	FabricCIDRs []netip.Prefix

	// FabricDevices restricts those interfaces to the devices named
	// here, as the provider names them. Empty imposes no restriction.
	FabricDevices []string

	// FabricMinLinkSpeed rejects interfaces below this link speed in
	// bits per second. Zero imposes no restriction.
	FabricMinLinkSpeed uint64

	// Kubeconfig is an optional kubeconfig path; empty uses in-cluster.
	Kubeconfig string

	// ProbeAddr is the address for http liveness/readiness probes.
	ProbeAddr string

	// MetricsAddr is the address for prometheus metrics.
	MetricsAddr string

	// PprofAddr is the address the net/http/pprof endpoint binds to.
	// Empty disables the endpoint. The chart's values.schema.json
	// constrains this to loopback (127.0.0.1: or localhost:) so an
	// operator with multi-NIC pods cannot accidentally expose pprof.
	PprofAddr string

	// ResyncPeriod is how often the gateway refreshes
	// MxlNodeCapabilities status.
	ResyncPeriod time.Duration

	// ProbePeriod is the shortest interval between two libmxl-fabrics
	// enumerations. The status still refreshes on ResyncPeriod.
	ProbePeriod time.Duration

	// PacingFraction is how much of a grain's own edit-rate interval
	// the source gateway spreads that grain's transmission over. It
	// caps peak rate at the flow's own rate divided by this, and costs
	// up to this much of a grain interval in added latency. Negative
	// disables pacing and restores whole-grain transfers.
	PacingFraction float64

	// PacingChunks is how many slice ranges a paced grain is split
	// into. Under 2 disables pacing.
	PacingChunks int

	// DegradedAfter is the inactivity window the target-side
	// reconciler uses to demote a Ready mirror to Degraded and to
	// invalidate its Reconcile fast-path. Matches the operator-side
	// MxlFlowMirror freshness expectation.
	DegradedAfter time.Duration

	// DomainGCInterval is how often the gateway asks libmxl to reclaim
	// flow directories no writer holds. Zero disables the sweep.
	DomainGCInterval time.Duration

	// DomainGCGrace is how long after start the first sweep is held
	// back, so a restart does not collect mirror copies the target
	// reconciler is about to re-establish.
	DomainGCGrace time.Duration

	// ReaderStallAfter is how long the source-side reconciler lets a
	// reader report an unchanged head index, having never transferred
	// a grain, before SourceProgress reports ReaderNotAdvancing.
	ReaderStallAfter time.Duration

	// KubeAPIQPS is the sustained request rate the Kubernetes API
	// client allows before throttling. The per-mirror status flushers
	// publish roughly one PATCH per second per flowing mirror, so the
	// sustained write rate scales with the flowing mirrors on the
	// node. client-go falls back to 5 QPS when the limit is unset,
	// which saturates at a handful of mirrors and queues status
	// writes behind second-long delays - stale LastSentAt/LastGrainAt
	// then skews the cross-side stuck-handshake comparison.
	KubeAPIQPS float64

	// KubeAPIBurst is the burst ceiling of the Kubernetes API client
	// rate limiter.
	KubeAPIBurst int
}

// FromFlags populates a Config from command-line flags.
func FromFlags(fs *flag.FlagSet, args []string) (*Config, error) {
	c := &Config{}
	var providers, fabricCIDRs, fabricDevices, fabricMinLinkSpeed string
	fs.StringVar(&c.NodeName, "node-name", os.Getenv("NODE_NAME"),
		"Kubernetes node name (defaults to $NODE_NAME).")
	fs.StringVar(&c.DomainPath, "domain-path", os.Getenv("MXL_DOMAIN"),
		"Absolute path to the MXL domain directory the gateway operates on.")
	fs.StringVar(&c.BindAddress, "bind-address", os.Getenv("POD_IP"),
		"Local address libmxl-fabrics endpoints bind to (defaults to $POD_IP, empty for all interfaces).")
	fs.StringVar(&providers, "providers", "any",
		"Comma-separated upper bound on the libmxl-fabrics providers to advertise and use "+
			"(any,tcp,verbs,efa,shm; auto is an alias for any). What the node advertises is "+
			"this list intersected with what libmxl-fabrics finds on it.")
	fs.StringVar(&fabricCIDRs, "fabric-cidr", "",
		"Comma-separated CIDRs the fabric interfaces must fall inside. Empty considers every "+
			"address libmxl-fabrics reports. Set this on a node whose other NICs carry traffic "+
			"MXL must stay off, such as an ST 2110 or management network.")
	fs.StringVar(&fabricDevices, "fabric-device", "",
		"Comma-separated device names the fabric interfaces must match, as the provider names "+
			"them (the kernel netdev name for tcp). Empty considers every device.")
	fs.StringVar(&fabricMinLinkSpeed, "fabric-min-link-speed", "",
		"Minimum link speed a fabric interface must report, as a quantity in bits per second "+
			"(for example 25G). Empty imposes no floor. An interface whose provider reports no "+
			"link speed is rejected when this is set, which includes most interfaces with no "+
			"physical NIC behind them.")
	fs.StringVar(&c.Kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"),
		"Path to a kubeconfig file. Empty uses the in-cluster config.")
	fs.StringVar(&c.ProbeAddr, "health-probe-bind-address", ":8081",
		"Address the health probe endpoint binds to.")
	fs.StringVar(&c.MetricsAddr, "metrics-bind-address", ":8080",
		"Address the metrics endpoint binds to.")
	fs.StringVar(&c.PprofAddr, "pprof-bind-address", "",
		"Address the net/http/pprof endpoint binds to. Empty disables. "+
			"Must be a loopback bind (127.0.0.1: or localhost:); use "+
			"kubectl port-forward to reach it.")
	fs.DurationVar(&c.ResyncPeriod, "resync-period", 30*time.Second,
		"How often to refresh MxlNodeCapabilities status.")
	fs.DurationVar(&c.ProbePeriod, "probe-period", 5*time.Minute,
		"Shortest interval between two libmxl-fabrics interface enumerations. The status "+
			"still refreshes every --resync-period from the last result. Each enumeration "+
			"sweeps every provider libfabric was built with and warns about those it finds "+
			"no device for.")
	fs.Float64Var(&c.PacingFraction, "pacing-fraction", 0.5,
		"Fraction of a grain's own interval to spread that grain's transmission over. "+
			"Caps peak rate at the flow's rate divided by this, and costs up to this much "+
			"of a grain interval in latency. Negative disables pacing.")
	fs.IntVar(&c.PacingChunks, "pacing-chunks", 8,
		"Number of slice ranges a paced grain is split into. Higher shortens the burst "+
			"further at the cost of one cgo call and one RMA write per plane each. "+
			"Under 2 disables pacing.")
	fs.DurationVar(&c.DegradedAfter, "degraded-after", 10*time.Second,
		"Grain-commit inactivity after which the target gateway demotes a mirror to Degraded.")
	fs.DurationVar(&c.DomainGCInterval, "domain-gc-interval", domaingc.DefaultInterval,
		"How often to reclaim flow directories no writer holds. Zero disables it.")
	fs.DurationVar(&c.DomainGCGrace, "domain-gc-grace", domaingc.DefaultGrace,
		"How long after start to hold back the first reclaim sweep.")
	fs.DurationVar(&c.ReaderStallAfter, "reader-stall-after", 20*time.Second,
		"Head-index inactivity after which the source gateway reports a reader that has never transferred a grain as not advancing.")
	fs.Float64Var(&c.KubeAPIQPS, "kube-api-qps", 50,
		"Sustained Kubernetes API request rate allowed before client-side throttling.")
	fs.IntVar(&c.KubeAPIBurst, "kube-api-burst", 100,
		"Burst ceiling of the Kubernetes API client rate limiter.")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	for _, name := range strings.Split(providers, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		// fabrics.ParseProvider accepts concrete provider names only;
		// the any sentinel has no parseable string form. "any" is its
		// String() form, "auto" the pre-v1.1 libmxl-fabrics name.
		if name == "any" || name == "auto" {
			c.Providers = append(c.Providers, fabrics.ProviderAny)
			continue
		}
		p, err := fabrics.ParseProvider(name)
		if err != nil {
			return nil, fmt.Errorf("--providers: %w", err)
		}
		c.Providers = append(c.Providers, p)
	}

	for _, cidr := range splitList(fabricCIDRs) {
		prefix, err := netip.ParsePrefix(cidr)
		if err != nil {
			return nil, fmt.Errorf("--fabric-cidr: %w", err)
		}
		c.FabricCIDRs = append(c.FabricCIDRs, prefix)
	}
	c.FabricDevices = splitList(fabricDevices)

	if fabricMinLinkSpeed != "" {
		q, err := resource.ParseQuantity(fabricMinLinkSpeed)
		if err != nil {
			return nil, fmt.Errorf("--fabric-min-link-speed: %w", err)
		}
		bits, ok := q.AsInt64()
		if !ok || bits < 0 {
			return nil, fmt.Errorf("--fabric-min-link-speed must be a non-negative whole number of bits per second, got %q", fabricMinLinkSpeed)
		}
		c.FabricMinLinkSpeed = uint64(bits)
	}

	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// splitList parses a comma-separated flag value, dropping empty
// entries so a trailing comma or an empty flag yields no entries.
func splitList(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// Selector is the fabric interface selection this configuration
// describes. The capability publisher and the mirror setup path both
// take it, which is what keeps a node from advertising an interface it
// would refuse to bind.
func (c *Config) Selector() fabric.Selector {
	return fabric.Selector{
		CIDRs:        c.FabricCIDRs,
		Devices:      c.FabricDevices,
		MinLinkSpeed: c.FabricMinLinkSpeed,
	}
}

// Validate checks required fields.
func (c *Config) Validate() error {
	if c.NodeName == "" {
		return fmt.Errorf("--node-name (or $NODE_NAME) is required")
	}
	if c.DomainPath == "" {
		return fmt.Errorf("--domain-path (or $MXL_DOMAIN) is required")
	}
	if c.DomainPath[0] != '/' {
		return fmt.Errorf("--domain-path must be absolute, got %q", c.DomainPath)
	}
	if len(c.Providers) == 0 {
		return fmt.Errorf("--providers must list at least one provider")
	}
	if c.ProbePeriod < 0 {
		return fmt.Errorf("--probe-period must not be negative, got %v", c.ProbePeriod)
	}
	if c.KubeAPIQPS <= 0 {
		return fmt.Errorf("--kube-api-qps must be positive, got %v", c.KubeAPIQPS)
	}
	if c.KubeAPIBurst <= 0 {
		return fmt.Errorf("--kube-api-burst must be positive, got %d", c.KubeAPIBurst)
	}
	// A fraction at or above 1 spreads a grain over its whole interval
	// or beyond, so its transmission runs into the next grain's and the
	// mirror never catches up. Disabling is spelled with a negative
	// value, which keeps zero meaning "unset" for the reconciler.
	if c.PacingFraction >= 1 {
		return fmt.Errorf("--pacing-fraction must be below 1, got %v", c.PacingFraction)
	}
	return nil
}
