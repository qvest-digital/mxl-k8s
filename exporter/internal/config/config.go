package config

import (
	"flag"
	"fmt"
	"os"
	"time"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Config holds the exporter's runtime configuration.
type Config struct {
	// NodeName is the Kubernetes node this exporter runs on. It labels
	// every series, so one flow visible on three nodes yields three
	// distinguishable samples rather than three indistinguishable ones.
	NodeName string

	// DomainPath is the absolute path of the primary MXL domain
	// directory. One mxl.Instance is opened against it at boot, and one
	// against each directory below domains/ next to it as it appears.
	DomainPath string

	// MetricsAddr is the address the /metrics endpoint binds to.
	MetricsAddr string

	// ProbeAddr is the address for http liveness/readiness probes.
	ProbeAddr string

	// ScanPeriod is how often the domain directory is re-listed for
	// flows that appeared or disappeared. Scrapes read the cache this
	// scan maintains, so a scrape never walks the filesystem.
	ScanPeriod time.Duration

	// FlowLifetime is how long a flow's series keep being exported
	// after its directory is gone, with mxl_flow_present at 0. Without
	// it a flow that ends between two scrapes leaves no trace of having
	// stopped, only a gap.
	//
	// It buys an after-image, not a history. Every series a departed
	// flow keeps is a series a consumer has to tell apart from a live
	// one, so the default is one sweep of the gateway's domain garbage
	// collector: long enough that the end of a flow is never missed,
	// short enough that what the metrics describe is the domain as it
	// is now.
	FlowLifetime time.Duration

	// Kubeconfig is an optional kubeconfig path; empty uses in-cluster.
	Kubeconfig string

	// TopologyEnabled publishes mxl_flow_location_info from the
	// MxlFlow CRs, which is what distinguishes a producing node from a
	// consuming one. Disabling it drops that metric and the API reads
	// behind it; the domain metrics are unaffected.
	TopologyEnabled bool
	// MirrorHeartbeats publishes mxl_mirror_heartbeat_age_seconds from
	// the heartbeat Leases of the mirror sides this node's gateway
	// serves. Disabling it drops that metric and the Lease reads behind
	// it.
	MirrorHeartbeats bool
	// LeaseNamespace is the namespace the mirror heartbeat Leases are
	// read from, which the chart sets to the one its Role grants.
	LeaseNamespace string

	// MxlfsNumaMetrics exports where the pages of each mxlfs domain and
	// flow ring are, and what the module's moves did, read on every scan.
	MxlfsNumaMetrics bool
}

// FromFlags populates a Config from command-line flags.
func FromFlags(fs *flag.FlagSet, args []string) (*Config, error) {
	c := &Config{}
	fs.StringVar(&c.NodeName, "node-name", os.Getenv("NODE_NAME"),
		"Kubernetes node name (defaults to $NODE_NAME).")
	fs.StringVar(&c.DomainPath, "domain-path", os.Getenv("MXL_DOMAIN"),
		"Absolute path to the primary MXL domain directory; every directory below domains/ next to it is exported too.")
	fs.StringVar(&c.MetricsAddr, "metrics-bind-address", ":8080",
		"Address the metrics endpoint binds to.")
	fs.StringVar(&c.ProbeAddr, "health-probe-bind-address", ":8081",
		"Address the health probe endpoint binds to.")
	fs.DurationVar(&c.ScanPeriod, "scan-period", 5*time.Second,
		"How often the domain directory is re-listed for flows that appeared or disappeared.")
	fs.DurationVar(&c.FlowLifetime, "flow-lifetime", 5*time.Minute,
		"How long a departed flow keeps being exported with mxl_flow_present at 0.")
	fs.StringVar(&c.Kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"),
		"Path to a kubeconfig file. Empty uses the in-cluster config.")
	fs.BoolVar(&c.TopologyEnabled, "topology", true,
		"Publish mxl_flow_location_info from the MxlFlow CRs, which names the producing node.")
	fs.BoolVar(&c.MirrorHeartbeats, "mirror-heartbeats", true,
		"Publish mxl_mirror_heartbeat_age_seconds from the heartbeat Leases of the mirror sides this node serves.")
	fs.StringVar(&c.LeaseNamespace, "lease-namespace", mxlv1alpha1.LeaseNamespace,
		"Namespace the mirror heartbeat Leases are read from.")
	fs.BoolVar(&c.MxlfsNumaMetrics, "mxlfs-numa-metrics", false,
		"Export the NUMA placement counters of every domain on an mxlfs, read on every scan.")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
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
	if c.ScanPeriod <= 0 {
		return fmt.Errorf("--scan-period must be positive, got %v", c.ScanPeriod)
	}
	if c.MirrorHeartbeats && c.LeaseNamespace == "" {
		return fmt.Errorf("--lease-namespace is required with --mirror-heartbeats")
	}
	if c.FlowLifetime < 0 {
		return fmt.Errorf("--flow-lifetime must not be negative, got %v", c.FlowLifetime)
	}
	return nil
}
