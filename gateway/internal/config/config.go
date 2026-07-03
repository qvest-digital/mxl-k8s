package config

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/qvest-digital/go-mxl/fabrics"
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

	// Providers is the set of libmxl-fabrics providers this gateway is
	// configured to support. Real per-provider probing happens at the
	// first flow setup; the gateway publishes this list to
	// MxlNodeCapabilities at boot.
	Providers []fabrics.Provider

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

	// DegradedAfter is the inactivity window the target-side
	// reconciler uses to demote a Ready mirror to Degraded and to
	// invalidate its Reconcile fast-path. Matches the operator-side
	// MxlFlowMirror freshness expectation.
	DegradedAfter time.Duration
}

// FromFlags populates a Config from command-line flags.
func FromFlags(fs *flag.FlagSet, args []string) (*Config, error) {
	c := &Config{}
	var providers string
	fs.StringVar(&c.NodeName, "node-name", os.Getenv("NODE_NAME"),
		"Kubernetes node name (defaults to $NODE_NAME).")
	fs.StringVar(&c.DomainPath, "domain-path", os.Getenv("MXL_DOMAIN"),
		"Absolute path to the MXL domain directory the gateway operates on.")
	fs.StringVar(&c.BindAddress, "bind-address", os.Getenv("POD_IP"),
		"Local address libmxl-fabrics endpoints bind to (defaults to $POD_IP, empty for all interfaces).")
	fs.StringVar(&providers, "providers", "tcp",
		"Comma-separated libmxl-fabrics providers to advertise (any,tcp,verbs,efa,shm; auto is an alias for any).")
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
	fs.DurationVar(&c.DegradedAfter, "degraded-after", 10*time.Second,
		"Grain-commit inactivity after which the target gateway demotes a mirror to Degraded.")
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
	if len(c.Providers) == 0 {
		return fmt.Errorf("--providers must list at least one provider")
	}
	return nil
}
