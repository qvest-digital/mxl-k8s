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

	// ResyncPeriod is how often the gateway refreshes
	// MxlNodeCapabilities status.
	ResyncPeriod time.Duration
}

// FromFlags populates a Config from command-line flags.
func FromFlags(fs *flag.FlagSet, args []string) (*Config, error) {
	c := &Config{}
	var providers string
	fs.StringVar(&c.NodeName, "node-name", os.Getenv("NODE_NAME"),
		"Kubernetes node name (defaults to $NODE_NAME).")
	fs.StringVar(&providers, "providers", "tcp",
		"Comma-separated libmxl-fabrics providers to advertise (auto,tcp,verbs,efa,shm).")
	fs.StringVar(&c.Kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"),
		"Path to a kubeconfig file. Empty uses the in-cluster config.")
	fs.StringVar(&c.ProbeAddr, "health-probe-bind-address", ":8081",
		"Address the health probe endpoint binds to.")
	fs.StringVar(&c.MetricsAddr, "metrics-bind-address", ":8080",
		"Address the metrics endpoint binds to.")
	fs.DurationVar(&c.ResyncPeriod, "resync-period", 30*time.Second,
		"How often to refresh MxlNodeCapabilities status.")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	for _, name := range strings.Split(providers, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
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
	if len(c.Providers) == 0 {
		return fmt.Errorf("--providers must list at least one provider")
	}
	return nil
}
