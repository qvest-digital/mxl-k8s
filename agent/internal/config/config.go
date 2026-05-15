package config

import (
	"flag"
	"fmt"
	"os"
	"time"
)

// Config holds the agent's runtime configuration.
type Config struct {
	// DomainPath is the absolute path of the MXL domain directory the
	// agent watches.
	DomainPath string

	// NodeName is the Kubernetes node hosting this agent. Sourced
	// from --node-name or the NODE_NAME env var (downward API).
	NodeName string

	// Kubeconfig is an optional kubeconfig path. Empty falls back to
	// the in-cluster config.
	Kubeconfig string

	// ProbeAddr is the address for the http liveness/readiness probes.
	ProbeAddr string

	// MetricsAddr is the address for the prometheus metrics server.
	MetricsAddr string

	// ResyncPeriod is how often the agent refreshes MxlDomain status.
	ResyncPeriod time.Duration
}

// FromFlags populates a Config from command-line flags.
func FromFlags(fs *flag.FlagSet, args []string) (*Config, error) {
	c := &Config{}
	fs.StringVar(&c.DomainPath, "domain-path", "",
		"Absolute path to the MXL domain directory to watch.")
	fs.StringVar(&c.NodeName, "node-name", os.Getenv("NODE_NAME"),
		"Kubernetes node name (defaults to $NODE_NAME).")
	fs.StringVar(&c.Kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"),
		"Path to a kubeconfig file. Empty uses the in-cluster config.")
	fs.StringVar(&c.ProbeAddr, "health-probe-bind-address", ":8081",
		"Address the health probe endpoint binds to.")
	fs.StringVar(&c.MetricsAddr, "metrics-bind-address", ":8080",
		"Address the metrics endpoint binds to.")
	fs.DurationVar(&c.ResyncPeriod, "resync-period", 30*time.Second,
		"How often to refresh MxlDomain status.")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// Validate checks that required fields are set.
func (c *Config) Validate() error {
	if c.DomainPath == "" {
		return fmt.Errorf("--domain-path is required")
	}
	if c.DomainPath[0] != '/' {
		return fmt.Errorf("--domain-path must be absolute, got %q", c.DomainPath)
	}
	if c.NodeName == "" {
		return fmt.Errorf("--node-name (or $NODE_NAME) is required")
	}
	return nil
}
