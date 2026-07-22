package config

import (
	"flag"
	"fmt"
	"os"
	"time"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
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

	// IntentSocketPath is the UDS the agent serves for libmxl-
	// intent.so to request on-demand mirror materialization. Empty
	// disables the intent endpoint.
	IntentSocketPath string

	// MaterializeTimeout caps the per-request wait the intent
	// dispatcher allows before giving up on a mirror reaching
	// Ready.
	MaterializeTimeout time.Duration

	// Provider is an explicit per-cluster libmxl-fabrics provider
	// override for on-demand mirrors. Empty (or "auto") lets the
	// dispatcher resolve a concrete provider from the source and
	// target nodes' MxlNodeCapabilities; any other value is stamped
	// onto every intent mirror verbatim and bypasses resolution.
	Provider string

	// KubeAPIQPS is the sustained request rate the Kubernetes API
	// client allows before throttling. client-go falls back to
	// 5 QPS when the limit is unset, which queues MxlFlow status
	// publishes and intent mirror writes behind second-long delays
	// during flow appear/vanish bursts.
	KubeAPIQPS float64

	// KubeAPIBurst is the burst ceiling of the Kubernetes API client
	// rate limiter.
	KubeAPIBurst int
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
	fs.StringVar(&c.IntentSocketPath, "intent-socket", "/run/mxl/agent.sock",
		"UDS path for the on-demand intent endpoint. Empty disables.")
	fs.DurationVar(&c.MaterializeTimeout, "materialize-timeout", 5*time.Second,
		"Per-request budget for the intent dispatcher waiting for a mirror Ready.")
	fs.StringVar(&c.Provider, "provider", "",
		"Explicit libmxl-fabrics provider (tcp, verbs, efa, shm) stamped "+
			"onto on-demand mirrors. Empty or 'auto' resolves per node from "+
			"MxlNodeCapabilities.")
	fs.Float64Var(&c.KubeAPIQPS, "kube-api-qps", 50,
		"Sustained Kubernetes API request rate allowed before client-side throttling.")
	fs.IntVar(&c.KubeAPIBurst, "kube-api-burst", 100,
		"Burst ceiling of the Kubernetes API client rate limiter.")
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
	switch mxlv1alpha1.MxlFabricsProvider(c.Provider) {
	case "",
		mxlv1alpha1.ProviderAuto,
		mxlv1alpha1.ProviderTCP,
		mxlv1alpha1.ProviderVerbs,
		mxlv1alpha1.ProviderEFA,
		mxlv1alpha1.ProviderSHM:
	default:
		return fmt.Errorf("--provider %q is not one of tcp, verbs, efa, shm, auto", c.Provider)
	}
	if c.KubeAPIQPS <= 0 {
		return fmt.Errorf("--kube-api-qps must be positive, got %v", c.KubeAPIQPS)
	}
	if c.KubeAPIBurst <= 0 {
		return fmt.Errorf("--kube-api-burst must be positive, got %d", c.KubeAPIBurst)
	}
	return nil
}
