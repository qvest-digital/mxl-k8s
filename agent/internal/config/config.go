package config

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

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

	// ShimPath is where the agent writes the copy of libmxl-intent.so
	// it carries, for consumer pods that preload the shim off the
	// node instead of copying it out of the carrier image. Empty
	// disables the drop.
	ShimPath string

	// CSIEndpoint is the unix socket the agent serves the CSI node
	// plugin on, which delivers the domain to pods as an inline
	// ephemeral volume. Empty disables it.
	CSIEndpoint string

	// ClaimStatusLabel is the pod label that names a pod's
	// MediaFunctionClaim, whose status.io the agent then writes from the
	// flows the pod is observed to read and write. Empty disables it.
	ClaimStatusLabel string

	// MaterializeTimeout caps the per-request wait the intent
	// dispatcher allows before giving up on a mirror reaching
	// Ready.
	MaterializeTimeout time.Duration

	// IntentMirrorsPerPod, IntentMirrorsPerNode and IntentMirrorBurst
	// within IntentMirrorBurstWindow bound the mirrors the intent
	// dispatcher creates on demand; see intent.Limits. Zero leaves a
	// limit off.
	IntentMirrorsPerPod     int
	IntentMirrorsPerNode    int
	IntentMirrorBurst       int
	IntentMirrorBurstWindow time.Duration

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

	// Mxlfs runs what a domain on mxlfs needs: placeholder and wanted
	// flow watchers, redundancy set demand. Off, every domain is a plain
	// directory.
	Mxlfs bool

	// MxlfsHuge is the huge= option every mxlfs domain is mounted with:
	// within_size or always. Empty leaves it to the module, which then
	// needs no such option.
	MxlfsHuge string

	// MxlfsNumaMode is how the rings of mxlfs domains are placed on NUMA
	// nodes: off, node or flow.
	MxlfsNumaMode string

	// MxlfsNumaNodeDefault is the numa_node= option node mode mounts
	// every domain with: a NUMA node, or auto for the node every RDMA
	// device of the node is on.
	MxlfsNumaNodeDefault string

	// MxlfsNumaFlowDefault is where flow mode allocates a page nothing
	// placed: nic mounts every domain with the NICs' node as numa_node=,
	// local leaves it to the allocating process.
	MxlfsNumaFlowDefault string

	// MxlfsNumaRelease* pace flow mode's release loop: how often it
	// looks, how long a ring stays unheld before it moves, and how many
	// bytes a second it moves.
	MxlfsNumaReleasePeriod time.Duration
	MxlfsNumaReleaseDelay  time.Duration
	MxlfsNumaReleaseRate   string
}

// Placement modes.
const (
	NumaOff  = "off"
	NumaNode = "node"
	NumaFlow = "flow"
)

// ReleaseRate is --mxlfs-numa-placement-flow-release-rate in bytes a
// second.
func (c *Config) ReleaseRate() uint64 {
	q, err := resource.ParseQuantity(c.MxlfsNumaReleaseRate)
	if err != nil {
		return 0
	}
	v, _ := q.AsInt64()
	return uint64(max(v, 0))
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
	fs.StringVar(&c.ShimPath, "shim-path", "/run/mxl/libmxl-intent.so",
		"Path the agent writes its copy of libmxl-intent.so to on every "+
			"start, for consumers that preload it off the node. Empty disables.")
	fs.StringVar(&c.CSIEndpoint, "csi-endpoint", "",
		"Unix socket for the CSI node plugin that mounts the domain into pods. Empty disables.")
	fs.StringVar(&c.ClaimStatusLabel, "claim-status-label", "",
		"Pod label naming a pod's MediaFunctionClaim, whose status.io the agent "+
			"writes from observed flow use. Empty disables.")
	fs.BoolVar(&c.Mxlfs, "mxlfs", false,
		"Run what a domain on mxlfs needs: mirrors for opened placeholders and "+
			"waited-for flows, and MxlFlowRedundancySet demand. Off, every domain is "+
			"treated as a plain directory.")
	fs.StringVar(&c.MxlfsHuge, "mxlfs-huge", "",
		"huge= mount option of every mxlfs domain (within_size, always). Empty "+
			"leaves it out, for an mxlfs module without the option.")
	fs.StringVar(&c.MxlfsNumaMode, "mxlfs-numa-placement-mode", NumaOff,
		"NUMA placement of the rings of mxlfs domains: off; node mounts every domain with "+
			"--mxlfs-numa-placement-node-default as numa_node=; flow places rings per flow: the "+
			"gateway holds and moves the rings it registers to the NICs' node, and the agent "+
			"moves an unheld ring to the one node its local writers and readers are confined to.")
	fs.StringVar(&c.MxlfsNumaNodeDefault, "mxlfs-numa-placement-node-default", "auto",
		"Node mode's numa_node= mount option: a NUMA node, or auto for the node every RDMA "+
			"device of the node is on, left out when they report none or several.")
	fs.StringVar(&c.MxlfsNumaFlowDefault, "mxlfs-numa-placement-flow-default", "local",
		"Flow mode's numa_node= for pages nothing placed: nic for the NICs' node, local for none.")
	fs.DurationVar(&c.MxlfsNumaReleasePeriod, "mxlfs-numa-placement-flow-release-period", 30*time.Second,
		"How often flow mode's release loop looks at the node's flows.")
	fs.DurationVar(&c.MxlfsNumaReleaseDelay, "mxlfs-numa-placement-flow-release-delay", time.Minute,
		"How long a ring stays unheld, counted from when the agent first saw it so, before it moves.")
	fs.StringVar(&c.MxlfsNumaReleaseRate, "mxlfs-numa-placement-flow-release-rate", "512Mi",
		"Bytes a second the release loop moves, a quantity.")
	fs.DurationVar(&c.MaterializeTimeout, "materialize-timeout", 5*time.Second,
		"Per-request budget for the intent dispatcher waiting for a mirror Ready.")
	fs.IntVar(&c.IntentMirrorsPerPod, "intent-mirrors-per-pod", 64,
		"Most live mirrors created on demand for one pod over its lifetime. 0 is unlimited.")
	fs.IntVar(&c.IntentMirrorsPerNode, "intent-mirrors-per-node", 128,
		"Most live mirrors created on demand onto this node. 0 is unlimited.")
	fs.IntVar(&c.IntentMirrorBurst, "intent-mirror-burst", 8,
		"Most mirrors created on demand for one pod within --intent-mirror-burst-window. 0 is unlimited.")
	fs.DurationVar(&c.IntentMirrorBurstWindow, "intent-mirror-burst-window", 10*time.Second,
		"Window --intent-mirror-burst counts in.")
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
	// Every other domain lives in domains/ beside the primary one; a
	// primary inside it would be tracked twice, once under each name.
	if filepath.Base(filepath.Dir(filepath.Clean(c.DomainPath))) == mxlv1alpha1.DomainsDir {
		return fmt.Errorf("--domain-path must not be inside %s/, got %q", mxlv1alpha1.DomainsDir, c.DomainPath)
	}
	if c.NodeName == "" {
		return fmt.Errorf("--node-name (or $NODE_NAME) is required")
	}
	if c.ShimPath != "" && c.ShimPath[0] != '/' {
		return fmt.Errorf("--shim-path must be absolute, got %q", c.ShimPath)
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
	if c.IntentMirrorsPerPod < 0 || c.IntentMirrorsPerNode < 0 || c.IntentMirrorBurst < 0 {
		return fmt.Errorf("--intent-mirrors-per-pod, --intent-mirrors-per-node and --intent-mirror-burst must not be negative")
	}
	if c.IntentMirrorBurst > 0 && c.IntentMirrorBurstWindow <= 0 {
		return fmt.Errorf("--intent-mirror-burst-window must be positive while --intent-mirror-burst is set, got %v",
			c.IntentMirrorBurstWindow)
	}
	switch c.MxlfsHuge {
	case "", "within_size", "always":
	default:
		return fmt.Errorf("--mxlfs-huge %q is not one of within_size, always", c.MxlfsHuge)
	}
	if err := c.validateNuma(); err != nil {
		return err
	}
	if c.KubeAPIQPS <= 0 {
		return fmt.Errorf("--kube-api-qps must be positive, got %v", c.KubeAPIQPS)
	}
	if c.KubeAPIBurst <= 0 {
		return fmt.Errorf("--kube-api-burst must be positive, got %d", c.KubeAPIBurst)
	}
	return nil
}

func (c *Config) validateNuma() error {
	switch c.MxlfsNumaNodeDefault {
	case "", "auto":
	default:
		if _, err := strconv.ParseUint(c.MxlfsNumaNodeDefault, 10, 31); err != nil {
			return fmt.Errorf("--mxlfs-numa-placement-node-default %q is not a NUMA node or auto", c.MxlfsNumaNodeDefault)
		}
	}
	switch c.MxlfsNumaMode {
	case "", NumaOff, NumaNode:
		return nil
	case NumaFlow:
		if !c.Mxlfs {
			return fmt.Errorf("--mxlfs-numa-placement-mode=flow needs --mxlfs")
		}
	default:
		return fmt.Errorf("--mxlfs-numa-placement-mode %q is not one of off, node, flow", c.MxlfsNumaMode)
	}
	switch c.MxlfsNumaFlowDefault {
	case "", "nic", "local":
	default:
		return fmt.Errorf("--mxlfs-numa-placement-flow-default %q is not one of nic, local", c.MxlfsNumaFlowDefault)
	}
	if c.MxlfsNumaReleasePeriod <= 0 {
		return fmt.Errorf("--mxlfs-numa-placement-flow-release-period must be positive, got %v", c.MxlfsNumaReleasePeriod)
	}
	if c.MxlfsNumaReleaseDelay < 0 {
		return fmt.Errorf("--mxlfs-numa-placement-flow-release-delay must not be negative, got %v", c.MxlfsNumaReleaseDelay)
	}
	if c.ReleaseRate() == 0 {
		return fmt.Errorf("--mxlfs-numa-placement-flow-release-rate %q is not a positive quantity", c.MxlfsNumaReleaseRate)
	}
	return nil
}
