package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/operator/internal/domain"
	"github.com/qvest-digital/mxl-k8s/operator/internal/flow"
	"github.com/qvest-digital/mxl-k8s/operator/internal/leasecheck"
	"github.com/qvest-digital/mxl-k8s/operator/internal/leasegc"
	"github.com/qvest-digital/mxl-k8s/operator/internal/mirror"
	"github.com/qvest-digital/mxl-k8s/operator/internal/nodecaps"
	"github.com/qvest-digital/mxl-k8s/operator/internal/receiver"
	"github.com/qvest-digital/mxl-k8s/operator/internal/redundancyset"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(mxlv1alpha1.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr string
		probeAddr   string
		leaderElect bool
		gcGrace     time.Duration
		setMirrorNS string
		mxlfs       bool
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080",
		"The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081",
		"The address the probe endpoint binds to.")
	flag.BoolVar(&leaderElect, "leader-elect", false,
		"Enable leader election for the controller manager.")
	flag.DurationVar(&gcGrace, "gc-grace-period", flow.DefaultGracePeriod,
		"How long an MxlFlow must have no live copy, and an MxlFlowMirror "+
			"no claim or no source, before it is deleted. Has to outlast a "+
			"producer or consumer pod rolling over.")

	flag.BoolVar(&mxlfs, "mxlfs", false,
		"Reconcile MxlFlowRedundancySets, which need mxlfs domains on the nodes "+
			"that read a set's output.")
	flag.StringVar(&setMirrorNS, "redundancy-mirror-namespace", "",
		"Namespace for the member mirrors of MxlFlowRedundancySets. Empty is the "+
			"operator's own namespace.")

	zapOpts := zap.Options{Development: false}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "mxl-operator.mxl.qvest-digital.com",
	})
	if err != nil {
		setupLog.Error(err, "failed to construct manager")
		os.Exit(1)
	}

	if mxlfs {
		if err := requireKind(mgr.GetRESTMapper(), "MxlFlowRedundancySet"); err != nil {
			setupLog.Error(err, "--mxlfs needs the redundancy set CRD")
			os.Exit(1)
		}
	}
	for _, rc := range reconcilers(mgr, gcGrace, mxlfs, setMirrorNS) {
		if err := rc.setup(mgr); err != nil {
			setupLog.Error(err, "failed to set up reconciler", "kind", rc.name)
			os.Exit(1)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "failed to register healthz check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "failed to register readyz check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager exited with error")
		os.Exit(1)
	}
}

type reconciler struct {
	name  string
	setup func(ctrl.Manager) error
}

// reconcilers are the controllers the operator runs. MxlFlowRedundancySet
// is among them only with mxlfs.
func reconcilers(mgr ctrl.Manager, gcGrace time.Duration, mxlfs bool, setMirrorNS string) []reconciler {
	rs := []reconciler{
		{"MxlDomain", (&domain.Reconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager},
		{"MxlFlow", (&flow.Reconciler{
			Client:      mgr.GetClient(),
			Scheme:      mgr.GetScheme(),
			Recorder:    mgr.GetEventRecorderFor("mxlflow-controller"),
			Lease:       &leasecheck.Checker{Client: mgr.GetClient()},
			GracePeriod: gcGrace,
		}).SetupWithManager},
		{"MxlFlowMirror", (&mirror.Reconciler{
			Client:         mgr.GetClient(),
			APIReader:      mgr.GetAPIReader(),
			Scheme:         mgr.GetScheme(),
			Recorder:       mgr.GetEventRecorderFor("mxlflowmirror-controller"),
			Lease:          &leasecheck.Checker{Client: mgr.GetClient()},
			GracePeriod:    gcGrace,
			RedundancySets: mxlfs,
		}).SetupWithManager},
		{"OriginLease", (&leasegc.Reconciler{
			Client: mgr.GetClient(),
			Scheme: mgr.GetScheme(),
		}).SetupWithManager},
		{"MxlReceiver", (&receiver.Reconciler{
			Client:    mgr.GetClient(),
			APIReader: mgr.GetAPIReader(),
			Scheme:    mgr.GetScheme(),
			Recorder:  mgr.GetEventRecorderFor("mxlreceiver-controller"),
			Lease:     &leasecheck.Checker{Client: mgr.GetClient()},
		}).SetupWithManager},
		{"MxlNodeCapabilities", (&nodecaps.Reconciler{
			Client:   mgr.GetClient(),
			Scheme:   mgr.GetScheme(),
			Recorder: mgr.GetEventRecorderFor("mxlnodecapabilities-controller"),
		}).SetupWithManager},
	}
	if mxlfs {
		rs = append(rs, reconciler{"MxlFlowRedundancySet", (&redundancyset.Reconciler{
			Client:           mgr.GetClient(),
			Namespace:        ownNamespace(setMirrorNS),
			GatewayNamespace: ownNamespace(""),
			Lease:            &leasecheck.Checker{Client: mgr.GetClient()},
		}).SetupWithManager})
	}
	return rs
}

// requireKind fails when the API server does not serve kind: a
// controller for it would otherwise wait for its cache without end.
func requireKind(mapper meta.RESTMapper, kind string) error {
	gk := schema.GroupKind{Group: mxlv1alpha1.GroupVersion.Group, Kind: kind}
	if _, err := mapper.RESTMapping(gk, mxlv1alpha1.GroupVersion.Version); err != nil {
		return fmt.Errorf("%s is not served (is its CRD installed?): %w", gk, err)
	}
	return nil
}

// ownNamespace is ns, or the namespace of the operator's service account.
func ownNamespace(ns string) string {
	if ns != "" {
		return ns
	}
	b, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return "default"
	}
	return strings.TrimSpace(string(b))
}
