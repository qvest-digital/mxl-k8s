package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/operator/internal/domain"
	"github.com/qvest-digital/mxl-k8s/operator/internal/flow"
	"github.com/qvest-digital/mxl-k8s/operator/internal/mirror"
	"github.com/qvest-digital/mxl-k8s/operator/internal/nodecaps"
	"github.com/qvest-digital/mxl-k8s/operator/internal/receiver"
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
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080",
		"The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081",
		"The address the probe endpoint binds to.")
	flag.BoolVar(&leaderElect, "leader-elect", false,
		"Enable leader election for the controller manager.")

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

	reconcilers := []struct {
		name  string
		setup func(ctrl.Manager) error
	}{
		{"MxlDomain", (&domain.Reconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager},
		{"MxlFlow", (&flow.Reconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager},
		{"MxlFlowMirror", (&mirror.Reconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager},
		{"MxlReceiver", (&receiver.Reconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager},
		{"MxlNodeCapabilities", (&nodecaps.Reconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager},
	}
	for _, rc := range reconcilers {
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
