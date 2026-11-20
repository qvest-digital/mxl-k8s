package main

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/qvest-digital/mxl-k8s/agent/internal/config"
	"github.com/qvest-digital/mxl-k8s/agent/internal/intent"
	"github.com/qvest-digital/mxl-k8s/agent/internal/podlookup"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// apiClients are the agent's two Kubernetes API clients.
//
// A client limits its rate per resource it requests, and two paths
// request the same resources: what readers ask of the API, where a wait
// on a missing flow Gets the MxlFlow whose status flow publishing writes
// and lists the mirrors the publisher lists, and flow publishing and
// Lease renewal. With one client, readers asking for many flows spend
// what publishing and renewal need. intent serves pod lookups and
// on-demand mirrors with limiters of its own; main serves the rest. Both
// share one REST mapping and, through client-go's transport cache, one
// transport: only their limits are apart.
type apiClients struct {
	main, intent client.Client
}

// newClients returns the agent's API clients for cfg, main at qps and
// burst, intent at intentQPS and intentBurst.
func newClients(cfg *rest.Config, scheme *runtime.Scheme, qps float32, burst int,
	intentQPS float32, intentBurst int) (apiClients, error) {
	// client-go defaults to 5 QPS / 10 burst when the limits are left
	// zero; flow appear/vanish bursts on a busy domain exceed that.
	mainCfg := rest.CopyConfig(cfg)
	mainCfg.QPS, mainCfg.Burst = qps, burst
	mainClient, err := client.New(mainCfg, client.Options{Scheme: scheme})
	if err != nil {
		return apiClients{}, fmt.Errorf("build client: %w", err)
	}
	intentCfg := rest.CopyConfig(cfg)
	intentCfg.QPS, intentCfg.Burst = intentQPS, intentBurst
	intentClient, err := client.New(intentCfg, client.Options{Scheme: scheme, Mapper: mainClient.RESTMapper()})
	if err != nil {
		return apiClients{}, fmt.Errorf("build intent client: %w", err)
	}
	return apiClients{main: mainClient, intent: intentClient}, nil
}

// pods returns the resolver of pids to this node's pods, on the intent
// client.
func (c apiClients) pods(node string) *podlookup.Resolver {
	return &podlookup.Resolver{Client: c.intent, NodeName: node}
}

// dispatcher returns the dispatcher of on-demand mirrors for cfg, on the
// intent client and resolving pids through pods. The caller sets what it
// takes from the rest of the agent: Lease, Origin, Own and Domains.
func (c apiClients) dispatcher(cfg *config.Config, pods *podlookup.Resolver) *intent.Dispatcher {
	return &intent.Dispatcher{
		Client:             c.intent,
		Resolver:           pods,
		DomainPath:         cfg.DomainPath,
		NodeName:           cfg.NodeName,
		Provider:           mxlv1alpha1.MxlFabricsProvider(cfg.Provider),
		MaterializeTimeout: cfg.MaterializeTimeout,
		Limits: intent.Limits{
			PerPod:      cfg.IntentMirrorsPerPod,
			PerNode:     cfg.IntentMirrorsPerNode,
			Burst:       cfg.IntentMirrorBurst,
			BurstWindow: cfg.IntentMirrorBurstWindow,
		},
	}
}
