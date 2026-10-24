package observed

import (
	"context"
	"encoding/json"
	"reflect"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClaimGVK is the dmf-k8s MediaFunctionClaim, whose status.io holds what
// the function is observed to read and write.
var ClaimGVK = schema.GroupVersionKind{Group: "dmf.qvest-digital.com", Version: "v1alpha1", Kind: "MediaFunctionClaim"}

// ClaimWriter writes the flows each claim's pods on this node use into
// the claim's status.io. The claim of a pod is named by a pod label in
// the pod's namespace: dmf-k8s names a function's HelmRelease, and so the
// Helm release whose pods carry app.kubernetes.io/instance, after the
// claim. Only a claim whose pods run on this node is written here, so a
// claim with pods on several nodes is out of scope.
type ClaimWriter struct {
	Client   client.Client
	NodeName string
	Label    string
	Scanner  *Scanner
	Log      logr.Logger

	scan func() (map[string][]Use, error) // a seam for tests; nil scans
}

// binding is one status.io entry. Port is left out: what a function
// opens says nothing about which of its ports the flow is for.
type binding struct {
	Direction string `json:"direction"`
	Type      string `json:"type"`
	ID        string `json:"id"`
	Domain    string `json:"domain,omitempty"`
}

// Sync scans once and writes every claim whose observed io changed.
func (w *ClaimWriter) Sync(ctx context.Context) error {
	scan := w.scan
	if scan == nil {
		scan = w.Scanner.Scan
	}
	uses, err := scan()
	if err != nil {
		return err
	}
	var pods corev1.PodList
	if err := w.Client.List(ctx, &pods, client.MatchingFields{"spec.nodeName": w.NodeName}); err != nil {
		return err
	}
	type claimKey struct{ ns, name string }
	io := map[claimKey][]binding{}
	for _, pod := range pods.Items {
		name := pod.Labels[w.Label]
		if name == "" || pod.Status.Phase != corev1.PodRunning {
			continue
		}
		k := claimKey{pod.Namespace, name}
		if _, ok := io[k]; !ok {
			io[k] = []binding{}
		}
		for _, u := range uses[string(pod.UID)] {
			b := binding{Direction: string(u.Direction), Type: "MxlFlow", ID: u.FlowID, Domain: u.Domain}
			if !contains(io[k], b) {
				io[k] = append(io[k], b)
			}
		}
	}
	for k, want := range io {
		if err := w.write(ctx, k.ns, k.name, want); err != nil {
			w.Log.Error(err, "write status.io", "claim", k.ns+"/"+k.name)
		}
	}
	return nil
}

func contains(bs []binding, b binding) bool {
	for _, x := range bs {
		if x == b {
			return true
		}
	}
	return false
}

func (w *ClaimWriter) write(ctx context.Context, ns, name string, want []binding) error {
	claim := &unstructured.Unstructured{}
	claim.SetGroupVersionKind(ClaimGVK)
	if err := w.Client.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, claim); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // a release that is not a claim's
		}
		return err
	}
	cur, _, _ := unstructured.NestedSlice(claim.Object, "status", "io")
	var have []binding
	if raw, err := json.Marshal(cur); err == nil {
		_ = json.Unmarshal(raw, &have)
	}
	if have == nil {
		have = []binding{}
	}
	if sameSet(have, want) {
		return nil
	}
	patch, err := json.Marshal(map[string]any{"status": map[string]any{"io": want}})
	if err != nil {
		return err
	}
	return w.Client.Status().Patch(ctx, claim, client.RawPatch(types.MergePatchType, patch))
}

func sameSet(a, b []binding) bool {
	if len(a) != len(b) {
		return false
	}
	ma := map[binding]bool{}
	for _, x := range a {
		ma[x] = true
	}
	mb := map[binding]bool{}
	for _, x := range b {
		mb[x] = true
	}
	return reflect.DeepEqual(ma, mb)
}

// Run syncs every period until ctx ends.
func (w *ClaimWriter) Run(ctx context.Context, period time.Duration) {
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		if err := w.Sync(ctx); err != nil {
			w.Log.Error(err, "observe flow use")
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
