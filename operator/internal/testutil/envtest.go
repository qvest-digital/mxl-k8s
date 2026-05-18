// Package testutil hosts shared test helpers for the operator's
// reconcilers: an envtest harness that boots a real kube-apiserver +
// etcd with the project's CRDs applied, plus a small set of builder
// helpers for the CRD types so individual tests stay readable.
//
// Tests that need a faithful API server (admission, defaulting, status
// subresource separation, structured-merge-diff) take this harness.
// Tests of pure helpers that only need a typed client should reach for
// fake.NewClientBuilder instead.
package testutil

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Env wraps an envtest.Environment together with a direct API-server
// client (no informer cache). Tests should always read state through
// Client so assertions see what kube-apiserver actually persisted,
// rather than what a manager-side cache decided to reflect.
type Env struct {
	TestEnv *envtest.Environment
	Cfg     *rest.Config
	Scheme  *apiruntime.Scheme
	Client  client.Client

	stop func()
}

// Start boots envtest from TestMain without depending on a real
// *testing.T. Returns the Env plus a Stop function the caller must
// invoke before exiting. KUBEBUILDER_ASSETS must be set (the Makefile
// provisions it via setup-envtest); the caller can detect the missing
// asset case and skip envtest tests rather than fail noisily.
func Start() (*Env, error) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		return nil, fmt.Errorf("KUBEBUILDER_ASSETS is unset; run via " +
			"`make test` so setup-envtest provisions kube-apiserver " +
			"+ etcd into bin/k8s")
	}

	scheme := apiruntime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(mxlv1alpha1.AddToScheme(scheme))

	te := &envtest.Environment{
		CRDDirectoryPaths:     []string{crdDirFromSource()},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := te.Start()
	if err != nil {
		return nil, fmt.Errorf("envtest start: %w", err)
	}

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		_ = te.Stop()
		return nil, fmt.Errorf("envtest client: %w", err)
	}

	e := &Env{
		TestEnv: te,
		Cfg:     cfg,
		Scheme:  scheme,
		Client:  c,
	}
	e.stop = func() { _ = te.Stop() }
	return e, nil
}

// Stop tears down envtest. Idempotent.
func (e *Env) Stop() {
	if e == nil || e.stop == nil {
		return
	}
	e.stop()
	e.stop = nil
}

// crdDirFromSource returns the absolute path to the repo's config/crd
// directory, derived from the source location of this file. Tests can
// run from any working directory the test runner picks.
func crdDirFromSource() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "config", "crd")
}

// NewNamespace creates a namespace named after the current test and
// deletes it on cleanup. Returning the name lets callers feed it into
// CR builders so isolation is automatic.
func (e *Env) NewNamespace(t testing.TB) string {
	t.Helper()
	name := dnsNamespace(t.Name())
	ctx := context.Background()
	ns := &corev1.Namespace{}
	ns.SetName(name)
	require.NoError(t, e.Client.Create(ctx, ns))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = e.Client.Delete(ctx, ns)
	})
	return name
}

// dnsNamespace coerces an arbitrary test name into a DNS-1123-safe
// namespace label. Test names contain slashes (subtests) and
// underscores that the API server rejects on namespace creation; the
// agreed transformation lower-cases everything and replaces every
// other rune with '-'.
func dnsNamespace(in string) string {
	out := make([]byte, 0, len(in))
	for i := 0; i < len(in); i++ {
		c := in[i]
		switch {
		case c >= 'A' && c <= 'Z':
			out = append(out, c+('a'-'A'))
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
			out = append(out, c)
		case c == '-':
			out = append(out, '-')
		default:
			out = append(out, '-')
		}
	}
	if len(out) > 63 {
		out = out[:63]
	}
	for len(out) > 0 && out[0] == '-' {
		out = out[1:]
	}
	for len(out) > 0 && out[len(out)-1] == '-' {
		out = out[:len(out)-1]
	}
	if len(out) == 0 {
		return "test"
	}
	return string(out)
}
