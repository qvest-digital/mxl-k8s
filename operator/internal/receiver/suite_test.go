package receiver_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/qvest-digital/mxl-k8s/operator/internal/testutil"
)

// env is the shared envtest harness for every Test* in this package.
// Booting kube-apiserver + etcd costs about two seconds per process;
// reusing one instance across the package's tests keeps the suite
// fast while per-test namespaces preserve isolation.
var env *testutil.Env

func TestMain(m *testing.M) {
	e, err := testutil.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "envtest unavailable, skipping suite: %v\n", err)
		// Exit 0 so a developer running `go test ./...` without
		// provisioned assets sees a clean PASS-but-skipped rather
		// than a CI-killing failure. The CI `test` job sets
		// KUBEBUILDER_ASSETS so this path never triggers there.
		os.Exit(0)
	}
	env = e

	code := m.Run()
	env.Stop()
	os.Exit(code)
}
