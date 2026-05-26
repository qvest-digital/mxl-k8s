package receiver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	apiv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/operator/internal/testutil"
)

// recordingReconciler wraps a real Reconciler and counts every
// (namespace, name) the manager dispatches to Reconcile. The Lease
// watch envtest below uses it to verify that the manager's watch
// wiring -- not just the mapper in isolation -- enqueues the
// receiver in response to Lease events. The direct-Reconcile tests
// in controller_envtest_test.go cannot prove that path.
type recordingReconciler struct {
	inner *Reconciler

	mu    sync.Mutex
	calls map[client.ObjectKey]int
}

func newRecordingReconciler(inner *Reconciler) *recordingReconciler {
	return &recordingReconciler{
		inner: inner,
		calls: map[client.ObjectKey]int{},
	}
}

func (c *recordingReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	c.mu.Lock()
	c.calls[req.NamespacedName]++
	c.mu.Unlock()
	return c.inner.Reconcile(ctx, req)
}

func (c *recordingReconciler) countFor(key client.ObjectKey) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[key]
}

func (c *recordingReconciler) waitForAtLeast(key client.ObjectKey, minCount int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := c.countFor(key); got >= minCount {
			return got
		}
		time.Sleep(25 * time.Millisecond)
	}
	return c.countFor(key)
}

// envOnce holds the package-internal envtest fixture shared by every
// test in this file. Booting kube-apiserver + etcd costs ~2s; reuse
// keeps the suite fast while per-test namespaces preserve isolation.
var (
	envOnce sync.Once
	envInst *testutil.Env
	envErr  error
)

func sharedEnv(t *testing.T) *testutil.Env {
	t.Helper()
	envOnce.Do(func() {
		envInst, envErr = testutil.Start()
	})
	if envErr != nil {
		t.Skipf("envtest unavailable, skipping: %v", envErr)
	}
	return envInst
}

func ensureLeaseNamespace(t *testing.T, c client.Client) {
	t.Helper()
	ns := &corev1.Namespace{}
	ns.Name = apiv1alpha1.LeaseNamespace
	err := c.Create(context.Background(), ns)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create lease namespace: %v", err)
	}
}

func startReceiverManager(t *testing.T, env *testutil.Env, rec *recordingReconciler) context.CancelFunc {
	t.Helper()

	skip := true
	mgr, err := ctrl.NewManager(env.Cfg, manager.Options{
		Scheme:                 env.Scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		LeaderElection:         false,
		// Tests in this file spin up multiple managers against a
		// single envtest. controller-runtime rejects a duplicate
		// controller name across managers in the same process
		// without this opt-out.
		Controller: config.Controller{SkipNameValidation: &skip},
	})
	require.NoError(t, err)

	rec.inner.Client = mgr.GetClient()
	rec.inner.Scheme = mgr.GetScheme()
	require.NoError(t, rec.inner.setupWithManagerAgainst(mgr, rec))

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = mgr.Start(ctx)
	}()
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		cancel()
		t.Fatal("manager cache failed to sync within startup window")
	}
	return cancel
}

func freshNamespace(t *testing.T, env *testutil.Env, name string) string {
	t.Helper()
	ns := &corev1.Namespace{}
	ns.Name = name
	require.NoError(t, env.Client.Create(context.Background(), ns))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = env.Client.Delete(ctx, ns)
	})
	return name
}

// TestReceiver_LeaseWatchTriggersReconcile verifies the controller
// manager wakes the receiver on Lease Create and Update (renew). The
// Lease is the authoritative liveness signal for an Origin location;
// without this watch the receiver only reconverged on the next Pod
// or MxlFlow event, so demote and promote on Lease expiry lagged
// arbitrarily.
func TestReceiver_LeaseWatchTriggersReconcile(t *testing.T) {
	env := sharedEnv(t)
	ctx := context.Background()
	ns := freshNamespace(t, env, "lease-watch-trigger")
	ensureLeaseNamespace(t, env.Client)

	const flowID = "11111111-2222-3333-4444-555555555555"

	require.NoError(t, env.Client.Create(ctx,
		testutil.NewReceiver(ns, "r", testutil.WithReceiverFlowID(flowID))))

	rec := newRecordingReconciler(&Reconciler{})
	stop := startReceiverManager(t, env, rec)
	defer stop()

	key := client.ObjectKey{Namespace: ns, Name: "r"}

	// The For() watch fires on initial cache sync; baseline so
	// subsequent assertions observe only Lease-driven dispatches.
	baseline := rec.waitForAtLeast(key, 1, 5*time.Second)
	require.GreaterOrEqual(t, baseline, 1,
		"initial cache sync must dispatch at least one reconcile for the receiver")

	leaseName := apiv1alpha1.LeaseName(flowID, "nodea")
	now := metav1.MicroTime{Time: time.Now()}
	duration := int32(30)
	t.Cleanup(func() {
		_ = env.Client.Delete(context.Background(), &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: apiv1alpha1.LeaseNamespace,
				Name:      leaseName,
			},
		})
	})
	// Let the reconcile feedback loop quiesce so subsequent assertions
	// observe only Lease-driven dispatches rather than mid-flight
	// status-update echoes.
	time.Sleep(750 * time.Millisecond)
	baseline = rec.countFor(key)

	require.NoError(t, env.Client.Create(ctx, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: apiv1alpha1.LeaseNamespace,
			Name:      leaseName,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptr("agent-node-a"),
			LeaseDurationSeconds: &duration,
			RenewTime:            &now,
		},
	}))

	got := rec.waitForAtLeast(key, baseline+1, 10*time.Second)
	assert.GreaterOrEqual(t, got, baseline+1,
		"Lease Create in mxl-system must enqueue the matching receiver; "+
			"the Lease watch is the canonical wake for Origin liveness. "+
			"baseline=%d got=%d", baseline, got)

	// Re-baseline after the create so the renewal assertion measures
	// only the Update event, not residual feedback loops.
	time.Sleep(750 * time.Millisecond)
	preRenew := rec.countFor(key)

	// Renewing the Lease must fire a fresh reconcile so the receiver
	// observes the refreshed RenewTime promptly.
	var live coordinationv1.Lease
	require.NoError(t, env.Client.Get(ctx,
		types.NamespacedName{Namespace: apiv1alpha1.LeaseNamespace, Name: leaseName}, &live))
	renew := metav1.MicroTime{Time: time.Now().Add(10 * time.Second)}
	live.Spec.RenewTime = &renew
	require.NoError(t, env.Client.Update(ctx, &live))

	got = rec.waitForAtLeast(key, preRenew+1, 10*time.Second)
	assert.GreaterOrEqual(t, got, preRenew+1,
		"Lease renewal must enqueue the matching receiver; without that "+
			"signal the operator would not see a freshly-renewed Origin "+
			"until the next unrelated event. preRenew=%d got=%d",
		preRenew, got)
}

// TestReceiver_LeaseWatchIgnoresOtherNamespaces guards the
// namespace-scoped predicate: Leases outside mxl-system must not
// enqueue any receiver, otherwise leader election Leases in
// kube-system would wake every receiver on every renewal tick.
func TestReceiver_LeaseWatchIgnoresOtherNamespaces(t *testing.T) {
	env := sharedEnv(t)
	ctx := context.Background()
	ns := freshNamespace(t, env, "lease-watch-ignore")
	ensureLeaseNamespace(t, env.Client)

	const flowID = "22222222-3333-4444-5555-666666666666"
	require.NoError(t, env.Client.Create(ctx,
		testutil.NewReceiver(ns, "r", testutil.WithReceiverFlowID(flowID))))

	rec := newRecordingReconciler(&Reconciler{})
	stop := startReceiverManager(t, env, rec)
	defer stop()

	key := client.ObjectKey{Namespace: ns, Name: "r"}
	baseline := rec.waitForAtLeast(key, 1, 5*time.Second)
	require.GreaterOrEqual(t, baseline, 1)

	// Lease in default namespace with a name that would otherwise
	// match this receiver's flow ID. The predicate must reject it
	// before the mapper runs.
	leaseName := apiv1alpha1.LeaseName(flowID, "nodea")
	now := metav1.MicroTime{Time: time.Now()}
	duration := int32(30)
	t.Cleanup(func() {
		_ = env.Client.Delete(context.Background(), &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: leaseName},
		})
	})
	require.NoError(t, env.Client.Create(ctx, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      leaseName,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptr("not-the-agent"),
			LeaseDurationSeconds: &duration,
			RenewTime:            &now,
		},
	}))

	// Allow time for any spurious dispatch to land; assert the count
	// does not move beyond the baseline floor. No other watches fire
	// in this namespace, so growth past baseline would mean the Lease
	// watch leaked.
	time.Sleep(750 * time.Millisecond)
	got := rec.countFor(key)
	assert.LessOrEqual(t, got, baseline,
		"a Lease outside mxl-system must not enqueue the receiver; the "+
			"namespace predicate is what keeps the operator from waking on "+
			"every kube-system leader-election renew tick. baseline=%d got=%d",
		baseline, got)
}

func ptr[T any](v T) *T { return &v }
