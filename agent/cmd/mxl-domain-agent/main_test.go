package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/qvest-digital/mxl-k8s/agent/internal/config"
	"github.com/qvest-digital/mxl-k8s/agent/internal/flowpublisher"
	"github.com/qvest-digital/mxl-k8s/agent/internal/intent"
	"github.com/qvest-digital/mxl-k8s/agent/internal/mxlfsmount"
	"github.com/qvest-digital/mxl-k8s/agent/internal/observed"
	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Without mxlfs the publisher asks about no redundancy set: each flow
// that appears would otherwise cost an uncached List of a kind the agent
// then has no access to.
func TestNewFlowPublisherConsultsSetsOnlyWithMxlfs(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	fp, sets := newFlowPublisher(&config.Config{DomainPath: "/run/mxl/domain", NodeName: "n1"}, c, nil)
	assert.Nil(t, fp.IsOutput)
	assert.Nil(t, sets)

	fp, sets = newFlowPublisher(&config.Config{DomainPath: "/run/mxl/domain", NodeName: "n1", Mxlfs: true}, c, nil)
	assert.NotNil(t, fp.IsOutput)
	assert.NotNil(t, sets)
}

func TestRequireKindNamesTheMissingCRD(t *testing.T) {
	err := requireKind(meta.NewDefaultRESTMapper(nil), "MxlFlowRedundancySet")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MxlFlowRedundancySet.mxl.qvest-digital.com is not served")

	m := meta.NewDefaultRESTMapper(nil)
	m.Add(mxlv1alpha1.GroupVersion.WithKind("MxlFlowRedundancySet"), meta.RESTScopeRoot)
	require.NoError(t, requireKind(m, "MxlFlowRedundancySet"))
}

// Without mxlfs a CSI publish binds at once, as a plain directory needs
// nothing first; with it, only once the agent has decided the domain.
func TestCSIReadyWaitsOnlyWithMxlfs(t *testing.T) {
	assert.Nil(t, csiReady(nil))

	m := &mxlfsmount.Mounter{IsMxlfs: func(string) bool { return true }}
	ready := csiReady(m)
	dir := t.TempDir()
	assert.False(t, ready(dir))
	require.NoError(t, m.Ensure(dir))
	assert.True(t, ready(dir))
}

type podsByPID map[int32]*corev1.Pod

func (p podsByPID) PodForPID(_ context.Context, pid int32) (*corev1.Pod, error) {
	if pod, ok := p[pid]; ok {
		return pod, nil
	}
	return nil, errors.New("no pod")
}

// Only the components that open placeholders themselves are skipped. A
// consumer deployed into the agent's namespace still gets its mirror,
// and a pid whose pod lookup fails is not silently dropped as ours.
func TestOursIsOnlyMxlK8sComponents(t *testing.T) {
	pod := func(ns, component string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns,
			Labels: map[string]string{"app.kubernetes.io/component": component}}}
	}
	o := ours(podsByPID{
		1: pod("mxl-system", "gateway"),
		2: pod("mxl-system", "exporter"),
		3: pod("mxl-system", "agent"),
		4: pod("mxl-system", ""),
		5: pod("media", "gateway"),
	}, ownPod("mxl-system"))
	ctx := context.Background()
	assert.True(t, o(ctx, 1), "gateway")
	assert.True(t, o(ctx, 2), "exporter")
	assert.True(t, o(ctx, 3), "agent")
	assert.False(t, o(ctx, 4), "a consumer in the agent's namespace")
	assert.False(t, o(ctx, 5), "a gateway-labelled pod elsewhere")
	assert.False(t, o(ctx, 6), "a pid with no resolvable pod")
}

// pidsInPods resolves pids to pod UIDs and UIDs to pods as /proc and
// the API server would, counting the API lookups.
type pidsInPods struct {
	uid     map[int32]string
	pod     map[string]*corev1.Pod
	lookups int
}

func (p *pidsInPods) UIDForPID(pid int32) (string, error) {
	if uid, ok := p.uid[pid]; ok {
		return uid, nil
	}
	return "", errors.New("not in a pod")
}

func (p *pidsInPods) PodForUID(_ context.Context, uid string) (*corev1.Pod, error) {
	p.lookups++
	if pod, ok := p.pod[uid]; ok {
		return pod, nil
	}
	return nil, errors.New("no pod")
}

// A producer is positive evidence only: the gateway is not one, and a
// pid whose pod cannot be resolved is no evidence either way. Counting
// it would claim Origin for a mirror copy, which no later publish
// demotes. The rescan asks about every writer of every copy each pass,
// so each pod is looked up once, not once per pid per pass.
func TestProducersIsAResolvedPodThatIsNotOurs(t *testing.T) {
	pod := func(ns, component string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns,
			Labels: map[string]string{"app.kubernetes.io/component": component}}}
	}
	pods := &pidsInPods{
		uid: map[int32]string{1: "gw", 2: "gw", 3: "mf", 4: "gone"},
		pod: map[string]*corev1.Pod{"gw": pod("mxl-system", "gateway"), "mf": pod("media", "")},
	}
	p, err := newProducers(pods, "mxl-system")
	require.NoError(t, err)
	ctx := context.Background()
	for range 3 {
		assert.False(t, p.Producer(ctx, 1), "the gateway filling a mirror target")
		assert.False(t, p.Producer(ctx, 2), "another of the gateway's pids")
		assert.True(t, p.Producer(ctx, 3), "a media function")
		assert.False(t, p.Producer(ctx, 4), "a pid whose pod does not resolve")
		assert.False(t, p.Producer(ctx, 5), "a pid in no pod")
	}
	assert.Equal(t, 2+3, pods.lookups,
		"one lookup per pod that resolves; one that does not is asked about again")
}

// Without the agent's namespace no pod is mxl-k8s's own, and the gateway
// filling every mirror copy on the node would be taken for its producer.
func TestProducersNeedTheAgentsNamespace(t *testing.T) {
	_, err := newProducers(&pidsInPods{}, "")
	require.Error(t, err)
}

// The primary domain is materialised by an MxlDomain of its own name,
// but its flows, and the outputs of sets in it, are spelled with no
// domain name. Reported under that name, a reader of a set's output was
// never seen using it, and the node left the set every grace period:
// the output was torn down under its readers and the member mirrors
// collected.
func TestDomainDevicesSpellsThePrimaryDomainWithoutAName(t *testing.T) {
	root := t.TempDir()
	primary := filepath.Join(root, "domain")
	require.NoError(t, os.Mkdir(primary, 0o755))
	dev, err := observed.Device(primary)
	require.NoError(t, err)

	got := domainDevices(primary, intent.Domains{Root: root, Primary: "domain", ByDir: map[string]string{"domain": "default"}})
	assert.Equal(t, map[string]string{dev: ""}, got)
}

// The gateway maps the output it merges and the exporter reads every
// flow; counted as readers they would keep every output in use forever.
func TestFlowsInUseLeavesOutMxlK8sComponents(t *testing.T) {
	pod := func(name, ns, uid, node, component string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID(uid),
			Labels: map[string]string{"app.kubernetes.io/component": component}}, Spec: corev1.PodSpec{NodeName: node}}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pod("gw", "mxl-system", "gw", "n1", "gateway"), pod("exp", "mxl-system", "exp", "n1", "exporter"),
			pod("viewer", "media", "viewer", "n1", "")).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*corev1.Pod).Spec.NodeName}
		}).Build()
	const merged, other = "33333333-3333-4333-8333-333333333333", "44444444-4444-4444-8444-444444444444"
	scan := func() (map[string][]observed.Use, error) {
		return map[string][]observed.Use{
			"gw":     {{FlowID: merged, Direction: observed.Output}},
			"exp":    {{FlowID: merged, Direction: observed.Input}},
			"viewer": {{Domain: "studio", FlowID: other, Direction: observed.Input}},
		}, nil
	}

	got, err := flowsInUse(context.Background(), c, "n1", ownPod("mxl-system"), scan)
	require.NoError(t, err)
	assert.Equal(t, map[mxlv1alpha1.FlowRef]bool{{Domain: "studio", ID: other}: true}, got)
}

// A domain other than the primary on mxlfs gets the placeholder and
// redundancy set handling the primary gets, for as long as it is
// tracked: the gateway lists placeholders and merges set outputs in
// every domain it opens. A domain left plain gets none.
func TestTrackerRunsMxlfsForATrackedDomainOnMxlfs(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	onMxlfs, plain := t.TempDir(), t.TempDir()
	watched := map[string]*flowpublisher.Publisher{}
	type run struct {
		ctx context.Context
		dir string
		fp  *flowpublisher.Publisher
	}
	var runs []run
	waited := 0
	tracking := domainTracking{
		Client: c,
		Node:   "n1",
		WatchFlows: func(_ context.Context, dir string, fp *flowpublisher.Publisher) (func(), error) {
			watched[dir] = fp
			return func() {}, nil
		},
		Mxlfs: &trackerMxlfs{
			IsMxlfs:  func(dir string) bool { return dir == onMxlfs },
			IsOutput: func(context.Context, string, string) bool { return true },
			Producer: func(context.Context, int32) bool { return true },
			Run: func(ctx context.Context, dir string, fp *flowpublisher.Publisher) (func(), error) {
				runs = append(runs, run{ctx, dir, fp})
				return func() { <-ctx.Done(); waited++ }, nil
			},
		},
	}
	ctx := context.Background()

	tr, err := tracking.start(ctx, "studio", onMxlfs)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	assert.Equal(t, onMxlfs, runs[0].dir)
	assert.Same(t, watched[onMxlfs], runs[0].fp, "placeholder changes go to the domain's own publisher")
	assert.Equal(t, "studio", runs[0].fp.Domain)
	assert.NotNil(t, runs[0].fp.IsOutput, "a set's output merged here is no location to publish")
	assert.NotNil(t, runs[0].fp.Producer, "a producer that takes over a mirror's copy here is the Origin")
	require.NoError(t, runs[0].ctx.Err())
	tr.Stop()
	assert.Error(t, runs[0].ctx.Err(), "stopping the tracker stops its mxlfs handling")
	assert.Equal(t, 1, waited, "and waits for it to end")

	tr, err = tracking.start(ctx, "local", plain)
	require.NoError(t, err)
	tr.Stop()
	assert.Len(t, runs, 1, "a plain domain has no placeholders to watch")
	assert.Nil(t, watched[plain].IsOutput)
	assert.Nil(t, watched[plain].Producer, "off mxlfs the gateway's locks are not told from a producer's")
}

type leases struct{ released []string }

func (l *leases) Renew(context.Context, string) error { return nil }

func (l *leases) Release(_ context.Context, key string) error {
	l.released = append(l.released, key)
	return nil
}

// A tracked domain whose mxlfs handling cannot start is not tracked:
// the manager retries it on the next sync, the flow watch already
// started ends, and the node gives up the leases of the flows the
// initial sync found, as a stopped tracker does, rather than leave them
// to run out unrenewed.
func TestTrackerFailsWhenMxlfsCannotStart(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	dir := t.TempDir()
	const id = "55555555-5555-4555-8555-555555555555"
	require.NoError(t, os.Mkdir(filepath.Join(dir, id+".mxl-flow"), 0o755))
	held := &leases{}
	var watchCtx context.Context
	tracking := domainTracking{
		Client: c,
		Node:   "n1",
		Lease:  held,
		WatchFlows: func(ctx context.Context, _ string, _ *flowpublisher.Publisher) (func(), error) {
			watchCtx = ctx
			return func() {}, nil
		},
		Mxlfs: &trackerMxlfs{
			IsMxlfs:  func(string) bool { return true },
			IsOutput: func(context.Context, string, string) bool { return false },
			Run: func(context.Context, string, *flowpublisher.Publisher) (func(), error) {
				return nil, errors.New("no marker directory")
			},
		},
	}

	_, err := tracking.start(context.Background(), "studio", dir)
	require.Error(t, err)
	assert.Error(t, watchCtx.Err())
	assert.Equal(t, []string{mxlv1alpha1.FlowRef{Domain: "studio", ID: id}.Name()}, held.released)
}

// Every watch of a domain on mxlfs is set up before runMxlfs returns. On
// a directory that is no mxlfs one of them cannot be, so runMxlfs fails,
// the tracker with it, and the manager retries on its next sync; nothing
// of what it set up is left running.
func TestRunMxlfsFailsOnADirectoryThatIsNoMxlfs(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	dir := t.TempDir()

	wait, err := runMxlfs(ctx, dir, &flowpublisher.Publisher{Client: c, DomainPath: dir}, nil, nil, nil)
	require.Error(t, err)
	assert.Nil(t, wait)
}
