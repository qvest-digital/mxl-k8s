package podlookup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// parsePodUID is what stands between a UDS request and the correct
// Pod. Wrong format detection here means SO_PEERCRED-based access
// control routes the wrong consumer's mirrors. Each cgroup format the
// kubelet might emit must round-trip to a canonical hyphenated UID.

func TestParsePodUID(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
		ok   bool
	}{
		{
			name: "systemd format with underscores",
			in:   "0::/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod11111111_2222_3333_4444_555555555555.slice/cri-containerd-abcd.scope",
			want: "11111111-2222-3333-4444-555555555555",
			ok:   true,
		},
		{
			name: "cgroupfs format with hyphens",
			in:   "0::/kubepods/burstable/pod11111111-2222-3333-4444-555555555555/abcd",
			want: "11111111-2222-3333-4444-555555555555",
			ok:   true,
		},
		{
			name: "cgroupfs at the end of the line",
			in:   "0::/kubepods/burstable/pod11111111-2222-3333-4444-555555555555",
			want: "11111111-2222-3333-4444-555555555555",
			ok:   true,
		},
		{
			name: "multi-line cgroup, first match wins",
			in:   "10:devices:/\n0::/kubepods.slice/kubepods-pod11111111_2222_3333_4444_555555555555.slice/x.scope\n",
			want: "11111111-2222-3333-4444-555555555555",
			ok:   true,
		},
		{
			name: "no kubepods entry",
			in:   "0::/user.slice/user-1000.slice/session-1.scope",
			want: "",
			ok:   false,
		},
		{
			name: "uid shape wrong (too short)",
			in:   "0::/kubepods/burstable/pod11111111-2222-3333-4444-55555555",
			want: "",
			ok:   false,
		},
		{
			name: "empty input",
			in:   "",
			want: "",
			ok:   false,
		},
		{
			name: "raw UUID-looking string outside pod-prefix is rejected",
			in:   "0::/something/11111111-2222-3333-4444-555555555555/x",
			want: "",
			ok:   false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parsePodUID(tc.in)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

// The client is uncached and every placeholder open resolves a pid, so
// a list of every pod in the cluster per open would load the API server
// with the cluster's size; only this node's pods can own a local pid.
func TestPodForUIDListsOnlyThisNodesPods(t *testing.T) {
	pod := func(name, uid, node string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: types.UID(uid)},
			Spec: corev1.PodSpec{NodeName: node}}
	}
	var selectors []string
	c := fake.NewClientBuilder().
		WithObjects(pod("here", "11111111-2222-3333-4444-555555555555", "n1"),
			pod("there", "66666666-2222-3333-4444-555555555555", "n2")).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
			return []string{o.(*corev1.Pod).Spec.NodeName}
		}).
		WithInterceptorFuncs(interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, l client.ObjectList, opts ...client.ListOption) error {
			lo := (&client.ListOptions{}).ApplyOptions(opts)
			sel := ""
			if lo.FieldSelector != nil {
				sel = lo.FieldSelector.String()
			}
			selectors = append(selectors, sel)
			return c.List(ctx, l, opts...)
		}}).Build()
	r := &Resolver{Client: c, NodeName: "n1"}

	p, err := r.podForUID(context.Background(), "11111111-2222-3333-4444-555555555555")
	require.NoError(t, err)
	assert.Equal(t, "here", p.Name)
	assert.Equal(t, []string{"spec.nodeName=n1"}, selectors)
}
