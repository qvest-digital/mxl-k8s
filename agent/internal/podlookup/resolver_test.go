package podlookup

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
