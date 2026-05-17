// Package podlookup resolves host PIDs to the Kubernetes Pod that
// owns them, by parsing /proc/<pid>/cgroup for the kubepods slice
// and looking the matching UID up via the API server.
package podlookup

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Resolver holds the agent's local node name and the controller-
// runtime client used to enumerate pods.
type Resolver struct {
	Client   client.Client
	NodeName string
}

// PodForPID returns the Pod scheduled on this node whose
// metadata.uid matches the kubepods slice for the given host PID.
func (r *Resolver) PodForPID(ctx context.Context, pid int32) (*corev1.Pod, error) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return nil, fmt.Errorf("read cgroup for pid %d: %w", pid, err)
	}
	uid, ok := parsePodUID(string(raw))
	if !ok {
		return nil, fmt.Errorf("pid %d is not under a kubepods cgroup", pid)
	}

	var pods corev1.PodList
	if err := r.Client.List(ctx, &pods); err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if string(p.UID) == uid && p.Spec.NodeName == r.NodeName {
			return p, nil
		}
	}
	return nil, fmt.Errorf("no pod with uid %s on node %s", uid, r.NodeName)
}

// systemd cgroup driver: `.../kubepods-burstable-podUID.slice/...`.
// UID hyphens are replaced with underscores.
var systemdPodRE = regexp.MustCompile(`pod([0-9a-f_]{36})\.slice`)

// cgroupfs driver: `.../kubepods/burstable/podUID/...`. UID keeps
// its native hyphenated form.
var cgroupfsPodRE = regexp.MustCompile(`pod([0-9a-f-]{36})(?:/|$)`)

// parsePodUID accepts the raw contents of /proc/<pid>/cgroup and
// returns the canonical hyphenated pod UID if the process is under
// a kubepods slice.
func parsePodUID(cgroupContents string) (string, bool) {
	if m := systemdPodRE.FindStringSubmatch(cgroupContents); m != nil {
		return strings.ReplaceAll(m[1], "_", "-"), true
	}
	if m := cgroupfsPodRE.FindStringSubmatch(cgroupContents); m != nil {
		return m[1], true
	}
	return "", false
}
