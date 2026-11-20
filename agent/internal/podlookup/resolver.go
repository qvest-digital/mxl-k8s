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
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Resolver holds the agent's local node name and the controller-
// runtime client used to enumerate pods.
type Resolver struct {
	Client   client.Client
	NodeName string

	// listing holds one List at a time, so lookups that miss together
	// share it. A channel rather than a mutex: a lookup waiting for
	// another's List gives up when its own request ends.
	listing     chan struct{}
	listingOnce sync.Once
	mu          sync.Mutex
	byUID       map[string]*corev1.Pod // this node's pods, from the last List
	listed      time.Time              // when the last List started
}

// podsFresh is how long the last List answers for the pods it holds. A
// pod's name, namespace and UID never change, and the labels read are
// not expected to.
const podsFresh = 30 * time.Second

// listEvery is the shortest time between two Lists. A lookup that misses
// sooner waits for it, so a pod that started after the last List still
// resolves, and a flood of unknown UIDs costs one List per interval.
// Tests shorten it.
var listEvery = time.Second

// listTimeout bounds one List, which every lookup that misses meanwhile
// waits for: a request with no deadline of its own must not hold them
// past it. Tests shorten it.
var listTimeout = 5 * time.Second

// PodForPID returns the Pod scheduled on this node whose
// metadata.uid matches the kubepods slice for the given host PID.
func (r *Resolver) PodForPID(ctx context.Context, pid int32) (*corev1.Pod, error) {
	uid, err := r.UIDForPID(pid)
	if err != nil {
		return nil, err
	}
	return r.PodForUID(ctx, uid)
}

// UIDForPID returns the UID of the pod whose kubepods slice holds the
// given host PID, from /proc alone.
func (r *Resolver) UIDForPID(pid int32) (string, error) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return "", fmt.Errorf("read cgroup for pid %d: %w", pid, err)
	}
	uid, ok := parsePodUID(string(raw))
	if !ok {
		return "", fmt.Errorf("pid %d is not under a kubepods cgroup", pid)
	}
	return uid, nil
}

// PodForUID returns this node's pod with uid. The pods come from a List
// of this node's pods, made again when uid is not in the last one or it
// is older than podsFresh: the client is uncached, and every placeholder
// open and wanted flow resolves its pid.
func (r *Resolver) PodForUID(ctx context.Context, uid string) (*corev1.Pod, error) {
	missed := time.Now()
	if p, ok := r.cached(uid, missed); ok {
		return p, nil
	}
	r.listingOnce.Do(func() { r.listing = make(chan struct{}, 1) })
	select {
	case r.listing <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("list pods: %w", ctx.Err())
	}
	defer func() { <-r.listing }()
	r.mu.Lock()
	listed := r.listed
	r.mu.Unlock()
	// A List started after the miss holds every pod that had a process
	// by then, so its answer stands, found or not.
	if listed.After(missed) {
		if p, ok := r.cached(uid, time.Now()); ok {
			return p, nil
		}
		return nil, r.notFound(uid)
	}
	if wait := time.Until(listed.Add(listEvery)); wait > 0 {
		t := time.NewTimer(wait)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("list pods: %w", ctx.Err())
		case <-t.C:
		}
	}
	start := time.Now()
	var pods corev1.PodList
	lctx, cancel := context.WithTimeout(ctx, listTimeout)
	err := r.Client.List(lctx, &pods, client.MatchingFields{"spec.nodeName": r.NodeName})
	cancel()
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	byUID := make(map[string]*corev1.Pod, len(pods.Items))
	for i := range pods.Items {
		if p := &pods.Items[i]; p.Spec.NodeName == r.NodeName {
			byUID[string(p.UID)] = p
		}
	}
	r.mu.Lock()
	r.byUID, r.listed = byUID, start
	r.mu.Unlock()
	if p, ok := r.cached(uid, start); ok {
		return p, nil
	}
	return nil, r.notFound(uid)
}

// cached returns a copy of the pod with uid from a List fresh at now.
func (r *Resolver) cached(uid string, now time.Time) (*corev1.Pod, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.byUID[uid]
	if !ok || now.Sub(r.listed) >= podsFresh {
		return nil, false
	}
	return p.DeepCopy(), true
}

func (r *Resolver) notFound(uid string) error {
	return fmt.Errorf("no pod with uid %s on node %s", uid, r.NodeName)
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
