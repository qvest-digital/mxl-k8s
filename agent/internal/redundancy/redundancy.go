// Package redundancy is the agent's side of MxlFlowRedundancySets: it
// records the nodes a reader wants a set's output on, removes this node
// again once no reader here uses the output, and keeps outputs out of
// the flows it publishes.
//
// An output is merged on each node it is read on, from member mirrors
// there; it is never a location of a flow other nodes mirror from. Its
// pages move between members, which a NIC registration would not follow.
package redundancy

import (
	"context"
	"slices"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// ReleaseGrace is how long an output may go unused on this node before
// the node leaves the set's status.nodes. A reader that restarts, or
// waits while its output is first built from member mirrors, uses
// nothing for a while, and leaving would tear the output down under it.
const ReleaseGrace = time.Minute

// leaveTimeout bounds leaving one set. Leaving holds the lock every Want
// records a use under, so a status update the API server is slow to
// answer would hold up each reader waiting for an output on this node.
const leaveTimeout = 5 * time.Second

// Sets finds sets by output.
type Sets struct {
	Client   client.Client
	NodeName string

	now      func() time.Time // a seam for tests; nil is time.Now
	timeout  time.Duration    // a seam for tests; zero is leaveTimeout
	mu       sync.Mutex
	lastUsed map[string]time.Time // by set name: last seen used here
}

func (s *Sets) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func (s *Sets) used(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastUsed == nil {
		s.lastUsed = map[string]time.Time{}
	}
	s.lastUsed[name] = s.clock()
}

// outputOf returns the set whose output is flow id in domain.
func (s *Sets) outputOf(ctx context.Context, domain, id string) (*mxlv1alpha1.MxlFlowRedundancySet, error) {
	var list mxlv1alpha1.MxlFlowRedundancySetList
	if err := s.Client.List(ctx, &list); err != nil {
		return nil, err
	}
	for i := range list.Items {
		o := list.Items[i].Spec.Output
		if o.FlowID == id && o.Domain == domain {
			return &list.Items[i], nil
		}
	}
	return nil, nil
}

// IsOutput reports whether flow id in domain is a set's output. A lookup
// that fails is an error, not "no": an MxlFlow published for an output
// makes the operator refuse its set, which stops the merge on every node
// until the object is collected.
func (s *Sets) IsOutput(ctx context.Context, domain, id string) (bool, error) {
	set, err := s.outputOf(ctx, domain, id)
	return set != nil, err
}

// Want records that a reader on this node waits for flow id in domain,
// when that is a set's output, and reports whether it was one.
func (s *Sets) Want(ctx context.Context, domain, id string) (bool, error) {
	set, err := s.outputOf(ctx, domain, id)
	if err != nil || set == nil {
		return false, err
	}
	s.used(set.Name)
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cur mxlv1alpha1.MxlFlowRedundancySet
		if err := s.Client.Get(ctx, types.NamespacedName{Name: set.Name}, &cur); err != nil {
			return err
		}
		for _, n := range cur.Status.Nodes {
			if n == s.NodeName {
				return nil
			}
		}
		cur.Status.Nodes = append(cur.Status.Nodes, s.NodeName)
		return s.Client.Status().Update(ctx, &cur)
	})
	return true, err
}

// Release takes this node out of status.nodes of every set whose output
// no process here has used for grace. inUse reports the flows processes
// on this node other than mxl-k8s's own map; it is asked only while some
// set lists this node. An output's grace starts when this agent first
// sees the set list the node, so a restarted agent gives every reader
// the whole grace again.
func (s *Sets) Release(ctx context.Context, inUse func() (map[mxlv1alpha1.FlowRef]bool, error), grace time.Duration) error {
	var list mxlv1alpha1.MxlFlowRedundancySetList
	if err := s.Client.List(ctx, &list); err != nil {
		return err
	}
	var here []*mxlv1alpha1.MxlFlowRedundancySet
	for i := range list.Items {
		if slices.Contains(list.Items[i].Status.Nodes, s.NodeName) {
			here = append(here, &list.Items[i])
		}
	}
	var used map[mxlv1alpha1.FlowRef]bool
	if len(here) > 0 {
		var err error
		if used, err = inUse(); err != nil {
			return err
		}
	}
	now := s.clock()
	var idle []string
	s.mu.Lock()
	if s.lastUsed == nil {
		s.lastUsed = map[string]time.Time{}
	}
	listed := map[string]bool{}
	for _, set := range here {
		listed[set.Name] = true
		last, seen := s.lastUsed[set.Name]
		switch {
		case used[mxlv1alpha1.FlowRef{Domain: set.Spec.Output.Domain, ID: set.Spec.Output.FlowID}] || !seen:
			s.lastUsed[set.Name] = now
		case now.Sub(last) >= grace:
			idle = append(idle, set.Name)
		}
	}
	for name := range s.lastUsed {
		if !listed[name] {
			delete(s.lastUsed, name)
		}
	}
	s.mu.Unlock()
	for _, name := range idle {
		if err := s.leave(ctx, name, grace); err != nil {
			return err
		}
	}
	return nil
}

// leave removes this node from the set's status.nodes, unless the output
// was used again within grace since Release found it idle. The check and
// the update hold the lock Want records a use under: a Want that finds
// the node still listed has then recorded its use before the check, and
// one that comes after the update finds the node gone and adds it back.
// It gives up after leaveTimeout; the next Release tries again.
func (s *Sets) leave(ctx context.Context, name string, grace time.Duration) error {
	timeout := s.timeout
	if timeout <= 0 {
		timeout = leaveTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var cur mxlv1alpha1.MxlFlowRedundancySet
		if err := s.Client.Get(ctx, types.NamespacedName{Name: name}, &cur); err != nil {
			return client.IgnoreNotFound(err)
		}
		i := slices.Index(cur.Status.Nodes, s.NodeName)
		if i < 0 {
			return nil
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if last, ok := s.lastUsed[name]; ok && s.clock().Sub(last) < grace {
			return nil
		}
		cur.Status.Nodes = slices.Delete(cur.Status.Nodes, i, i+1)
		return s.Client.Status().Update(ctx, &cur)
	})
}
