// Package redundancy is the agent's side of MxlFlowRedundancySets: it
// records the nodes a reader wants a set's output on, and keeps outputs
// out of the flows it publishes.
//
// An output is merged on each node it is read on, from member mirrors
// there; it is never a location of a flow other nodes mirror from. Its
// pages move between members, which a NIC registration would not follow.
package redundancy

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// Sets finds sets by output.
type Sets struct {
	Client   client.Client
	NodeName string
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
// that fails answers false: publishing an output costs a wrong location
// until the next pass, while hiding a real flow costs its consumers.
func (s *Sets) IsOutput(ctx context.Context, domain, id string) bool {
	set, err := s.outputOf(ctx, domain, id)
	return err == nil && set != nil
}

// Want records that a reader on this node waits for flow id in domain,
// when that is a set's output, and reports whether it was one.
func (s *Sets) Want(ctx context.Context, domain, id string) (bool, error) {
	set, err := s.outputOf(ctx, domain, id)
	if err != nil || set == nil {
		return false, err
	}
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
