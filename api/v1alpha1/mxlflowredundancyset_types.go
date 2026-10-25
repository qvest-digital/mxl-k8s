package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RedundancyGranularity is the unit a redundancy set switches members in.
// +kubebuilder:validation:Enum=Grain
type RedundancyGranularity string

// RedundancyGrain publishes a grain only once one member holds it
// complete, so a grain never mixes members. Audio switches per page of
// the channel buffer either way.
const RedundancyGrain RedundancyGranularity = "Grain"

// RedundancyPreference is how a redundancy set picks among members that
// all hold a grain.
// +kubebuilder:validation:Enum=Ordered;FirstArrival
type RedundancyPreference string

const (
	// RedundancyOrdered serves from the first member in spec.members that
	// delivers, and stays with it while it does.
	RedundancyOrdered RedundancyPreference = "Ordered"
	// RedundancyFirstArrival serves each grain from whichever member
	// delivered it first.
	RedundancyFirstArrival RedundancyPreference = "FirstArrival"
)

// RedundancyFlowRef names one flow by domain and id.
type RedundancyFlowRef struct {
	// FlowID is the flow's id.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`
	FlowID string `json:"flowID"`

	// Domain is the MxlDomain the flow belongs to; empty is the primary
	// domain.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Domain string `json:"domain,omitempty"`
}

// MxlFlowRedundancySetSpec declares a flow served from several member
// flows: on every node a reader opens the output, each grain, or each
// page of an audio channel buffer, comes from a member that holds it,
// without copying payload.
//
// Members must be the same flow in content and timing: the same format,
// geometry and rate, with indices anchored to the same epoch, as two
// copies of one source over separate paths or two producers locked to
// one reference are.
//
// The output is none of the members: a gateway building the output
// replaces whatever directory holds its id.
// +kubebuilder:validation:XValidation:rule="!self.members.exists(m, m.flowID == self.output.flowID && has(m.domain) == has(self.output.domain) && (!has(m.domain) || m.domain == self.output.domain))",message="output must not be one of the members"
type MxlFlowRedundancySetSpec struct {
	// Members are the flows the output is served from, in order of
	// preference. At most seven: a merged flow selects among its members
	// and one placeholder for what no member delivered.
	// +kubebuilder:validation:MinItems=2
	// +kubebuilder:validation:MaxItems=7
	// +listType=atomic
	Members []RedundancyFlowRef `json:"members"`

	// Output is the flow readers open. Its id must name no other flow.
	// +kubebuilder:validation:Required
	Output RedundancyFlowRef `json:"output"`

	// Granularity is the unit members are switched in. Grain is the only
	// one.
	// +kubebuilder:default=Grain
	// +optional
	Granularity RedundancyGranularity `json:"granularity,omitempty"`

	// SkewBudget is how long a grain or sample range may be late from
	// every member before it is published as missing. It bounds the
	// latency a member outage adds; members further apart than this lose
	// grains. Defaults to 40ms.
	// +kubebuilder:default="40ms"
	// +optional
	SkewBudget metav1.Duration `json:"skewBudget,omitempty"`

	// Preference picks among members that all deliver. Defaults to
	// Ordered.
	// +kubebuilder:default=Ordered
	// +optional
	Preference RedundancyPreference `json:"preference,omitempty"`
}

// MxlFlowRedundancySetStatus reports where the output is served.
type MxlFlowRedundancySetStatus struct {
	// Nodes are the nodes a reader asked for the output on. The gateway
	// on each serves it, from member mirrors the operator creates there.
	// A node's agent removes it once no reader there has used the output
	// for a minute; the operator removes a node that is deleted. Only a
	// reader in a pod counts: a process outside any pod keeps no node.
	// +optional
	// +listType=set
	Nodes []string `json:"nodes,omitempty"`

	// ObservedGeneration is the spec generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions: Valid reports whether the members can be merged,
	// MembersReady whether every member is produced in the output's
	// geometry.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=mxlrs
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name=Output,type=string,JSONPath=`.spec.output.flowID`
// +kubebuilder:printcolumn:name=Members,type=integer,JSONPath=`.spec.members.length()`,priority=1
// +kubebuilder:printcolumn:name=Age,type=date,JSONPath=`.metadata.creationTimestamp`

// MxlFlowRedundancySet serves one flow from several member flows,
// switching members per grain, or per page of audio, wherever one fails
// to deliver. The output's members all have to be on an mxlfs domain of
// the reading node, and to agree in format, rate and geometry.
//
// While the output waits up to the skew budget for a grain no member has,
// the members keep writing a ring period ahead, into slots the output
// still offers as its oldest grains. The gateway moves each such slot to
// a member that still holds it, or to an INVALID grain, at every grain
// boundary; a gateway that gets no CPU for longer than a grain can leave
// the oldest grain of the output reading the next ring period's picture.
// Readers at the output's head are unaffected. A member that goes away
// leaves the output to the others. One that comes back in the output's
// geometry and ring depth takes its old place in the output where the
// node's mxlfs has MXLFS_IOC_VIEW_REPLACE_SOURCE. The output is built
// again, and its readers have to reopen it, without that ioctl; when a
// replacement fails part-way, or its new ring cannot be opened for
// several rechecks in a row; when a member comes back in another
// geometry, or with another ring depth after the domain's history
// duration changed; when a member that was absent or left out when the
// output was built comes back; and when a set is created again. A member
// whose geometry differs from the output's is left out of it while
// another member keeps that geometry.
type MxlFlowRedundancySet struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MxlFlowRedundancySetSpec   `json:"spec,omitempty"`
	Status MxlFlowRedundancySetStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MxlFlowRedundancySetList is a list of MxlFlowRedundancySet resources.
type MxlFlowRedundancySetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MxlFlowRedundancySet `json:"items"`
}
