package v1alpha1

import (
	"crypto/sha256"
	"encoding/hex"
)

// MirrorSide is one of the two gateways serving a mirror.
type MirrorSide string

const (
	// MirrorSideSource is the gateway on spec.sourceNode.
	MirrorSideSource MirrorSide = "source"
	// MirrorSideTarget is the gateway on spec.targetNode.
	MirrorSideTarget MirrorSide = "target"
)

// LabelMirrorSide marks a mirror heartbeat Lease and names the side
// whose gateway renews it.
const LabelMirrorSide = "mxl.qvest-digital.com/mirror-side"

// AnnotationMirror names the MxlFlowMirror a heartbeat Lease belongs
// to, as "<namespace>/<name>". The Lease lives in LeaseNamespace and a
// mirror may live in any namespace, so an owner reference cannot carry
// the link.
const AnnotationMirror = "mxl.qvest-digital.com/mirror"

// mirrorLeasePrefix is shared by every mirror heartbeat Lease name and
// differs from the Origin Leases' prefix, so ParseLeaseName never takes
// one for the other.
const mirrorLeasePrefix = "mxl-mirror-"

// MirrorLeaseName is the name of the heartbeat Lease one side of the
// mirror namespace/name renews in LeaseNamespace. spec.renewTime is
// that side's progress time: the last completed transfer for the
// source, the last committed grain for the target.
//
// The mirror's namespace and name are hashed because together they
// can exceed what a Lease name holds; AnnotationMirror carries them in
// the clear.
func MirrorLeaseName(namespace, name string, side MirrorSide) string {
	sum := sha256.Sum256([]byte(namespace + "/" + name))
	return mirrorLeasePrefix + hex.EncodeToString(sum[:16]) + "-" + string(side)
}

// IsMirrorLease reports whether a Lease with these labels is a mirror
// heartbeat. Heartbeats share LeaseNamespace with the Origin Leases and
// renew every few seconds per mirror side, so a watch that is after
// Origin Leases filters them out.
func IsMirrorLease(labels map[string]string) bool {
	_, ok := labels[LabelMirrorSide]
	return ok
}
