// Package agent hosts the per-node MXL domain agent. It watches an MXL
// domain via fanotify, publishes flow state to the Kubernetes API, and
// gates consumer opens to drive on-demand flow materialization.
package agent
