package intentsock

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// observingDispatcher satisfies both halves of the contract so the
// server routes an attach notification away from Materialize.
type observingDispatcher struct {
	fakeDispatcher
	mu        sync.Mutex
	attached  []dispatcherCall
	attachErr error
}

func (o *observingDispatcher) NotifyProducerAttached(_ context.Context, pid int32, path string) error {
	o.mu.Lock()
	o.attached = append(o.attached, dispatcherCall{pid: pid, path: path})
	o.mu.Unlock()
	return o.attachErr
}

func (o *observingDispatcher) lastAttach() dispatcherCall {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.attached) == 0 {
		return dispatcherCall{}
	}
	return o.attached[len(o.attached)-1]
}

const attachPath = "/run/mxl/domain/11111111-2222-3333-4444-555555555555.mxl-flow/data"

// An attach notification reports a producer, not a consumer waiting
// on a mirror; routing it into Materialize would make the agent build
// a mirror for a flow the node already produces.
func TestServer_AttachEventRoutesToObserver(t *testing.T) {
	d := &observingDispatcher{}
	srv := &Server{Dispatcher: d, PeerPIDFn: func(net.Conn) (int32, error) { return 4242, nil }}

	resp := runOneRequest(t, srv, `{"path":"`+attachPath+`","event":"attached"}`)
	assert.Contains(t, resp, `"ok":true`)

	assert.Equal(t, dispatcherCall{pid: 4242, path: attachPath}, d.lastAttach())
	assert.Empty(t, d.calls, "an attach notification must not drive materialization")
}

// Every shim built before the field existed sends no event, and those
// have to keep materializing.
func TestServer_MissingEventStillMaterializes(t *testing.T) {
	d := &observingDispatcher{}
	srv := &Server{Dispatcher: d, PeerPIDFn: func(net.Conn) (int32, error) { return 7, nil }}

	resp := runOneRequest(t, srv, `{"path":"`+attachPath+`"}`)
	assert.Contains(t, resp, `"ok":true`)

	assert.Equal(t, dispatcherCall{pid: 7, path: attachPath}, d.lastCall())
	assert.Empty(t, d.attached)
}

// A dispatcher that only materializes must not fail the connection.
func TestServer_AttachEventWithoutObserverIsAccepted(t *testing.T) {
	d := &fakeDispatcher{}
	srv := &Server{Dispatcher: d, PeerPIDFn: func(net.Conn) (int32, error) { return 9, nil }}

	resp := runOneRequest(t, srv, `{"path":"`+attachPath+`","event":"attached"}`)
	assert.Contains(t, resp, `"ok":true`)
	assert.Empty(t, d.calls)
	require.False(t, strings.Contains(resp, `"error"`))
}
