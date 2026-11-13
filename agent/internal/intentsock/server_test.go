package intentsock

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDispatcher captures Materialize calls and returns a canned
// error. Each call records (pid, path) so the test can assert the
// server fed the dispatcher correctly.
type fakeDispatcher struct {
	mu    sync.Mutex
	calls []dispatcherCall
	err   error
}

type dispatcherCall struct {
	pid  int32
	path string
}

func (f *fakeDispatcher) Materialize(ctx context.Context, pid int32, path string) error {
	f.mu.Lock()
	f.calls = append(f.calls, dispatcherCall{pid: pid, path: path})
	f.mu.Unlock()
	return f.err
}

func (f *fakeDispatcher) lastCall() dispatcherCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return dispatcherCall{}
	}
	return f.calls[len(f.calls)-1]
}

// runOneRequest drives the connection through one full request/
// response cycle. The test pair is built on net.Pipe so SO_PEERCRED
// never fires; PeerPIDFn injects the value the test expects.
//
// The Write happens in a background goroutine so a server that
// closes the conn before reading (the PeerPID-error path) does not
// deadlock the test on a blocked synchronous Write.
func runOneRequest(t *testing.T, srv *Server, requestJSON string) string {
	t.Helper()
	client, server := net.Pipe()
	defer client.Close()

	done := make(chan struct{})
	go func() {
		srv.handle(context.Background(), server)
		close(done)
	}()
	go func() {
		_, _ = client.Write([]byte(requestJSON))
	}()

	br := bufio.NewReader(client)
	line, err := br.ReadString('\n')
	require.NoError(t, err)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handle did not return after response")
	}
	return strings.TrimRight(line, "\n")
}

func TestHandle_ValidRequest_Succeeds(t *testing.T) {
	disp := &fakeDispatcher{}
	srv := &Server{
		Dispatcher: disp,
		PeerPIDFn:  func(net.Conn) (int32, error) { return 4242, nil },
	}

	out := runOneRequest(t, srv, `{"path":"/run/mxl/domain/`+
		`11111111-2222-3333-4444-555555555555.mxl-flow/flow_def.json"}`+"\n")

	var resp response
	require.NoError(t, json.Unmarshal([]byte(out), &resp))
	assert.True(t, resp.OK)
	assert.Empty(t, resp.Error)

	call := disp.lastCall()
	assert.Equal(t, int32(4242), call.pid,
		"the server must hand the SO_PEERCRED-derived PID to the dispatcher; "+
			"forwarding the wrong PID would let any caller materialize a "+
			"mirror as a different pod")
	assert.Contains(t, call.path, "flow_def.json")
}

func TestHandle_InvalidJSON_RespondsWithError(t *testing.T) {
	disp := &fakeDispatcher{}
	srv := &Server{
		Dispatcher: disp,
		PeerPIDFn:  func(net.Conn) (int32, error) { return 1, nil },
	}

	out := runOneRequest(t, srv, "not-json\n")

	var resp response
	require.NoError(t, json.Unmarshal([]byte(out), &resp))
	assert.False(t, resp.OK)
	assert.NotEmpty(t, resp.Error)
	assert.Empty(t, disp.calls,
		"malformed input must never reach the dispatcher; otherwise a "+
			"buggy shim could trigger Materialize calls with arbitrary state")
}

func TestHandle_EmptyPath_RespondsWithError(t *testing.T) {
	disp := &fakeDispatcher{}
	srv := &Server{
		Dispatcher: disp,
		PeerPIDFn:  func(net.Conn) (int32, error) { return 1, nil },
	}

	out := runOneRequest(t, srv, `{"path":""}`+"\n")

	var resp response
	require.NoError(t, json.Unmarshal([]byte(out), &resp))
	assert.False(t, resp.OK)
	assert.Contains(t, resp.Error, "empty path")
	assert.Empty(t, disp.calls)
}

func TestHandle_DispatcherError_PropagatesReason(t *testing.T) {
	disp := &fakeDispatcher{err: errors.New("mirror Failed phase")}
	srv := &Server{
		Dispatcher: disp,
		PeerPIDFn:  func(net.Conn) (int32, error) { return 1, nil },
	}

	out := runOneRequest(t, srv, `{"path":"/run/mxl/domain/f.mxl-flow/flow_def.json"}`+"\n")

	var resp response
	require.NoError(t, json.Unmarshal([]byte(out), &resp))
	assert.False(t, resp.OK)
	assert.Contains(t, resp.Error, "mirror Failed phase",
		"the dispatcher's error string must round-trip to the shim so the "+
			"consumer pod's log carries a non-generic reason for the open() fail")
}

func TestHandle_PeerPIDError_ShortCircuitsBeforeDispatcher(t *testing.T) {
	disp := &fakeDispatcher{}
	srv := &Server{
		Dispatcher: disp,
		PeerPIDFn:  func(net.Conn) (int32, error) { return 0, errors.New("no creds") },
	}

	out := runOneRequest(t, srv, `{"path":"/run/mxl/domain/f.mxl-flow/flow_def.json"}`+"\n")

	var resp response
	require.NoError(t, json.Unmarshal([]byte(out), &resp))
	assert.False(t, resp.OK)
	assert.Contains(t, resp.Error, "peer credentials")
	assert.Empty(t, disp.calls,
		"if the agent cannot identify the caller, the dispatcher must "+
			"not run; the access-control story relies on knowing the pod")
}

func TestServer_RunBindsAndServesOverRealUDS(t *testing.T) {
	tmp := t.TempDir()
	sock := tmp + "/agent.sock"

	disp := &fakeDispatcher{}
	srv := &Server{
		SocketPath: sock,
		Dispatcher: disp,
		PeerPIDFn:  func(net.Conn) (int32, error) { return 9999, nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	// Poll until the socket appears (Run does some setup work).
	deadline := time.Now().Add(2 * time.Second)
	var conn net.Conn
	var err error
	for time.Now().Before(deadline) {
		conn, err = net.Dial("unix", sock)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.NoError(t, err, "could not connect to %s within deadline", sock)
	defer conn.Close()

	_, err = conn.Write([]byte(`{"path":"/run/mxl/domain/f.mxl-flow/flow_def.json"}` + "\n"))
	require.NoError(t, err)

	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	require.NoError(t, err)
	var resp response
	require.NoError(t, json.Unmarshal([]byte(line), &resp))
	assert.True(t, resp.OK)
	assert.Equal(t, int32(9999), disp.lastCall().pid)

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// The wire contract between a shim and an agent of different releases.
// Both ship independently -- the shim is copied into consumer pods and
// outlives agent upgrades -- so each side must accept what the other
// side's older or newer build sends.
//
// A shim newer than this agent may add fields to the request; an agent
// that refused them would turn every open into ENOENT across a rolling
// upgrade. The shim reads only "ok":true from the reply, so the reply
// may grow as well, but must keep that key and value.
func TestHandle_WireContract_AcrossVersions(t *testing.T) {
	flowPath := `/run/mxl/domain/11111111-2222-3333-4444-555555555555.mxl-flow/flow_def.json`
	cases := []struct {
		name string
		req  string
	}{
		{"shim before the event field", `{"path":"` + flowPath + `"}`},
		{"shim with a field this agent does not know", `{"path":"` + flowPath + `","domain_id":"x","v":2}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			disp := &fakeDispatcher{}
			srv := &Server{Dispatcher: disp,
				PeerPIDFn: func(net.Conn) (int32, error) { return 7, nil }}

			out := runOneRequest(t, srv, tc.req+"\n")
			assert.Contains(t, out, `"ok":true`,
				"the shim matches this exact substring; anything else is ENOENT")
			assert.Equal(t, flowPath, disp.lastCall().path)
		})
	}
}

// A refusal must come back as a reply, never as a dropped connection:
// the shim maps both to ENOENT, but only a reply carries the reason
// into the agent log and back to anyone reading the socket.
func TestHandle_Refusal_IsAReplyWithoutOK(t *testing.T) {
	disp := &fakeDispatcher{err: errors.New("in a domain mxl-k8s does not mirror")}
	srv := &Server{Dispatcher: disp,
		PeerPIDFn: func(net.Conn) (int32, error) { return 7, nil }}

	out := runOneRequest(t, srv, `{"path":"/run/mxl/scratch/x.mxl-flow/flow_def.json"}`+"\n")
	assert.NotContains(t, out, `"ok":true`)
	assert.Contains(t, out, "does not mirror")
}
