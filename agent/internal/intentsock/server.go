// Package intentsock hosts the agent's Unix-domain socket endpoint
// that consumer pods reach (via the libmxl-intent.so LD_PRELOAD
// shim) to ask for on-demand flow materialization.
//
// The wire protocol is one line-delimited JSON request per
// connection:
//
//	{"path":"/run/mxl/domain/<uuid>.mxl-flow/flow_def.json"}\n
//
// followed by one line-delimited JSON response from the agent:
//
//	{"ok":true}\n
//
// or
//
//	{"ok":false,"error":"<reason>"}\n
//
// The peer's PID is taken from SO_PEERCRED on the UDS, so callers
// don't need to (and can't trustworthily) declare their own PID.
package intentsock

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	"golang.org/x/sys/unix"
)

// MaterializeDispatcher is the contract the Server expects from the
// intent dispatcher: given a peer PID and a flow_def.json path, drive
// the mirror to Ready (or surface a terminal error).
//
// The narrow interface keeps the server testable without pulling in
// the full intent package; *intent.Dispatcher satisfies it
// implicitly.
type MaterializeDispatcher interface {
	Materialize(ctx context.Context, pid int32, path string) error
}

// Server listens on a UDS and dispatches requests to the intent
// dispatcher.
type Server struct {
	SocketPath string
	Dispatcher MaterializeDispatcher

	// PeerPIDFn overrides the SO_PEERCRED-based PID extraction.
	// Tests inject a closure returning a known PID over net.Pipe;
	// production leaves this nil and the unix(7) implementation
	// kicks in.
	PeerPIDFn func(net.Conn) (int32, error)
}

type request struct {
	Path string `json:"path"`
}

type response struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// Run binds the socket, sets it world-accessible (so any consumer
// pod with the bind-mount can connect), and serves until ctx is
// canceled.
func (s *Server) Run(ctx context.Context) error {
	l := ctrl.Log.WithName("intentsock")

	if err := os.MkdirAll(filepath.Dir(s.SocketPath), 0o755); err != nil {
		return fmt.Errorf("mkdir for socket: %w", err)
	}
	// Remove any stale socket from a previous run.
	_ = os.Remove(s.SocketPath)

	listener, err := net.Listen("unix", s.SocketPath)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.SocketPath, err)
	}
	defer listener.Close()
	defer os.Remove(s.SocketPath)

	// World-rw so consumer pods that mount the socket can connect.
	// Access control happens via SO_PEERCRED + the pod-lookup
	// step, not via filesystem perms.
	if err := os.Chmod(s.SocketPath, 0o666); err != nil {
		return fmt.Errorf("chmod %s: %w", s.SocketPath, err)
	}

	l.Info("intent socket listening", "path", s.SocketPath)

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			l.Error(err, "accept")
			continue
		}
		go s.handle(ctx, conn)
	}
}

func (s *Server) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	l := ctrl.Log.WithName("intentsock")

	// Connection-level deadline guards against a misbehaving shim
	// that connects and never sends. The Materialize call itself
	// runs against the agent-level timeout in the Dispatcher.
	if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		l.Error(err, "set deadline")
		return
	}

	pidFn := s.PeerPIDFn
	if pidFn == nil {
		pidFn = peerPID
	}
	pid, err := pidFn(conn)
	if err != nil {
		writeResponse(conn, response{Error: fmt.Sprintf("peer credentials: %s", err)})
		return
	}

	var req request
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&req); err != nil {
		writeResponse(conn, response{Error: fmt.Sprintf("decode request: %s", err)})
		return
	}
	if req.Path == "" {
		writeResponse(conn, response{Error: "empty path"})
		return
	}

	if err := s.Dispatcher.Materialize(ctx, pid, req.Path); err != nil {
		l.V(1).Info("materialize failed", "pid", pid, "path", req.Path, "err", err.Error())
		writeResponse(conn, response{Error: err.Error()})
		return
	}
	writeResponse(conn, response{OK: true})
}

func writeResponse(w net.Conn, r response) {
	b, err := json.Marshal(r)
	if err != nil {
		return
	}
	b = append(b, '\n')
	_, _ = w.Write(b)
}

// peerPID returns the host PID of the connected client via
// SO_PEERCRED on the underlying UnixConn.
func peerPID(conn net.Conn) (int32, error) {
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		return 0, errors.New("not a UnixConn")
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return 0, err
	}
	var cred *unix.Ucred
	var inner error
	if ctrlErr := raw.Control(func(fd uintptr) {
		cred, inner = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); ctrlErr != nil {
		return 0, ctrlErr
	}
	if inner != nil {
		return 0, inner
	}
	if cred == nil {
		return 0, errors.New("no peer credentials")
	}
	return cred.Pid, nil
}
