// Package server serves NMOS IS-04 Node API resources for MXL senders.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	mxlv1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

const (
	nodeBasePath            = "/x-nmos/node/"
	nodeV13Path             = "/x-nmos/node/v1.3/"
	connectionBasePath      = "/x-nmos/connection/"
	connectionV11Path       = "/x-nmos/connection/v1.1/"
	connectionSenderV11Path = "/x-nmos/connection/v1.1/single/senders/"
	activationModeImmediate = "activate_immediate"
	readHeaderTimeout       = 5 * time.Second
	shutdownTimeout         = 5 * time.Second
)

// Cache is the watcher read interface the NMOS server needs.
type Cache interface {
	GetDomain(id string) (mxlv1.MxlDomain, bool)
	GetDomainFlows(domainID string) []mxlv1.MxlFlow
}

// Options configures the IS-04 Node API server handler.
type Options struct {
	NodeName string
	DomainID string
	Host     string
	Port     int
	Cache    Cache
	Now      func() time.Time
	// Logger is controller-runtime's logging facade. In the agent it is
	// backed by the zap logger configured in main.go; keeping the server on
	// logr avoids creating a second zap configuration inside this package.
	Logger logr.Logger
}

// New returns an HTTP handler for the NMOS IS-04 Node API v1.3.
func New(opts Options) http.Handler {
	if opts.DomainID == "" {
		opts.DomainID = opts.NodeName
	}
	if opts.Host == "" {
		opts.Host = "127.0.0.1"
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger.IsZero() {
		opts.Logger = logr.Discard()
	}
	s := &Server{
		opts: opts,
		env: &HandlerEnv{
			Cache:    opts.Cache,
			NodeName: opts.NodeName,
			DomainID: opts.DomainID,
			Host:     opts.Host,
			Port:     opts.Port,
			Now:      opts.Now,
		},
	}
	return s.routes()
}

// NewHTTPServer creates the configured HTTP server for an already-built router.
func NewHTTPServer(addr string, router http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           router,
		ReadHeaderTimeout: readHeaderTimeout,
	}
}

// Run serves the NMOS API until ctx is canceled or a shutdown signal is received.
func Run(ctx context.Context, addr string, h http.Handler) error {
	runCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return Serve(runCtx, NewHTTPServer(addr, h), listener)
}

// Serve starts srv on listener and gracefully shuts it down when ctx is done.
func Serve(ctx context.Context, srv *http.Server, listener net.Listener) error {
	errc := make(chan error, 1)
	go func() {
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
			return
		}
		errc <- nil
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	return <-errc
}

// Server holds route handlers for the IS-04 Node API.
type Server struct {
	opts Options
	env  *HandlerEnv
}

// HandlerEnv carries the dependencies NMOS handlers need without coupling them
// to the HTTP server lifecycle.
type HandlerEnv struct {
	Cache    Cache
	NodeName string
	DomainID string
	Host     string
	Port     int
	Now      func() time.Time
}

type errorResponse struct {
	Code  int    `json:"code"`
	Error string `json:"error"`
	Debug string `json:"debug"`
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string, debug string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{Code: status, Error: msg, Debug: debug})
}

func methodNotAllowed(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusMethodNotAllowed, "method not allowed", fmt.Sprintf("%s is not supported", r.Method))
}

// leapSeconds is the current TAI-UTC offset. As of January 2017 the offset
// is 37 seconds; it will remain valid until the next announced leap second.
// IS-04 resource versions must be TAI timestamps (UTC + leap seconds).
const leapSeconds = 37

// nmosVersion returns a TAI timestamp string suitable for IS-04 resource
// version fields. TAI = UTC + leapSeconds.
func nmosVersion(t time.Time) string {
	tai := t.UTC().Add(leapSeconds * time.Second)
	return tai.Format("2006-01-02T15:04:05.000000000Z")
}
