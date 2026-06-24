// Package server serves NMOS IS-04 Node API resources for MXL senders.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/go-logr/logr"
	"github.com/qvest-digital/mxl-k8s/agent/internal/nmos/types"
	mxlv1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

const (
	nodeBasePath = "/x-nmos/node/"
	nodeV13Path  = "/x-nmos/node/v1.3/"
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
	Logger   logr.Logger
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
	s := &Server{opts: opts}
	return s.routes()
}

// Run serves the NMOS API until ctx is canceled.
func Run(ctx context.Context, addr string, h http.Handler) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Server holds route handlers for the IS-04 Node API.
type Server struct {
	opts Options
}

func (s *Server) routes() http.Handler {
	routes := map[string]http.Handler{
		nodeBasePath:              http.HandlerFunc(s.handleVersions),
		nodeV13Path:               http.HandlerFunc(s.handleNode),
		nodeV13Path + "self":      http.HandlerFunc(s.handleNode),
		nodeV13Path + "devices":   http.HandlerFunc(s.handleDevices),
		nodeV13Path + "sources":   http.HandlerFunc(s.handleSources),
		nodeV13Path + "flows":     http.HandlerFunc(s.handleFlows),
		nodeV13Path + "senders":   http.HandlerFunc(s.handleSenders),
		nodeV13Path + "receivers": http.HandlerFunc(s.handleReceivers),
	}
	router := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h, ok := routes[r.URL.Path]
		if !ok {
			writeError(w, http.StatusNotFound, "not found", "resource not found")
			return
		}
		h.ServeHTTP(w, r)
	})
	return loggingMiddleware(s.opts.Logger)(recoverMiddleware(s.opts.Logger)(corsMiddleware(methodMiddleware(router))))
}

func (s *Server) handleVersions(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, []string{"v1.3/"})
}

func (s *Server) handleNode(w http.ResponseWriter, _ *http.Request) {
	resources, ok := s.resources(w)
	if !ok {
		return
	}
	writeJSON(w, resources.Node)
}

func (s *Server) handleDevices(w http.ResponseWriter, _ *http.Request) {
	resources, ok := s.resources(w)
	if !ok {
		return
	}
	writeJSON(w, resources.Devices)
}

func (s *Server) handleSources(w http.ResponseWriter, _ *http.Request) {
	resources, ok := s.resources(w)
	if !ok {
		return
	}
	writeJSON(w, resources.Sources)
}

func (s *Server) handleFlows(w http.ResponseWriter, _ *http.Request) {
	resources, ok := s.resources(w)
	if !ok {
		return
	}
	writeJSON(w, resources.Flows)
}

func (s *Server) handleSenders(w http.ResponseWriter, _ *http.Request) {
	resources, ok := s.resources(w)
	if !ok {
		return
	}
	writeJSON(w, resources.Senders)
}

func (s *Server) handleReceivers(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, []any{})
}

func (s *Server) resources(w http.ResponseWriter) (types.ResourceSet, bool) {
	if s.opts.Cache == nil {
		writeError(w, http.StatusInternalServerError, "internal error", "NMOS cache is not configured")
		return types.ResourceSet{}, false
	}
	domain, ok := s.opts.Cache.GetDomain(s.opts.DomainID)
	if !ok {
		writeError(w, http.StatusInternalServerError, "internal error", fmt.Sprintf("MxlDomain %s is not cached", s.opts.DomainID))
		return types.ResourceSet{}, false
	}
	resources, err := types.BuildIS04Resources(s.opts.NodeName, domain, s.opts.Cache.GetDomainFlows(s.opts.DomainID), nmosVersion(s.opts.Now()))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error", err.Error())
		return types.ResourceSet{}, false
	}
	resources.Node.Href = fmt.Sprintf("http://%s/x-nmos/node/v1.3/", net.JoinHostPort(s.opts.Host, fmt.Sprint(s.opts.Port)))
	resources.Node.API.Endpoints = []types.Endpoint{{Host: s.opts.Host, Port: s.opts.Port, Protocol: "http"}}
	return resources, true
}

type errorResponse struct {
	Code  int    `json:"code"`
	Error string `json:"error"`
	Debug string `json:"debug"`
}

func setCommonHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Accept")
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setCommonHeaders(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func methodMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed", fmt.Sprintf("%s is not supported", r.Method))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func recoverMiddleware(logger logr.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if recovered := recover(); recovered != nil {
					logger.Error(fmt.Errorf("panic: %v", recovered), "NMOS request panic", "method", r.Method, "path", r.URL.Path)
					writeError(w, http.StatusInternalServerError, "internal error", "unexpected server error")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

func loggingMiddleware(logger logr.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			start := time.Now()
			next.ServeHTTP(recorder, r)
			logger.Info("NMOS request", "method", r.Method, "path", r.URL.Path, "status", recorder.status, "duration", time.Since(start))
		})
	}
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

func nmosVersion(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000000Z")
}
