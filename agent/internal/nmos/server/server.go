// Package server serves NMOS IS-04 Node API resources for MXL senders.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

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
	return &handler{opts: opts}
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

type handler struct {
	opts Options
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	setCommonHeaders(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed", fmt.Sprintf("%s is not supported", r.Method))
		return
	}

	switch r.URL.Path {
	case nodeBasePath:
		writeJSON(w, []string{"v1.3/"})
	case nodeV13Path, nodeV13Path + "self":
		resources, ok := h.resources(w)
		if !ok {
			return
		}
		writeJSON(w, resources.Node)
	case nodeV13Path + "devices":
		resources, ok := h.resources(w)
		if !ok {
			return
		}
		writeJSON(w, resources.Devices)
	case nodeV13Path + "sources":
		resources, ok := h.resources(w)
		if !ok {
			return
		}
		writeJSON(w, resources.Sources)
	case nodeV13Path + "flows":
		resources, ok := h.resources(w)
		if !ok {
			return
		}
		writeJSON(w, resources.Flows)
	case nodeV13Path + "senders":
		resources, ok := h.resources(w)
		if !ok {
			return
		}
		writeJSON(w, resources.Senders)
	case nodeV13Path + "receivers":
		writeJSON(w, []any{})
	default:
		writeError(w, http.StatusNotFound, "not found", "resource not found")
	}
}

func (h *handler) resources(w http.ResponseWriter) (types.ResourceSet, bool) {
	if h.opts.Cache == nil {
		writeError(w, http.StatusInternalServerError, "internal error", "NMOS cache is not configured")
		return types.ResourceSet{}, false
	}
	domain, ok := h.opts.Cache.GetDomain(h.opts.DomainID)
	if !ok {
		writeError(w, http.StatusInternalServerError, "internal error", fmt.Sprintf("MxlDomain %s is not cached", h.opts.DomainID))
		return types.ResourceSet{}, false
	}
	resources, err := types.BuildIS04Resources(h.opts.NodeName, domain, h.opts.Cache.GetDomainFlows(h.opts.DomainID), nmosVersion(h.opts.Now()))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error", err.Error())
		return types.ResourceSet{}, false
	}
	resources.Node.Href = fmt.Sprintf("http://%s/x-nmos/node/v1.3/", net.JoinHostPort(h.opts.Host, fmt.Sprint(h.opts.Port)))
	resources.Node.API.Endpoints = []types.Endpoint{{Host: h.opts.Host, Port: h.opts.Port, Protocol: "http"}}
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
