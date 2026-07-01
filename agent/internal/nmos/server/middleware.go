package server

import (
	"bytes"
	"fmt"
	"net/http"
	"time"

	"github.com/go-logr/logr"
)

func setCommonHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PATCH, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Accept")
}

// CORSMiddleware adds NMOS CORS headers and handles preflight requests.
func CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setCommonHeaders(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ContentTypeMiddleware defaults all downstream responses to JSON.
func ContentTypeMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}

// RecoverMiddleware logs panics and converts them to JSON 500 responses.
func RecoverMiddleware(logger logr.Logger) func(http.Handler) http.Handler {
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

// LoggingMiddleware logs request method, path, status, and duration.
func LoggingMiddleware(logger logr.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			start := time.Now()
			next.ServeHTTP(recorder, r)
			logger.Info("NMOS request", "method", r.Method, "path", r.URL.Path, "status", recorder.status, "duration", time.Since(start))
		})
	}
}

type bufferedResponseWriter struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func newBufferedResponseWriter() *bufferedResponseWriter {
	return &bufferedResponseWriter{header: make(http.Header), status: http.StatusOK}
}

func (w *bufferedResponseWriter) Header() http.Header {
	return w.header
}

func (w *bufferedResponseWriter) WriteHeader(status int) {
	w.status = status
}

func (w *bufferedResponseWriter) Write(b []byte) (int, error) {
	return w.body.Write(b)
}

func (w *bufferedResponseWriter) flushTo(dst http.ResponseWriter) {
	for key, values := range w.header {
		dst.Header().Del(key)
		for _, value := range values {
			dst.Header().Add(key, value)
		}
	}
	dst.WriteHeader(w.status)
	_, _ = dst.Write(w.body.Bytes())
}

func PreserveMuxErrorMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buffer := newBufferedResponseWriter()
		next.ServeHTTP(buffer, r)
		switch buffer.status {
		case http.StatusNotFound:
			writeError(w, http.StatusNotFound, "not found", "resource not found")
		case http.StatusMethodNotAllowed:
			writeError(w, http.StatusMethodNotAllowed, "method not allowed", fmt.Sprintf("%s is not supported", r.Method))
		default:
			buffer.flushTo(w)
		}
	})
}
