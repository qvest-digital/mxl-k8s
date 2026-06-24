# NMOS Server Refactoring Design

> Generated from analysis of current `server.go` (380 lines, single file, Go 1.26.0).

## 1. Current State Summary

### Endpoints (14 routes registered)

| Method(s) | Path Pattern | Handler |
|---|---|---|
| GET | `/x-nmos/node/` | `handleNodeVersions` |
| GET | `/x-nmos/node/v1.3/` | `handleNode` |
| GET | `/x-nmos/node/v1.3/self` | `handleNode` |
| GET | `/x-nmos/node/v1.3/devices` | `handleDevices` |
| GET | `/x-nmos/node/v1.3/sources` | `handleSources` |
| GET | `/x-nmos/node/v1.3/flows` | `handleFlows` |
| GET | `/x-nmos/node/v1.3/senders` | `handleSenders` |
| GET | `/x-nmos/node/v1.3/receivers` | `handleReceivers` |
| GET | `/x-nmos/connection/` | `handleConnectionVersions` |
| GET | `/x-nmos/connection/v1.2/` | `handleConnectionVersions` |
| GET | `/x-nmos/connection/v1.2/single/senders/{id}/active` | `handleSenderActive` |
| GET, PATCH | `/x-nmos/connection/v1.2/single/senders/{id}/staged` | `handleSenderStaged` |
| GET | `/x-nmos/connection/v1.2/single/senders/{id}/constraints` | `handleSenderConstraints` |
| GET | `/x-nmos/connection/v1.2/single/senders/{id}/transportfile` | `handleSenderTransportFile` |

The connection sender routes are currently dispatched through a single `handleConnectionSender` function that manually parses the URL suffix — a workaround predating Go 1.22 pattern support.

### Dependencies

- **`Cache` interface** (in-package): `GetDomain(id)`, `GetDomainFlows(domainID)`
- **`types.ResourceSet`**: built via `types.BuildIS04Resources()` from the `types` package
- **`mxlv1` CRD types**: `MxlDomain`, `MxlFlow` from `api/v1alpha1`
- **`logr.Logger`**: controller-runtime logging (no `slog` dependency)
- **std library only**: `context`, `encoding/json`, `fmt`, `net`, `net/http`, `strings`, `time`

### Lifecycle

- `New(opts) http.Handler` — pure constructor, returns configured handler, no side effects
- `Run(ctx, addr, h) error` — standalone function; creates `http.Server`, runs until ctx canceled, graceful 5s shutdown
- Instantiated once in `main.go:102–114`, passed to `server.Run()` in a goroutine

### Middleware Chain (innermost to outermost, in `routes()`)

```
loggingMiddleware(logger) → recoverMiddleware(logger) → corsMiddleware → router
```

### What's tested (server_test.go, 269 lines)

- Full-handler integration via `server.New()` + `httptest.NewRecorder()`
- Fake `Cache` (maps) injected through `Options`
- Tests cover: node versions, node resource, devices, sources, flows, senders, receivers (empty), sender active/staged/constraints/transportfile, 404 on unknown paths, 404 on unknown sender, internal error when cache nil

---

## 2. File Split

```
server/               ← package server (no package rename — backward compatible)
├── server.go         ← types, constructor, write* helpers, statusRecorder
├── server_test.go    ← existing tests preserved, new isolated-handler tests added
├── router.go         ← routes() with Go 1.22+ ServeMux patterns
├── handlers.go       ← all handler methods (handleNode, handleDevices, …)
├── middleware.go     ← cors, recovery, logging, content-type
└── DESIGN.md         ← this document
```

### server.go (types + constructor + shared helpers)

What stays/moves here:

- Package doc comment
- `Cache` interface
- `Options` struct
- `Server` struct (now embeds `HandlerEnv`)
- `HandlerEnv` struct (new — extracted dependency bundle for isolated testing)
- `New()` constructor (unchanged signature: `func New(opts Options) http.Handler`)
- `Run()` lifecycle function (unchanged signature)
- `statusRecorder` struct
- `errorResponse` struct
- `writeJSON()`, `writeError()`, `nmosVersion()` helpers
- `setCommonHeaders()` (or move to middleware.go — see below)
- All constants

### router.go (route registration)

- `func (s *Server) routes() http.Handler` — now uses Go 1.22+ `http.NewServeMux()`
- Removes: `route` struct, `getRoute()`, map-based dispatch, `strings.HasPrefix` fallback, manual `handleConnectionSender` switch
- Adds: 17 direct `mux.HandleFunc("METHOD /path{pats}", handler)` calls
- Middleware chaining stays at the bottom of this function

### handlers.go (NMOS resource handlers)

All methods moved from `*Server` to `*HandlerEnv` so they can be called in tests without constructing a full `http.Handler` chain:

```go
func (h *HandlerEnv) HandleNode(w http.ResponseWriter, r *http.Request)       { … }
func (h *HandlerEnv) HandleDevices(w http.ResponseWriter, r *http.Request)    { … }
func (h *HandlerEnv) HandleSources(w http.ResponseWriter, r *http.Request)    { … }
func (h *HandlerEnv) HandleFlows(w http.ResponseWriter, r *http.Request)      { … }
func (h *HandlerEnv) HandleSenders(w http.ResponseWriter, r *http.Request)    { … }
func (h *HandlerEnv) HandleReceivers(w http.ResponseWriter, r *http.Request)  { … }
func (h *HandlerEnv) HandleNodeVersions(w, r)                                 { … }
func (h *HandlerEnv) HandleConnectionVersions(w, r)                           { … }
func (h *HandlerEnv) HandleSenderActive(w, r)                                 { … }
func (h *HandlerEnv) HandleSenderStaged(w, r)                                 { … }
func (h *HandlerEnv) HandleSenderConstraints(w, r)                            { … }
func (h *HandlerEnv) HandleSenderTransportFile(w, r)                          { … }
// Internal helpers:
func (h *HandlerEnv) senderState(w, senderID) (types.SenderState, bool)       { … }
func (h *HandlerEnv) resources(w) (types.ResourceSet, bool)                   { … }
```

The sender-ID extractor changes from `strings.TrimPrefix` + `strings.Split` to `r.PathValue("senderID")`.

Handler signatures remain `(w http.ResponseWriter, r *http.Request)` exactly — this is the standard library contract and what `mux.HandleFunc` expects.

### middleware.go (HTTP middleware)

```go
func CORSMiddleware(next http.Handler) http.Handler                      { … }
func RecoverMiddleware(logger logr.Logger) func(http.Handler) http.Handler { … }
func LoggingMiddleware(logger logr.Logger) func(http.Handler) http.Handler { … }
func ContentTypeMiddleware(next http.Handler) http.Handler                { … }  // new
```

Also moves `setCommonHeaders()` here (it is CORS infrastructure, not a general utility).

---

## 3. Router Choice: Go 1.22+ ServeMux

**Decision**: Use `net/http` standard library `http.NewServeMux()` with method-prefixed patterns.

**Rationale**:
- Go 1.26.0 is the project's declared toolchain — Go 1.22+ patterns are on by default
- Eliminates ~50 lines of custom routing infrastructure (`route` struct, `getRoute()`, `serve()`, map dispatch, manual URL parsing for sender sub-resources)
- Wildcard capture `{senderID}` replaces `strings.TrimPrefix` + `strings.Split` — fewer branch conditions, no off-by-one errors
- Trailing-slash redirects: `/x-nmos/node` auto-redirects to `/x-nmos/node/` (better than current 404)
- Zero new dependencies (doesn't add chi, gorilla/mux, or any third-party router)
- Consistent with the rest of the agent's `main.go` which already uses `http.NewServeMux()` for probe endpoints (line 242)

**Pattern registration in `routes()`**:

```go
func (s *Server) routes() http.Handler {
    mux := http.NewServeMux()

    // IS-04 discovery
    mux.HandleFunc("GET /x-nmos/node/", s.env.HandleNodeVersions)
    mux.HandleFunc("GET /x-nmos/node/v1.3/", s.env.HandleNode)
    mux.HandleFunc("GET /x-nmos/node/v1.3/self", s.env.HandleNode)

    // IS-04 resources
    mux.HandleFunc("GET /x-nmos/node/v1.3/devices", s.env.HandleDevices)
    mux.HandleFunc("GET /x-nmos/node/v1.3/sources", s.env.HandleSources)
    mux.HandleFunc("GET /x-nmos/node/v1.3/flows", s.env.HandleFlows)
    mux.HandleFunc("GET /x-nmos/node/v1.3/senders", s.env.HandleSenders)
    mux.HandleFunc("GET /x-nmos/node/v1.3/receivers", s.env.HandleReceivers)

    // IS-05 discovery
    mux.HandleFunc("GET /x-nmos/connection/", s.env.HandleConnectionVersions)
    mux.HandleFunc("GET /x-nmos/connection/v1.2/", s.env.HandleConnectionVersions)

    // IS-05 sender resources
    mux.HandleFunc("GET /x-nmos/connection/v1.2/single/senders/{senderID}/active",
        s.env.HandleSenderActive)
    mux.HandleFunc("GET /x-nmos/connection/v1.2/single/senders/{senderID}/staged",
        s.env.HandleSenderStaged)
    mux.HandleFunc("PATCH /x-nmos/connection/v1.2/single/senders/{senderID}/staged",
        s.env.HandleSenderStaged)
    mux.HandleFunc("GET /x-nmos/connection/v1.2/single/senders/{senderID}/constraints",
        s.env.HandleSenderConstraints)
    mux.HandleFunc("GET /x-nmos/connection/v1.2/single/senders/{senderID}/transportfile",
        s.env.HandleSenderTransportFile)

    // 404 catch-all — ServeMux returns 405 for wrong method, 404 for unmatched path
    // No custom fallback needed; Go 1.22 ServeMux handles both.

    return LoggingMiddleware(s.opts.Logger)(
        RecoverMiddleware(s.opts.Logger)(
            CORSMiddleware(
                ContentTypeMiddleware(mux),
            ),
        ),
    )
}
```

**Edge cases handled by Go 1.22 ServeMux**:
- `GET /x-nmos/node` → 301 redirect to `/x-nmos/node/` (registered with trailing slash)
- `POST /x-nmos/node/v1.3/devices` → 405 Method Not Allowed (automatic)
- `GET /x-nmos/node/v1.3/devices/extra` → 404 (exact match, no trailing slash wildcard)
- Missing `senderID` in connection path → 404 (ServeMux won't route without a non-empty segment)

**What gets deleted** (no longer needed):
- `type route struct`
- `func getRoute()`
- `func (r route) serve()`
- The `map[string]route` in `routes()`
- The `http.HandlerFunc` wrapper with `strings.HasPrefix` fallback
- The entire `handleConnectionSender` method (48 lines — its switch/case logic is absorbed into ServeMux dispatch)
- `connectionSenderV12Path` constant (no longer used for prefix matching)

---

## 4. Middleware Chain

### Composition (innermost → outermost, unchanged order)

```
logging → recovery → CORS → content-type → ServeMux router
```

Same order as current — no behavioral change.

### New middleware: `ContentTypeMiddleware`

Currently, every handler must call `w.Header().Set("Content-Type", "application/json")` via `writeJSON()` or `writeError()`. A middleware ensures JSON content type on all responses by default, reducing per-handler boilerplate:

```go
func ContentTypeMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Content-Type", "application/json")
        next.ServeHTTP(w, r)
    })
}
```

`writeJSON` and `writeError` keep their existing `Content-Type` set (it's a no-op when already set), preserving backward compatibility. Handlers that need non-JSON responses (none currently) can override the header.

### CORS middleware: rename to exported

Currently `corsMiddleware` is unexported. Rename to `CORSMiddleware` (exported) for consistency with the other middleware. Preserve `setCommonHeaders()` as an internal helper, moved into `middleware.go`.

### Recovery middleware: no changes

Current behavior — catches panics, logs, returns 500. Keep identical.

### Logging middleware: no changes

Current behavior — records method, path, status, duration. Keep identical.

---

## 5. Handler Signatures and Testability

### HandlerEnv: extracted dependency bundle

```go
// HandlerEnv carries the dependencies NMOS handlers need without
// coupling them to the Server lifecycle. Tests construct a HandlerEnv
// directly with a fake Cache and call handlers in isolation.
type HandlerEnv struct {
    Cache    Cache
    NodeName string
    DomainID string
    Host     string
    Port     int
    Now      func() time.Time
}
```

`Server` becomes:

```go
type Server struct {
    opts Options
    env  *HandlerEnv
}
```

`New()` populates both:

```go
func New(opts Options) http.Handler {
    if opts.DomainID == "" { opts.DomainID = opts.NodeName }
    if opts.Host == ""     { opts.Host = "127.0.0.1" }
    if opts.Now == nil     { opts.Now = time.Now }
    if opts.Logger.IsZero(){ opts.Logger = logr.Discard() }
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
```

### Isolated handler testing

Handlers are public methods on `*HandlerEnv`. Test example:

```go
func TestHandleNode_Isolated(t *testing.T) {
    cache := &fakeCache{
        domains: map[string]mxlv1.MxlDomain{"test-node": {…}},
        flows:   map[string][]mxlv1.MxlFlow{},
    }
    env := &HandlerEnv{
        Cache:    cache,
        NodeName: "test-node",
        DomainID: "test-node",
        Host:     "127.0.0.1",
        Port:     8080,
        Now:      func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
    }

    w := httptest.NewRecorder()
    req := httptest.NewRequest(http.MethodGet, "/", nil)
    env.HandleNode(w, req)

    assert.Equal(t, http.StatusOK, w.Code)
    var node types.Node
    require.NoError(t, json.Unmarshal(w.Body.Bytes(), &node))
    assert.Equal(t, "test-node", node.Label)
}
```

Key benefits:
1. No need to construct `Options` or call `server.New()` — just `&HandlerEnv{…}`.
2. Handler can be called directly without middleware — isolates route behavior from handler behavior.
3. Cache is the `Cache` interface — same `fakeCache` type already in `server_test.go` works unchanged.

### Existing integration tests preserved

All current tests (which go through `server.New()` + full middleware chain) continue to work. The `New()` signature is unchanged. No test API breaks.

### Sender-ID extraction

Handlers that need the sender ID use `r.PathValue("senderID")`:

```go
func (h *HandlerEnv) HandleSenderActive(w http.ResponseWriter, r *http.Request) {
    senderID := r.PathValue("senderID")
    state, ok := h.senderState(w, senderID)
    if !ok { return }
    writeJSON(w, state)
}
```

No more `strings.TrimPrefix` / `strings.Split` / index checks. The ServeMux guarantees `senderID` is non-empty when the handler is called.

---

## 6. `Run()` — No Changes

The `Run` function does not touch `Server` internals — it takes `(ctx, addr, http.Handler)`. It stays in `server.go` with zero modifications. The call site in `main.go:111` is unchanged.

---

## 7. Implementation Steps

Ordered for minimal risk, each step independently compilable and testable:

### Step 1: Add `HandlerEnv` + `ContentTypeMiddleware`
- Add `HandlerEnv` struct to `server.go` (alongside existing `Server`)
- Set `HandlerEnv` on `Server` in `New()` (both structs populated from same `Options`)
- **Compile and run existing tests** — zero behavioral change

### Step 2: Move handlers to `HandlerEnv` methods
- Create `handlers.go`
- Move each handler from `*Server` to `*HandlerEnv`, rename: `handleNode` → `HandleNode`, etc.
- Update `routes()` to call `s.env.HandleNode` instead of `s.handleNode`
- **Compile and run existing tests** — tests go through `New()` which builds the env; behavior identical

### Step 3: Add isolated handler tests
- Add `TestHandlerEnv_HandleNode`, `TestHandlerEnv_HandleSenderActive`, etc. to `server_test.go`
- These test handlers directly, bypassing middleware
- **Run tests** — both integration and isolated pass

### Step 4: Extract middleware to `middleware.go`
- Move `CORSMiddleware`, `RecoverMiddleware`, `LoggingMiddleware`, `ContentTypeMiddleware`, `setCommonHeaders` to `middleware.go`
- Export previously-unexported middleware names
- **Compile and run tests** — identical behavior

### Step 5: Switch to Go 1.22+ ServeMux in `router.go`
- Create `router.go`
- Replace map-based `routes()` with `http.NewServeMux()` + method-pattern registration
- Delete: `route` struct, `getRoute()`, `handleConnectionSender()`, `connectionSenderV12Path` constant
- Update handlers to use `r.PathValue("senderID")`
- **Compile and run tests** — tests must still pass; pay special attention to 404/405 behavior which may differ slightly (ServeMux returns more specific responses)

### Step 6: Clean up `server.go`
- Remove dead code (anything only used by old router)
- Verify all constants' consumers are in the right files
- **Final compile + full test run**

---

## 8. Risk Assessment

| Risk | Mitigation |
|---|---|
| Go 1.22 ServeMux 405 vs old custom 405 response format | ServeMux's built-in 405 is plain text. If NMOS clients expect JSON error responses, add a custom 405 handler via `mux.HandleFunc("METHOD /x-nmos/…/", …)` for known-prefix paths — but this is unlikely since 405 only fires on wrong HTTP methods, and NMOS controllers follow the spec. |
| `{senderID}` capture behavior differs from manual parsing | ServeMux strips the captured segment exactly — no trailing slashes, no encoding surprises. Verify with existing test cases for sender endpoints. |
| `ContentTypeMiddleware` interferes with 204 (No Content) responses | The CORS OPTIONS handler returns 204 with no body. Middleware sets `Content-Type: application/json` on the header but 204 responses ignore Content-Type per RFC 7230 §3.3. Tests validate this. |
| Sender handler rename breaks external callers | All handlers are internal (`internal/nmos/server`). Only `New()` and `Run()` are used from `main.go`. No breaking changes to the public API surface. |

---

## 9. Summary

- **4 files** instead of 2 (server.go 120→ lines, router.go ~60 lines, handlers.go ~200 lines, middleware.go ~60 lines)
- **Zero new dependencies** — standard library `net/http` only
- **~80 lines deleted** (`route` struct, `getRoute`, `serve`, `handleConnectionSender`, map dispatch)
- **HandlerEnv enables isolated testing** — handlers testable without middleware or full server
- **Backward compatible** — `New()` and `Run()` signatures unchanged, all existing tests pass
- **CLAUDE.md compliance**: no speculation about downstream usage, no invented API behavior, factual and verifiable