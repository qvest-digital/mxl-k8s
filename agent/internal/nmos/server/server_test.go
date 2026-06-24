package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr/funcr"
	"github.com/qvest-digital/mxl-k8s/agent/internal/nmos/types"
	mxlv1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func TestNodeAPIEndpointsServeIS04Resources(t *testing.T) {
	flow := mxlFlow("flow-a", "5fbec3b1-1b0f-417d-9059-8b94a47197ed", `{
		"id":"5fbec3b1-1b0f-417d-9059-8b94a47197ed",
		"description":"MXL Test Flow",
		"tags":{},
		"format":"urn:x-nmos:format:video",
		"label":"MXL Test Flow",
		"parents":[],
		"media_type":"video/v210"
	}`)
	flow.Status.Locations = []mxlv1.MxlFlowLocation{{NodeName: "node-a", Phase: mxlv1.MxlFlowLocationOrigin}}
	cache := &staticCache{
		domain: mxlv1.MxlDomain{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Spec: mxlv1.MxlDomainSpec{NodeName: "node-a", HostPath: "/run/mxl/domain"}},
		flows:  []mxlv1.MxlFlow{flow},
	}
	h := New(Options{NodeName: "node-a", DomainID: "node-a", Host: "127.0.0.1", Port: 1080, Cache: cache})

	cases := []struct {
		path    string
		into    any
		wantLen int
	}{
		{path: "/x-nmos/node/v1.3/devices", into: &[]types.Device{}, wantLen: 1},
		{path: "/x-nmos/node/v1.3/sources", into: &[]types.Source{}, wantLen: 1},
		{path: "/x-nmos/node/v1.3/flows", into: &[]types.Flow{}, wantLen: 1},
		{path: "/x-nmos/node/v1.3/senders", into: &[]types.Sender{}, wantLen: 1},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, tc.path, nil))
			require.Equal(t, http.StatusOK, rr.Code)
			require.Equal(t, "application/json", rr.Header().Get("Content-Type"))
			require.Equal(t, "*", rr.Header().Get("Access-Control-Allow-Origin"))
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), tc.into))
			b, err := json.Marshal(tc.into)
			require.NoError(t, err)
			var rows []json.RawMessage
			require.NoError(t, json.Unmarshal(b, &rows))
			require.Len(t, rows, tc.wantLen)
		})
	}

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/x-nmos/node/v1.3/", nil))
	require.Equal(t, http.StatusOK, rr.Code)
	var node types.Node
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &node))
	require.Equal(t, "node-a", node.Hostname)
	require.Equal(t, "http://127.0.0.1:1080/x-nmos/node/v1.3/", node.Href)
	require.Equal(t, []string{"v1.3"}, node.API.Versions)
	require.Equal(t, []types.Endpoint{{Host: "127.0.0.1", Port: 1080, Protocol: "http"}}, node.API.Endpoints)
}

func TestConnectionAPISenderEndpointsExposeReadOnlyTransportParams(t *testing.T) {
	domainID := "11111111-1111-4111-8111-111111111111"
	flowID := "5fbec3b1-1b0f-417d-9059-8b94a47197ed"
	flow := mxlFlow("flow-a", flowID, `{
		"id":"5fbec3b1-1b0f-417d-9059-8b94a47197ed",
		"description":"MXL Test Flow",
		"tags":{},
		"format":"urn:x-nmos:format:video",
		"label":"MXL Test Flow",
		"parents":[],
		"media_type":"video/v210"
	}`)
	flow.Status.Locations = []mxlv1.MxlFlowLocation{{NodeName: "node-a", Phase: mxlv1.MxlFlowLocationOrigin}}
	cache := &staticCache{
		domain: mxlv1.MxlDomain{ObjectMeta: metav1.ObjectMeta{Name: domainID}, Spec: mxlv1.MxlDomainSpec{NodeName: "node-a", HostPath: "/run/mxl/domain"}},
		flows:  []mxlv1.MxlFlow{flow},
	}
	h := New(Options{NodeName: "node-a", DomainID: domainID, Host: "127.0.0.1", Port: 1080, Cache: cache})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/x-nmos/node/v1.3/senders", nil))
	require.Equal(t, http.StatusOK, rr.Code)
	var senders []types.Sender
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &senders))
	require.Len(t, senders, 1)
	senderID := senders[0].ID

	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/x-nmos/connection/", nil))
	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `["v1.2/"]`, rr.Body.String())

	for _, path := range []string{
		"/x-nmos/connection/v1.2/single/senders/" + senderID + "/active",
		"/x-nmos/connection/v1.2/single/senders/" + senderID + "/staged",
	} {
		t.Run(path, func(t *testing.T) {
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
			require.Equal(t, http.StatusOK, rr.Code)
			var state types.SenderState
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &state))
			require.True(t, state.MasterEnable)
			require.Equal(t, "activate_immediate", state.Activation.Mode)
			require.Len(t, state.TransportParams, 1)
			require.Equal(t, domainID, *state.TransportParams[0].MxlDomainID)
			require.Equal(t, flowID, *state.TransportParams[0].MxlFlowID)
		})
	}

	rr = httptest.NewRecorder()
	body := strings.NewReader(`{"master_enable":false,"transport_params":[{"mxl_domain_id":"auto","mxl_flow_id":"auto"}]}`)
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPatch, "/x-nmos/connection/v1.2/single/senders/"+senderID+"/staged", body))
	require.Equal(t, http.StatusOK, rr.Code)
	var patched types.SenderState
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &patched))
	require.True(t, patched.MasterEnable)
	require.Len(t, patched.TransportParams, 1)
	require.Equal(t, domainID, *patched.TransportParams[0].MxlDomainID)
	require.Equal(t, flowID, *patched.TransportParams[0].MxlFlowID)

	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/x-nmos/connection/v1.2/single/senders/"+senderID+"/constraints", nil))
	require.Equal(t, http.StatusOK, rr.Code)
	var constraints []types.TransportParamConstraint
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &constraints))
	require.Len(t, constraints, 1)
	require.Equal(t, []string{domainID}, constraints[0].MxlDomainID.Enum)
	require.Equal(t, []string{flowID}, constraints[0].MxlFlowID.Enum)
	require.NotContains(t, constraints[0].MxlDomainID.Enum, "auto")
	require.NotContains(t, constraints[0].MxlFlowID.Enum, "auto")

	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/x-nmos/connection/v1.2/single/senders/"+senderID+"/transportfile", nil))
	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `{"mxl_domain_id":"11111111-1111-4111-8111-111111111111","mxl_flow_id":"5fbec3b1-1b0f-417d-9059-8b94a47197ed"}`, rr.Body.String())
}

func TestVersionCorsMethodAndErrorResponses(t *testing.T) {
	h := New(Options{NodeName: "node-a", DomainID: "node-a", Host: "127.0.0.1", Port: 1080, Cache: &staticCache{}})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/x-nmos/node/", nil))
	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `["v1.3/"]`, rr.Body.String())

	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodOptions, "/x-nmos/node/v1.3/senders", nil))
	require.Equal(t, http.StatusNoContent, rr.Code)
	require.Contains(t, rr.Header().Get("Access-Control-Allow-Methods"), http.MethodGet)

	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/x-nmos/node/v1.3/senders", nil))
	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
	require.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	require.JSONEq(t, `{"code":405,"error":"method not allowed","debug":"POST is not supported"}`, rr.Body.String())

	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/x-nmos/node/v1.3/not-there", nil))
	require.Equal(t, http.StatusNotFound, rr.Code)
	require.JSONEq(t, `{"code":404,"error":"not found","debug":"resource not found"}`, rr.Body.String())
}

func TestServerReturnsErrorWhenCacheCannotBuildResources(t *testing.T) {
	h := New(Options{NodeName: "node-a", DomainID: "node-a", Cache: &staticCache{missingDomain: true}})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/x-nmos/node/v1.3/senders", nil))
	require.Equal(t, http.StatusInternalServerError, rr.Code)
	require.JSONEq(t, `{"code":500,"error":"internal error","debug":"MxlDomain node-a is not cached"}`, rr.Body.String())
}

func TestServerRecoversPanicsAsJSONErrors(t *testing.T) {
	h := New(Options{NodeName: "node-a", DomainID: "node-a", Cache: panicCache{}})

	rr := httptest.NewRecorder()
	require.NotPanics(t, func() {
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/x-nmos/node/v1.3/senders", nil))
	})
	require.Equal(t, http.StatusInternalServerError, rr.Code)
	require.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	require.Equal(t, "*", rr.Header().Get("Access-Control-Allow-Origin"))
	require.JSONEq(t, `{"code":500,"error":"internal error","debug":"unexpected server error"}`, rr.Body.String())
}

func TestServerLogsThroughInjectedControllerRuntimeLogger(t *testing.T) {
	var mu sync.Mutex
	var lines []string
	logger := funcr.New(func(prefix, args string) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, strings.TrimSpace(prefix+" "+args))
	}, funcr.Options{})
	h := New(Options{NodeName: "node-a", DomainID: "node-a", Cache: panicCache{}, Logger: logger})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/x-nmos/node/v1.3/senders", nil))

	mu.Lock()
	defer mu.Unlock()
	got := strings.Join(lines, "\n")
	require.NotEmpty(t, got)
	require.Contains(t, got, "NMOS request panic")
	require.Contains(t, got, "NMOS request")
}

func TestServerAcceptsControllerRuntimeZapLogger(t *testing.T) {
	var logs bytes.Buffer
	logger := ctrlzap.New(ctrlzap.WriteTo(&logs), ctrlzap.UseDevMode(false))
	h := New(Options{NodeName: "node-a", DomainID: "node-a", Cache: panicCache{}, Logger: logger})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/x-nmos/node/v1.3/senders", nil))

	require.Equal(t, http.StatusInternalServerError, rr.Code)
	require.Contains(t, logs.String(), `"msg":"NMOS request panic"`)
	require.Contains(t, logs.String(), `"msg":"NMOS request"`)
}

type staticCache struct {
	domain        mxlv1.MxlDomain
	flows         []mxlv1.MxlFlow
	missingDomain bool
}

func (s *staticCache) GetDomain(id string) (mxlv1.MxlDomain, bool) {
	if s.missingDomain || s.domain.Name != id {
		return mxlv1.MxlDomain{}, false
	}
	return s.domain, true
}

func (s *staticCache) GetDomainFlows(string) []mxlv1.MxlFlow {
	return append([]mxlv1.MxlFlow(nil), s.flows...)
}

type panicCache struct{}

func (panicCache) GetDomain(string) (mxlv1.MxlDomain, bool) {
	panic("cache exploded")
}

func (panicCache) GetDomainFlows(string) []mxlv1.MxlFlow {
	panic("cache exploded")
}

func mxlFlow(name string, id string, definition string) mxlv1.MxlFlow {
	return mxlv1.MxlFlow{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: mxlv1.MxlFlowSpec{
			ID:         id,
			Definition: runtime.RawExtension{Raw: []byte(definition)},
		},
	}
}
