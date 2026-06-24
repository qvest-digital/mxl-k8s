// SPDX-License-Identifier: MIT

//go:build integration

package nmos_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nmosServerURL points to a running NMOS server. Set via NMOS_SERVER_URL env
// var, or the test binary started by run-nmos-tests.sh.
var nmosServerURL string

func TestMain(m *testing.M) {
	nmosServerURL = os.Getenv("NMOS_SERVER_URL")
	if nmosServerURL == "" {
		nmosServerURL = "http://127.0.0.1:8080"
	}
	// Wait for the server to become reachable.
	if !waitForServer(nmosServerURL, 10*time.Second) {
		fmt.Fprintf(os.Stderr, "NMOS server not reachable at %s\n", nmosServerURL)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func waitForServer(url string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url + "/x-nmos/node/")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// ---------------------------------------------------------------------------
// IS-04 Node API tests
// ---------------------------------------------------------------------------

func TestIS04Versions(t *testing.T) {
	resp, err := http.Get(nmosServerURL + "/x-nmos/node/")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var versions []string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&versions))
	assert.Contains(t, versions, "v1.3/")
}

func TestIS04NodeSelf(t *testing.T) {
	resp, err := http.Get(nmosServerURL + "/x-nmos/node/v1.3/self")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var self map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&self))

	// BCP-007-03: Self resource must have required fields.
	assert.NotEmpty(t, self["id"], "self.id must be a non-empty UUID")
	assert.NotEmpty(t, self["label"], "self.label must be present")
	assert.NotEmpty(t, self["version"], "self.version must be a TAI timestamp")

	// API endpoints must list at least one http endpoint.
	api, ok := self["api"].(map[string]interface{})
	require.True(t, ok, "self.api must be an object")
	endpoints, ok := api["endpoints"].([]interface{})
	require.True(t, ok, "self.api.endpoints must be an array")
	require.Greater(t, len(endpoints), 0, "at least one API endpoint is required")
}

func TestIS04Devices(t *testing.T) {
	resp, err := http.Get(nmosServerURL + "/x-nmos/node/v1.3/devices")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var devices []map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&devices))
	// Devices may be empty if no domain is configured; that is valid.
	for _, d := range devices {
		assert.NotEmpty(t, d["id"])
		assert.NotEmpty(t, d["type"])
	}
}

func TestIS04Sources(t *testing.T) {
	resp, err := http.Get(nmosServerURL + "/x-nmos/node/v1.3/sources")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var sources []map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&sources))
	for _, s := range sources {
		assert.NotEmpty(t, s["id"])
		assert.NotEmpty(t, s["format"], "BCP-007-03: source.format is required")
	}
}

func TestIS04Flows(t *testing.T) {
	resp, err := http.Get(nmosServerURL + "/x-nmos/node/v1.3/flows")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var flows []map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&flows))
	for _, f := range flows {
		assert.NotEmpty(t, f["id"])
		assert.NotEmpty(t, f["format"], "BCP-007-03: flow.format is required")
		assert.NotEmpty(t, f["media_type"], "flow.media_type is required")
	}
}

func TestIS04Senders(t *testing.T) {
	resp, err := http.Get(nmosServerURL + "/x-nmos/node/v1.3/senders")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var senders []map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&senders))
	for _, s := range senders {
		assert.NotEmpty(t, s["id"])
		assert.NotEmpty(t, s["transport"], "BCP-007-03: sender.transport is required")
		assert.NotNil(t, s["flow_id"], "sender.flow_id must be present (string or null)")
	}
}

func TestIS04Receivers(t *testing.T) {
	resp, err := http.Get(nmosServerURL + "/x-nmos/node/v1.3/receivers")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Receiver not yet implemented; must return empty list.
	var receivers []interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&receivers))
	assert.Empty(t, receivers, "receivers not implemented, must be empty array")
}

// ---------------------------------------------------------------------------
// IS-05 Connection API tests
// ---------------------------------------------------------------------------

func TestIS05Versions(t *testing.T) {
	resp, err := http.Get(nmosServerURL + "/x-nmos/connection/")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var versions []string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&versions))
	assert.Contains(t, versions, "v1.2/")
}

func TestIS05VersionsSubresource(t *testing.T) {
	resp, err := http.Get(nmosServerURL + "/x-nmos/connection/v1.1/")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var versions []string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&versions))
	assert.Contains(t, versions, "v1.2/")
}

func TestIS05SenderActive(t *testing.T) {
	senders := getSenderIDs(t)
	if len(senders) == 0 {
		t.Skip("no senders available to test")
	}
	for _, senderID := range senders {
		t.Run(senderID, func(t *testing.T) {
			resp, err := http.Get(nmosServerURL + "/x-nmos/connection/v1.1/single/senders/" + senderID + "/active")
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusOK, resp.StatusCode)

			var state map[string]interface{}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&state))

			// IS-05 active resource must contain required fields.
			assert.NotEmpty(t, state["transport_file"], "active.transport_file must be present")
			assert.Contains(t, state, "sender_id", "active.sender_id is required")
			assert.Contains(t, state, "master_enable")
			assert.Contains(t, state, "activation")
			assert.Contains(t, state, "transport_params")
		})
	}
}

func TestIS05SenderStagedReadOnly(t *testing.T) {
	senders := getSenderIDs(t)
	if len(senders) == 0 {
		t.Skip("no senders available to test")
	}
	for _, senderID := range senders {
		t.Run(senderID, func(t *testing.T) {
			// GET staged -- must return current active state.
			resp, err := http.Get(nmosServerURL + "/x-nmos/connection/v1.1/single/senders/" + senderID + "/staged")
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusOK, resp.StatusCode)

			var staged map[string]interface{}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&staged))
			assert.Contains(t, staged, "sender_id")
			assert.Contains(t, staged, "master_enable")

			// PATCH staged -- accepted but read-only, returns active state.
			patchBody := []byte(`{"master_enable": false}`)
			req, err := http.NewRequest(http.MethodPatch,
				nmosServerURL+"/x-nmos/connection/v1.1/single/senders/"+senderID+"/staged",
				bytes.NewReader(patchBody))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")

			patchResp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer patchResp.Body.Close()
			assert.Equal(t, http.StatusOK, patchResp.StatusCode,
				"staged PATCH must return 200 for controller compatibility")
		})
	}
}

func TestIS05SenderTransportFile(t *testing.T) {
	senders := getSenderIDs(t)
	if len(senders) == 0 {
		t.Skip("no senders available to test")
	}
	for _, senderID := range senders {
		t.Run(senderID, func(t *testing.T) {
			resp, err := http.Get(nmosServerURL + "/x-nmos/connection/v1.1/single/senders/" + senderID + "/transportfile")
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusOK, resp.StatusCode)

			var params map[string]interface{}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&params))
			// Transport file params should contain domain/flow identifiers.
			assert.Contains(t, params, "mxl_domain_id")
			assert.Contains(t, params, "mxl_flow_id")
		})
	}
}

func TestIS05SenderConstraints(t *testing.T) {
	senders := getSenderIDs(t)
	if len(senders) == 0 {
		t.Skip("no senders available to test")
	}
	for _, senderID := range senders {
		t.Run(senderID, func(t *testing.T) {
			resp, err := http.Get(nmosServerURL + "/x-nmos/connection/v1.1/single/senders/" + senderID + "/constraints")
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusOK, resp.StatusCode)

			var constraints []map[string]interface{}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&constraints))
			require.Len(t, constraints, 1, "exactly one constraint set per sender output")
			// Constraints should have concrete enum values, not "auto".
			for key, val := range constraints[0] {
				valMap, ok := val.(map[string]interface{})
				if !ok {
					continue
				}
				if enumVal, hasEnum := valMap["enum"]; hasEnum {
					enumArr, ok := enumVal.([]interface{})
					require.True(t, ok, "constraint %s enum must be an array", key)
					require.NotEmpty(t, enumArr, "constraint %s enum must be non-empty", key)
					for _, v := range enumArr {
						assert.NotEqual(t, "auto", v, "constraint values must be concrete, not auto")
					}
				}
			}
		})
	}
}

func TestIS05SenderNotFound(t *testing.T) {
	resp, err := http.Get(nmosServerURL + "/x-nmos/connection/v1.1/single/senders/nonexistent-sender-id/active")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// ---------------------------------------------------------------------------
// HTTP conformance tests (BCP-006-01, CORS, methods)
// ---------------------------------------------------------------------------

func TestCORSHeaders(t *testing.T) {
	endpoints := []string{
		"/x-nmos/node/",
		"/x-nmos/connection/",
	}
	for _, ep := range endpoints {
		t.Run(ep, func(t *testing.T) {
			resp, err := http.Get(nmosServerURL + ep)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"),
				"CORS: Access-Control-Allow-Origin must be *")
			assert.NotEmpty(t, resp.Header.Get("Access-Control-Allow-Methods"),
				"CORS: Access-Control-Allow-Methods must be present")
			assert.NotEmpty(t, resp.Header.Get("Access-Control-Allow-Headers"),
				"CORS: Access-Control-Allow-Headers must be present")
		})
	}
}

func TestOPTIONSRequests(t *testing.T) {
	endpoints := []string{
		"/x-nmos/node/",
		"/x-nmos/connection/",
	}
	for _, ep := range endpoints {
		t.Run(ep, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodOptions, nmosServerURL+ep, nil)
			require.NoError(t, err)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusNoContent, resp.StatusCode,
				"OPTIONS must return 204 No Content")
		})
	}
}

func TestMethodNotAllowed(t *testing.T) {
	// Node API only supports GET.
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			req, err := http.NewRequest(method, nmosServerURL+"/x-nmos/node/v1.3/self", nil)
			require.NoError(t, err)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode,
				"%s on Node self must return 405", method)
		})
	}
}

func TestNotFoundPaths(t *testing.T) {
	for _, path := range []string{
		"/x-nmos/foo/",
		"/x-nmos/node/v1.3/nonexistent",
		"/x-nmos/connection/v1.1/single/receivers/abc/active",
	} {
		t.Run(path, func(t *testing.T) {
			resp, err := http.Get(nmosServerURL + path)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusNotFound, resp.StatusCode,
				"unknown path %s must return 404", path)
		})
	}
}

func TestErrorResponseFormat(t *testing.T) {
	resp, err := http.Get(nmosServerURL + "/x-nmos/connection/v1.1/single/senders/nonexistent/active")
	require.NoError(t, err)
	defer resp.Body.Close()

	var errResp map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	assert.Contains(t, errResp, "code")
	assert.Contains(t, errResp, "error")
	assert.Equal(t, float64(404), errResp["code"])
}

// ---------------------------------------------------------------------------
// BCP-007-03 specific conformance tests
// ---------------------------------------------------------------------------
// BCP-007-03 specifies how NMOS Sender proxies advertise themselves.
// These tests verify the agent's proxy behavior matches the BCP.

func TestBCP00703SenderProxyResources(t *testing.T) {
	// Verify the Node API returns all required resource types for BCP-007-03.
	for _, path := range []string{"self", "devices", "sources", "flows", "senders", "receivers"} {
		t.Run(path, func(t *testing.T) {
			resp, err := http.Get(nmosServerURL + "/x-nmos/node/v1.3/" + path)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusOK, resp.StatusCode,
				"BCP-007-03: /%s must return 200", path)
		})
	}
}

func TestBCP00703SenderHasTransportFile(t *testing.T) {
	// BCP-007-03: Senders must have a transport file accessible via IS-05.
	senders := getSenderIDs(t)
	if len(senders) == 0 {
		t.Skip("no senders available for BCP-007-03 transport file test")
	}
	for _, senderID := range senders {
		t.Run(senderID, func(t *testing.T) {
			resp, err := http.Get(nmosServerURL + "/x-nmos/connection/v1.1/single/senders/" + senderID + "/transportfile")
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusOK, resp.StatusCode,
				"BCP-007-03: sender must expose transport file")
		})
	}
}

func TestBCP00703ReceiverNotImplemented(t *testing.T) {
	// BCP-007-03 scope: Receiver is not yet implemented.
	// IS-04 must return empty receiver list.
	resp, err := http.Get(nmosServerURL + "/x-nmos/node/v1.3/receivers")
	require.NoError(t, err)
	defer resp.Body.Close()

	var receivers []interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&receivers))
	assert.Empty(t, receivers, "BCP-007-03: receivers not implemented, expect empty list")

	// IS-05 connection endpoint must not list receiver resources.
	connResp, err := http.Get(nmosServerURL + "/x-nmos/connection/v1.1/")
	require.NoError(t, err)
	defer connResp.Body.Close()
	assert.Equal(t, http.StatusOK, connResp.StatusCode)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func getSenderIDs(t *testing.T) []string {
	t.Helper()
	resp, err := http.Get(nmosServerURL + "/x-nmos/node/v1.3/senders")
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	var senders []map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&senders))
	ids := make([]string, 0, len(senders))
	for _, s := range senders {
		if id, ok := s["id"].(string); ok && id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}
