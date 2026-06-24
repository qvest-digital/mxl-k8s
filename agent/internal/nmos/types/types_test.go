package types

import (
	"encoding/json"
	"testing"

	mxlv1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestSenderMarshalMatchesBCP00703Example(t *testing.T) {
	sender := Sender{
		Description:       "MXL sender",
		DeviceID:          "6c370020-79bf-5cf6-8f3b-c12e3cb28b5d",
		FlowID:            "fb75bb21-36a1-5045-8a3d-a8ee69e913d7",
		ID:                "dd7b118d-9840-53ba-98e3-a44d97c55d28",
		InterfaceBindings: []string{},
		Label:             "MXL sender",
		ManifestHref:      nil,
		Subscription:      Subscription{Active: true, ReceiverID: nil},
		Tags:              Tags{},
		Transport:         TransportMXL,
		Version:           "2026-06-23T12:34:56.000000000Z",
	}

	got, err := json.Marshal(sender)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(got, &decoded))
	require.Equal(t, "MXL sender", decoded["description"])
	require.Equal(t, "6c370020-79bf-5cf6-8f3b-c12e3cb28b5d", decoded["device_id"])
	require.Equal(t, "fb75bb21-36a1-5045-8a3d-a8ee69e913d7", decoded["flow_id"])
	require.Equal(t, "dd7b118d-9840-53ba-98e3-a44d97c55d28", decoded["id"])
	require.Empty(t, decoded["interface_bindings"])
	require.Nil(t, decoded["manifest_href"])
	require.Equal(t, TransportMXL, decoded["transport"])
	require.Equal(t, map[string]any{"active": true, "receiver_id": nil}, decoded["subscription"])
}

func TestSenderTransportParamsMarshalRoundTrip(t *testing.T) {
	domainID := "11111111-1111-4111-8111-111111111111"
	flowID := "22222222-2222-4222-8222-222222222222"
	params := SenderTransportParams{
		MxlDomainID: &domainID,
		MxlFlowID:   &flowID,
	}

	got, err := json.Marshal(params)
	require.NoError(t, err)
	require.JSONEq(t, `{"mxl_domain_id":"11111111-1111-4111-8111-111111111111","mxl_flow_id":"22222222-2222-4222-8222-222222222222"}`, string(got))

	var roundTrip SenderTransportParams
	require.NoError(t, json.Unmarshal(got, &roundTrip))
	require.Equal(t, params, roundTrip)

	nullParams := SenderTransportParams{}
	nullJSON, err := json.Marshal(nullParams)
	require.NoError(t, err)
	require.JSONEq(t, `{"mxl_domain_id":null,"mxl_flow_id":null}`, string(nullJSON))
}

func TestTransportFileMarshalRoundTrip(t *testing.T) {
	tf := TransportFile{
		Data: `{"mxl_domain_id":"11111111-1111-4111-8111-111111111111","mxl_flow_id":"22222222-2222-4222-8222-222222222222"}`,
		Type: "application/json",
	}
	got, err := json.Marshal(tf)
	require.NoError(t, err)
	require.JSONEq(t, `{"data":"{\"mxl_domain_id\":\"11111111-1111-4111-8111-111111111111\",\"mxl_flow_id\":\"22222222-2222-4222-8222-222222222222\"}","type":"application/json"}`, string(got))

	var roundTrip TransportFile
	require.NoError(t, json.Unmarshal(got, &roundTrip))
	require.Equal(t, tf, roundTrip)
}

func TestSenderStateMarshalHasSenderIDAndTransportFile(t *testing.T) {
	domainID := "11111111-1111-4111-8111-111111111111"
	flowID := "22222222-2222-4222-8222-222222222222"
	state := SenderState{
		SenderID:     "sender-id-123",
		ReceiverID:   nil,
		MasterEnable: true,
		Activation:   SenderActivation{Mode: "activate_immediate", RequestedTime: nil, ActivationTime: "2026-06-24T12:00:37.000000000Z"},
		TransportFile: TransportFile{
			Data: `{"mxl_domain_id":"` + domainID + `","mxl_flow_id":"` + flowID + `"}`,
			Type: "application/json",
		},
		TransportParams: []SenderTransportParams{{MxlDomainID: &domainID, MxlFlowID: &flowID}},
	}
	got, err := json.Marshal(state)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(got, &decoded))
	require.Equal(t, "sender-id-123", decoded["sender_id"])
	require.Contains(t, decoded, "transport_file")
	tf, ok := decoded["transport_file"].(map[string]any)
	require.True(t, ok)
	require.NotEmpty(t, tf["data"])
	require.Equal(t, "application/json", tf["type"])
}

func TestNodeDeviceSourceFlowMarshalRoundTrip(t *testing.T) {
	node := Node{
		ID:          "11111111-1111-4111-8111-111111111111",
		Version:     "2026-06-23T12:34:56.000000000Z",
		Label:       "node-a",
		Description: "MXL node node-a",
		Tags:        Tags{},
		Caps:        map[string]any{},
		Hostname:    "node-a",
		API:         NodeAPI{Versions: []string{"v1.3"}, Endpoints: []Endpoint{}},
		Services:    []Service{},
		Clocks:      []Clock{},
		Interfaces:  []Interface{},
	}
	nodeJSON, err := json.Marshal(node)
	require.NoError(t, err)
	require.JSONEq(t, `{"id":"11111111-1111-4111-8111-111111111111","version":"2026-06-23T12:34:56.000000000Z","label":"node-a","description":"MXL node node-a","tags":{},"caps":{},"href":"","hostname":"node-a","api":{"versions":["v1.3"],"endpoints":[]},"services":[],"clocks":[],"interfaces":[]}`, string(nodeJSON))

	device := Device{ID: "device-id", Version: node.Version, Label: "device", Tags: Tags{}, Type: "urn:x-nmos:device:generic", NodeID: node.ID, Senders: []string{"sender-id"}, Receivers: []string{}, Controls: []Control{}}
	deviceJSON, err := json.Marshal(device)
	require.NoError(t, err)
	require.JSONEq(t, `{"id":"device-id","version":"2026-06-23T12:34:56.000000000Z","label":"device","description":"","tags":{},"type":"urn:x-nmos:device:generic","node_id":"11111111-1111-4111-8111-111111111111","senders":["sender-id"],"receivers":[],"controls":[]}`, string(deviceJSON))

	source := Source{ID: "source-id", Version: node.Version, Label: "source", Tags: Tags{}, Format: "urn:x-nmos:format:video", Caps: map[string]any{}, DeviceID: device.ID, Parents: []string{}, ClockName: nil}
	sourceJSON, err := json.Marshal(source)
	require.NoError(t, err)
	require.JSONEq(t, `{"id":"source-id","version":"2026-06-23T12:34:56.000000000Z","label":"source","description":"","tags":{},"format":"urn:x-nmos:format:video","caps":{},"device_id":"device-id","parents":[],"clock_name":null}`, string(sourceJSON))

	var flow Flow
	require.NoError(t, json.Unmarshal([]byte(`{"id":"flow-id","version":"2026-06-23T12:34:56.000000000Z","label":"flow","description":"desc","tags":{},"format":"urn:x-nmos:format:video","parents":[],"source_id":"source-id","media_type":"video/v210","frame_width":1920}`), &flow))
	require.Equal(t, float64(1920), flow.Extra["frame_width"])
	flowJSON, err := json.Marshal(flow)
	require.NoError(t, err)
	require.JSONEq(t, `{"id":"flow-id","version":"2026-06-23T12:34:56.000000000Z","label":"flow","description":"desc","tags":{},"format":"urn:x-nmos:format:video","parents":[],"source_id":"source-id","media_type":"video/v210","frame_width":1920}`, string(flowJSON))
}

func TestBuildIS04ResourcesFromCRDs(t *testing.T) {
	flow := mxlFlow("flow-a", "5fbec3b1-1b0f-417d-9059-8b94a47197ed", `{
		"description":"MXL Test Flow, 1080p29",
		"id":"5fbec3b1-1b0f-417d-9059-8b94a47197ed",
		"tags":{"urn:x-nmos:tag:grouphint/v1.0":["Media Function XYZ:Video"]},
		"format":"urn:x-nmos:format:video",
		"label":"MXL Test Flow, 1080p29",
		"parents":[],
		"media_type":"video/v210",
		"grain_rate":{"numerator":30000,"denominator":1001},
		"frame_width":1920
	}`)
	flow.Status.Locations = []mxlv1.MxlFlowLocation{
		{NodeName: "node-a", Phase: mxlv1.MxlFlowLocationOrigin},
		{NodeName: "node-b", Phase: mxlv1.MxlFlowLocationReady},
	}
	domain := mxlv1.MxlDomain{
		ObjectMeta: metav1.ObjectMeta{Name: "domain-a"},
		Spec:       mxlv1.MxlDomainSpec{NodeName: "node-a", HostPath: "/run/mxl/domain"},
	}

	resources, err := BuildIS04Resources("node-a", domain, []mxlv1.MxlFlow{flow}, "2026-06-23T12:34:56.000000000Z")
	require.NoError(t, err)
	require.Len(t, resources.Devices, 1)
	require.Len(t, resources.Sources, 1)
	require.Len(t, resources.Flows, 1)
	require.Len(t, resources.Senders, 1)

	require.Equal(t, "node-a", resources.Node.Hostname)
	require.Equal(t, resources.Devices[0].ID, resources.Sources[0].DeviceID)
	require.Equal(t, resources.Devices[0].ID, resources.Senders[0].DeviceID)
	require.Equal(t, "urn:x-nmos:format:video", resources.Sources[0].Format)
	require.Equal(t, "video/v210", resources.Flows[0].MediaType)
	require.Equal(t, "5fbec3b1-1b0f-417d-9059-8b94a47197ed", resources.Flows[0].ID)
	require.Equal(t, "5fbec3b1-1b0f-417d-9059-8b94a47197ed", resources.Senders[0].FlowID)
	require.Equal(t, TransportMXL, resources.Senders[0].Transport)
	require.Empty(t, resources.Senders[0].InterfaceBindings)
	require.Nil(t, resources.Senders[0].ManifestHref)

	flowJSON, err := json.Marshal(resources.Flows[0])
	require.NoError(t, err)
	require.JSONEq(t, `{"description":"MXL Test Flow, 1080p29","id":"5fbec3b1-1b0f-417d-9059-8b94a47197ed","tags":{"urn:x-nmos:tag:grouphint/v1.0":["Media Function XYZ:Video"]},"format":"urn:x-nmos:format:video","label":"MXL Test Flow, 1080p29","parents":[],"media_type":"video/v210","grain_rate":{"numerator":30000,"denominator":1001},"frame_width":1920,"version":"2026-06-23T12:34:56.000000000Z","source_id":"`+resources.Sources[0].ID+`"}`, string(flowJSON))
}

func TestBuildIS04ResourcesSkipsNonOriginFlows(t *testing.T) {
	flow := mxlFlow("flow-a", "5fbec3b1-1b0f-417d-9059-8b94a47197ed", `{"id":"5fbec3b1-1b0f-417d-9059-8b94a47197ed","format":"urn:x-nmos:format:video","label":"remote","parents":[],"media_type":"video/v210"}`)
	flow.Status.Locations = []mxlv1.MxlFlowLocation{{NodeName: "node-a", Phase: mxlv1.MxlFlowLocationReady}}
	domain := mxlv1.MxlDomain{Spec: mxlv1.MxlDomainSpec{NodeName: "node-a"}}

	resources, err := BuildIS04Resources("node-a", domain, []mxlv1.MxlFlow{flow}, "2026-06-23T12:34:56.000000000Z")
	require.NoError(t, err)
	require.Empty(t, resources.Sources)
	require.Empty(t, resources.Flows)
	require.Empty(t, resources.Senders)
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
