// Package types contains NMOS IS-04 and IS-05 JSON resource types for MXL senders.
package types

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	mxlv1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// TransportMXL is the BCP-007-03 MXL transport URN.
const TransportMXL = "urn:x-nmos:transport:mxl"

var namespaceUUID = uuid.MustParse("1ee6477a-e0c7-5b1c-8af3-7f29e2a5444f")

// Tags is the NMOS tags map shape.
type Tags map[string][]string

// ResourceSet groups IS-04 Node API resources derived from mxl-k8s CRDs.
type ResourceSet struct {
	Node    Node
	Devices []Device
	Sources []Source
	Flows   []Flow
	Senders []Sender
}

// Node is an IS-04 Node API v1.3 node resource.
type Node struct {
	ID          string         `json:"id"`
	Version     string         `json:"version"`
	Label       string         `json:"label"`
	Description string         `json:"description"`
	Tags        Tags           `json:"tags"`
	Caps        map[string]any `json:"caps"`
	Href        string         `json:"href"`
	Hostname    string         `json:"hostname"`
	API         NodeAPI        `json:"api"`
	Services    []Service      `json:"services"`
	Clocks      []Clock        `json:"clocks"`
	Interfaces  []Interface    `json:"interfaces"`
}

// NodeAPI is the IS-04 node API version and endpoint descriptor.
type NodeAPI struct {
	Versions  []string   `json:"versions"`
	Endpoints []Endpoint `json:"endpoints"`
}

// Endpoint describes one NMOS API endpoint.
type Endpoint struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Protocol string `json:"protocol"`
}

// Service is an advertised NMOS service.
type Service struct {
	Href string `json:"href"`
	Type string `json:"type"`
}

// Clock is an IS-04 clock descriptor.
type Clock struct {
	Name    string `json:"name"`
	RefType string `json:"ref_type"`
}

// Interface is an IS-04 node network interface descriptor.
type Interface struct {
	Name string `json:"name"`
}

// Device is an IS-04 Node API v1.3 device resource.
type Device struct {
	ID          string    `json:"id"`
	Version     string    `json:"version"`
	Label       string    `json:"label"`
	Description string    `json:"description"`
	Tags        Tags      `json:"tags"`
	Type        string    `json:"type"`
	NodeID      string    `json:"node_id"`
	Senders     []string  `json:"senders"`
	Receivers   []string  `json:"receivers"`
	Controls    []Control `json:"controls"`
}

// Control is an IS-04 device control endpoint descriptor.
type Control struct {
	Href string `json:"href"`
	Type string `json:"type"`
}

// Source is an IS-04 Node API v1.3 source resource.
type Source struct {
	ID          string         `json:"id"`
	Version     string         `json:"version"`
	Label       string         `json:"label"`
	Description string         `json:"description"`
	Tags        Tags           `json:"tags"`
	Format      string         `json:"format"`
	Caps        map[string]any `json:"caps"`
	DeviceID    string         `json:"device_id"`
	Parents     []string       `json:"parents"`
	ClockName   *string        `json:"clock_name"`
}

// Flow is an IS-04 Node API v1.3 flow resource.
type Flow struct {
	ID          string         `json:"id"`
	Version     string         `json:"version"`
	Label       string         `json:"label"`
	Description string         `json:"description"`
	Tags        Tags           `json:"tags"`
	Format      string         `json:"format"`
	Parents     []string       `json:"parents"`
	SourceID    string         `json:"source_id,omitempty"`
	DeviceID    string         `json:"device_id,omitempty"`
	MediaType   string         `json:"media_type,omitempty"`
	Extra       map[string]any `json:"-"`
}

// Sender is an IS-04 Node API v1.3 sender resource.
type Sender struct {
	ID                string       `json:"id"`
	Version           string       `json:"version"`
	Label             string       `json:"label"`
	Description       string       `json:"description"`
	Tags              Tags         `json:"tags"`
	FlowID            string       `json:"flow_id"`
	Transport         string       `json:"transport"`
	DeviceID          string       `json:"device_id"`
	ManifestHref      *string      `json:"manifest_href"`
	InterfaceBindings []string     `json:"interface_bindings"`
	Subscription      Subscription `json:"subscription"`
}

// Subscription is the sender subscription state embedded in IS-04 senders.
type Subscription struct {
	Active     bool    `json:"active"`
	ReceiverID *string `json:"receiver_id"`
}

// SenderTransportParams is the BCP-007-03 IS-05 sender transport params leg.
type SenderTransportParams struct {
	MxlDomainID *string `json:"mxl_domain_id"`
	MxlFlowID   *string `json:"mxl_flow_id"`
}

// SenderActivation is the activation block returned by IS-05 sender endpoints.
type SenderActivation struct {
	Mode           string  `json:"mode"`
	RequestedTime  *string `json:"requested_time"`
	ActivationTime string  `json:"activation_time"`
}

// TransportFile is the IS-05 transport_file object carried in active/staged
// sender responses. For BCP-007-03 MXL, Data carries the MXL transport
// parameters as a JSON string and Type identifies the transport file media type.
type TransportFile struct {
	Data string `json:"data"`
	Type string `json:"type"`
}

// SenderState is an IS-05 active or staged sender response.
type SenderState struct {
	SenderID        string                  `json:"sender_id"`
	ReceiverID      *string                 `json:"receiver_id"`
	MasterEnable    bool                    `json:"master_enable"`
	Activation      SenderActivation        `json:"activation"`
	TransportFile   TransportFile           `json:"transport_file"`
	TransportParams []SenderTransportParams `json:"transport_params"`
}

// TransportParamConstraint is one IS-05 sender constraints leg.
type TransportParamConstraint struct {
	MxlDomainID EnumConstraint `json:"mxl_domain_id"`
	MxlFlowID   EnumConstraint `json:"mxl_flow_id"`
}

// EnumConstraint limits a transport parameter to a set of concrete values.
type EnumConstraint struct {
	Enum []string `json:"enum"`
}

// BuildIS04Resources derives IS-04 node, device, source, flow, and sender resources
// for MXL flows whose origin location is on nodeName.
func BuildIS04Resources(nodeName string, domain mxlv1.MxlDomain, flows []mxlv1.MxlFlow, version string) (ResourceSet, error) {
	nodeID := deterministicID("node", nodeName)
	deviceID := deterministicID("device", nodeName, domain.Name, domain.Spec.HostPath)
	resources := ResourceSet{
		Node: Node{
			ID:          nodeID,
			Version:     version,
			Label:       nodeName,
			Description: "MXL node " + nodeName,
			Tags:        Tags{},
			Caps:        map[string]any{},
			Hostname:    nodeName,
			API:         NodeAPI{Versions: []string{"v1.3"}, Endpoints: []Endpoint{}},
			Services:    []Service{},
			Clocks:      []Clock{},
			Interfaces:  []Interface{},
		},
		Devices: []Device{{
			ID:          deviceID,
			Version:     version,
			Label:       nodeName + " MXL domain",
			Description: "MXL domain on " + nodeName,
			Tags:        Tags{},
			Type:        "urn:x-nmos:device:generic",
			NodeID:      nodeID,
			Senders:     []string{},
			Receivers:   []string{},
			Controls:    []Control{},
		}},
	}

	for _, mxlFlow := range flows {
		if !hasOriginOnNode(mxlFlow, nodeName) {
			continue
		}
		flow, err := flowFromDefinition(mxlFlow)
		if err != nil {
			return ResourceSet{}, err
		}
		if flow.ID == "" {
			flow.ID = mxlFlow.Spec.ID
		}
		flow.Version = version
		flow.Tags = ensureTags(flow.Tags)

		sourceID := deterministicID("source", nodeName, flow.ID)
		senderID := deterministicID("sender", nodeName, flow.ID)
		flow.SourceID = sourceID

		source := Source{
			ID:          sourceID,
			Version:     version,
			Label:       firstNonEmpty(flow.Label, flow.ID),
			Description: flow.Description,
			Tags:        copyTags(flow.Tags),
			Format:      flow.Format,
			Caps:        map[string]any{},
			DeviceID:    deviceID,
			Parents:     append([]string(nil), flow.Parents...),
			ClockName:   nil,
		}
		sender := Sender{
			ID:                senderID,
			Version:           version,
			Label:             firstNonEmpty(flow.Label, "MXL sender"),
			Description:       firstNonEmpty(flow.Description, "MXL sender"),
			Tags:              copyTags(flow.Tags),
			FlowID:            flow.ID,
			Transport:         TransportMXL,
			DeviceID:          deviceID,
			ManifestHref:      nil,
			InterfaceBindings: []string{},
			Subscription:      Subscription{Active: true, ReceiverID: nil},
		}

		resources.Sources = append(resources.Sources, source)
		resources.Flows = append(resources.Flows, flow)
		resources.Senders = append(resources.Senders, sender)
		resources.Devices[0].Senders = append(resources.Devices[0].Senders, sender.ID)
	}

	return resources, nil
}

// MarshalJSON preserves flow-definition fields that are not modeled explicitly.
func (f Flow) MarshalJSON() ([]byte, error) {
	m := map[string]any{}
	for k, v := range f.Extra {
		m[k] = v
	}
	m["id"] = f.ID
	m["version"] = f.Version
	m["label"] = f.Label
	m["description"] = f.Description
	m["tags"] = ensureTags(f.Tags)
	m["format"] = f.Format
	m["parents"] = f.Parents
	if f.SourceID != "" {
		m["source_id"] = f.SourceID
	}
	if f.DeviceID != "" {
		m["device_id"] = f.DeviceID
	}
	if f.MediaType != "" {
		m["media_type"] = f.MediaType
	}
	return json.Marshal(m)
}

// UnmarshalJSON reads known flow fields and preserves additional schema fields.
func (f *Flow) UnmarshalJSON(data []byte) error {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	f.Extra = map[string]any{}
	for k, v := range raw {
		switch k {
		case "id":
			f.ID, _ = v.(string)
		case "version":
			f.Version, _ = v.(string)
		case "label":
			f.Label, _ = v.(string)
		case "description":
			f.Description, _ = v.(string)
		case "tags":
			b, err := json.Marshal(v)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(b, &f.Tags); err != nil {
				return err
			}
		case "format":
			f.Format, _ = v.(string)
		case "parents":
			b, err := json.Marshal(v)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(b, &f.Parents); err != nil {
				return err
			}
		case "source_id":
			f.SourceID, _ = v.(string)
		case "device_id":
			f.DeviceID, _ = v.(string)
		case "media_type":
			f.MediaType, _ = v.(string)
		default:
			f.Extra[k] = v
		}
	}
	return nil
}

func flowFromDefinition(mxlFlow mxlv1.MxlFlow) (Flow, error) {
	var flow Flow
	if len(mxlFlow.Spec.Definition.Raw) == 0 {
		return Flow{}, fmt.Errorf("MxlFlow %s has empty definition", mxlFlow.Name)
	}
	if err := json.Unmarshal(mxlFlow.Spec.Definition.Raw, &flow); err != nil {
		return Flow{}, fmt.Errorf("decode MxlFlow %s definition: %w", mxlFlow.Name, err)
	}
	return flow, nil
}

func hasOriginOnNode(flow mxlv1.MxlFlow, nodeName string) bool {
	for _, location := range flow.Status.Locations {
		if location.NodeName == nodeName && location.Phase == mxlv1.MxlFlowLocationOrigin {
			return true
		}
	}
	return false
}

func deterministicID(parts ...string) string {
	name := ""
	for i, part := range parts {
		if i > 0 {
			name += ":"
		}
		name += part
	}
	return uuid.NewSHA1(namespaceUUID, []byte(name)).String()
}

func ensureTags(tags Tags) Tags {
	if tags == nil {
		return Tags{}
	}
	return tags
}

func copyTags(tags Tags) Tags {
	out := Tags{}
	for key, values := range tags {
		out[key] = append([]string(nil), values...)
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
