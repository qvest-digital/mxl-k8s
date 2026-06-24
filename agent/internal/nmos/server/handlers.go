package server

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/qvest-digital/mxl-k8s/agent/internal/nmos/types"
)

func (h *HandlerEnv) HandleNodeVersions(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, []string{"v1.3/"})
}

func (h *HandlerEnv) HandleConnectionVersions(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, []string{"v1.1/"})
}

func (h *HandlerEnv) HandleNode(w http.ResponseWriter, _ *http.Request) {
	resources, ok := h.resources(w)
	if !ok {
		return
	}
	writeJSON(w, resources.Node)
}

func (h *HandlerEnv) HandleDevices(w http.ResponseWriter, _ *http.Request) {
	resources, ok := h.resources(w)
	if !ok {
		return
	}
	writeJSON(w, resources.Devices)
}

func (h *HandlerEnv) HandleSources(w http.ResponseWriter, _ *http.Request) {
	resources, ok := h.resources(w)
	if !ok {
		return
	}
	writeJSON(w, resources.Sources)
}

func (h *HandlerEnv) HandleFlows(w http.ResponseWriter, _ *http.Request) {
	resources, ok := h.resources(w)
	if !ok {
		return
	}
	writeJSON(w, resources.Flows)
}

func (h *HandlerEnv) HandleSenders(w http.ResponseWriter, _ *http.Request) {
	resources, ok := h.resources(w)
	if !ok {
		return
	}
	writeJSON(w, resources.Senders)
}

func (h *HandlerEnv) HandleReceivers(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, []any{})
}

func (h *HandlerEnv) HandleSenderActive(w http.ResponseWriter, r *http.Request) {
	state, ok := h.senderState(w, r.PathValue("senderID"))
	if !ok {
		return
	}
	writeJSON(w, state)
}

func (h *HandlerEnv) HandleSenderStaged(w http.ResponseWriter, r *http.Request) {
	// MXL senders are always active from IS-05's point of view; staged PATCH
	// requests are accepted for controller compatibility but are read-only and
	// always return the current active transport parameters.
	h.HandleSenderActive(w, r)
}

func (h *HandlerEnv) HandleSenderTransportFile(w http.ResponseWriter, r *http.Request) {
	// BCP-007-03 (MXL IS-05 Senders and Receivers): the /transportfile
	// endpoint of an MXL IS-05 Sender MUST always return a 404. MXL transport
	// parameters are conveyed via the active/staged resources, not a transport
	// file, so there is no file to serve regardless of the sender ID.
	writeError(w, http.StatusNotFound, "not found", "MXL IS-05 senders do not expose a transport file")
}

func (h *HandlerEnv) HandleSenderConstraints(w http.ResponseWriter, r *http.Request) {
	state, ok := h.senderState(w, r.PathValue("senderID"))
	if !ok {
		return
	}
	params := state.TransportParams[0]
	constraint := types.TransportParamConstraint{}
	if params.MxlDomainID != nil {
		constraint.MxlDomainID.Enum = []string{*params.MxlDomainID}
	}
	if params.MxlFlowID != nil {
		constraint.MxlFlowID.Enum = []string{*params.MxlFlowID}
	}
	writeJSON(w, []types.TransportParamConstraint{constraint})
}

func (h *HandlerEnv) senderState(w http.ResponseWriter, senderID string) (types.SenderState, bool) {
	resources, ok := h.resources(w)
	if !ok {
		return types.SenderState{}, false
	}
	var flowID string
	for _, sender := range resources.Senders {
		if sender.ID == senderID {
			flowID = sender.FlowID
			break
		}
	}
	if flowID == "" {
		writeError(w, http.StatusNotFound, "not found", "sender not found")
		return types.SenderState{}, false
	}
	domainID := h.DomainID
	params := types.SenderTransportParams{MxlDomainID: &domainID, MxlFlowID: &flowID}
	return types.SenderState{
		SenderID:     senderID,
		ReceiverID:   nil,
		MasterEnable: true,
		Activation:   types.SenderActivation{Mode: activationModeImmediate, RequestedTime: nil, ActivationTime: nmosVersion(h.now())},
		TransportFile: types.TransportFile{
			Data: fmt.Sprintf(`{"mxl_domain_id":"%s","mxl_flow_id":"%s"}`, domainID, flowID),
			Type: "application/json",
		},
		TransportParams: []types.SenderTransportParams{params},
	}, true
}

func (h *HandlerEnv) resources(w http.ResponseWriter) (types.ResourceSet, bool) {
	if h.Cache == nil {
		writeError(w, http.StatusInternalServerError, "internal error", "NMOS cache is not configured")
		return types.ResourceSet{}, false
	}
	domain, ok := h.Cache.GetDomain(h.DomainID)
	if !ok {
		writeError(w, http.StatusInternalServerError, "internal error", fmt.Sprintf("MxlDomain %s is not cached", h.DomainID))
		return types.ResourceSet{}, false
	}
	resources, err := types.BuildIS04Resources(h.NodeName, domain, h.Cache.GetDomainFlows(h.DomainID), nmosVersion(h.now()))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error", err.Error())
		return types.ResourceSet{}, false
	}
	resources.Node.Href = fmt.Sprintf("http://%s/x-nmos/node/v1.3/", net.JoinHostPort(h.Host, fmt.Sprint(h.Port)))
	resources.Node.API.Endpoints = []types.Endpoint{{Host: h.Host, Port: h.Port, Protocol: "http"}}
	if len(resources.Devices) > 0 {
		resources.Devices[0].Controls = []types.Control{{
			Href: fmt.Sprintf("http://%s/x-nmos/connection/v1.1/", net.JoinHostPort(h.Host, fmt.Sprint(h.Port))),
			Type: "urn:x-nmos:control:sr-ctrl/v1.1",
		}}
	}
	return resources, true
}

func (h *HandlerEnv) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}
