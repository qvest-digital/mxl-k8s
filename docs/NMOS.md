# NMOS Sender Proxy

The mxl-k8s agent can serve as an AMWA NMOS IS-04 Node and IS-05
Connection Management sender proxy for MXL flows. This makes MXL
flows discoverable and controllable by NMOS controllers that
implement [BCP-007-03][bcp-007-03], the MXL transport binding for
NMOS.

The proxy is opt-in: it starts only when the agent is launched with
`--nmos-bind-address`. When enabled, the agent serves IS-04 Node API
v1.3 and IS-05 Connection Management v1.1 on the same HTTP listener.

## Architecture

```
  NMOS controller                mxl-k8s agent
  --------------                --------------------------------
                                +-------------------+
  IS-04 discovery ----HTTP--->  |  NMOS HTTP server |
  IS-05 connection              |  /x-nmos/node/    |
                                |  /x-nmos/         |
                                |   connection/     |
                                +--------+----------+
                                         |
                                +--------v----------+
                                |  CRD watcher      |
                                |  (in-memory cache) |
                                +--------+----------+
                                         |
                          +--------------+--------------+
                          |                             |
                   +------v------+               +-----v-----+
                   | MxlDomain   |               | MxlFlow   |
                   | CRDs        |               | CRDs      |
                   | (cluster)   |               | (cluster) |
                   +-------------+               +-----------+
```

The agent's CRD watcher maintains an in-memory cache of `MxlDomain`
and `MxlFlow` resources via Kubernetes watches. On each HTTP request,
the NMOS server reads the cache and derives IS-04 resources on the
fly. There is no persistent NMOS state: the Node API responses are a
live projection of the cluster's CRD state.

### Data flow

1. A media function writes a flow into `/run/mxl/domain`.
2. The agent's `fanotify` watcher detects the new flow directory and
   creates an `MxlFlow` CRD with the flow's definition (an
   NMOS-shaped JSON document) and the origin node in
   `status.locations`.
3. The agent ensures an `MxlDomain` CRD exists for its node, carrying
   the node name and domain host path.
4. The NMOS CRD watcher caches both resource types.
5. When a controller queries the IS-04 Node API, the server calls
   `types.BuildIS04Resources` to derive the Node, Device, Source,
   Flow, and Sender resources from the cached CRDs.
6. IS-05 Connection Management endpoints return the active transport
   parameters (`mxl_domain_id`, `mxl_flow_id`) for each sender.

## IS-04 Node API v1.3

The server implements the following endpoints under
`/x-nmos/node/`:

| Endpoint | Response |
| --- | --- |
| `GET /x-nmos/node/` | Version listing: `["v1.3/"]` |
| `GET /x-nmos/node/v1.3/` | Node resource (self) |
| `GET /x-nmos/node/v1.3/self` | Node resource (alias) |
| `GET /x-nmos/node/v1.3/devices` | Device array |
| `GET /x-nmos/node/v1.3/sources` | Source array |
| `GET /x-nmos/node/v1.3/flows` | Flow array |
| `GET /x-nmos/node/v1.3/senders` | Sender array |
| `GET /x-nmos/node/v1.3/receivers` | Empty array (senders only) |

### Resources

**Node**: One per Kubernetes node running the agent. The `href`
and `api.endpoints` fields carry the advertise host and port
derived from the `--nmos-bind-address` flag.

**Device**: One per MXL domain. Type is
`urn:x-nmos:device:generic`. The `senders` array lists the
deterministic sender IDs for flows whose origin is on this node.

**Source**: One per MXL flow with origin on this node. The format
(`urn:x-nmos:format:video`, `urn:x-nmos:format:audio`, etc.) and
other fields come from the `MxlFlow.spec.definition` JSON.

**Flow**: One per MXL flow with origin on this node. The flow
definition is the verbatim JSON from `MxlFlow.spec.definition`,
which is already NMOS IS-04 shaped. Fields not explicitly modeled
in the Go struct (`frame_width`, `grain_rate`, `components`, etc.)
are preserved through a custom `MarshalJSON` / `UnmarshalJSON`
pair.

**Sender**: One per MXL flow with origin on this node. Transport
is `urn:x-nmos:transport:mxl`. The `subscription` block reports
`active: true` because MXL senders are always live once the flow
exists on the domain.

## IS-05 Connection Management v1.1

The server implements sender-only endpoints under
`/x-nmos/connection/`:

| Endpoint | Response |
| --- | --- |
| `GET /x-nmos/connection/` | Version listing: `["v1.1/"]` |
| `GET /x-nmos/connection/v1.1/` | Version listing (alias) |
| `GET .../v1.1/single/senders/{senderID}/active` | Active transport state |
| `GET .../v1.1/single/senders/{senderID}/staged` | Staged state (read-only) |
| `PATCH .../v1.1/single/senders/{senderID}/staged` | Accepted, returns active state |
| `GET .../v1.1/single/senders/{senderID}/constraints` | Parameter constraints |
| `GET .../v1.1/single/senders/{senderID}/transportfile` | Always `404` (BCP-007-03) |

### Sender state

MXL senders are always active. The `activation.mode` is
`activate_immediate` with an `activation_time` timestamp in NMOS
TAI format (`YYYY-MM-DDTHH:MM:SS.sssssssssZ`).

The `transport_params` array has one leg with:

- `mxl_domain_id`: the MXL domain identifier (the `MxlDomain` CRD
  name)
- `mxl_flow_id`: the MXL flow UUID from `MxlFlow.spec.id`

The `staged` endpoint is read-only: `PATCH` requests are accepted
for controller compatibility but always return the current active
transport parameters. MXL does not support pre-staging transport
changes; the flow is either present on the domain or not.

The `constraints` endpoint returns enum constraints that restrict
`mxl_domain_id` and `mxl_flow_id` to the single concrete value
each sender currently exposes. This tells controllers there is no
parameter flexibility to negotiate.

The `active` and `staged` responses carry the IS-05 sender
connection resource fields. In addition to `transport_params`,
each response includes `sender_id` (the IS-04 sender UUID) and a
`transport_file` object whose `data` is the MXL transport
descriptor JSON (`{"mxl_domain_id":"...","mxl_flow_id":"..."}`)
with `type` `application/json`:

```json
{
  "sender_id": "5fbec3b1-1b0f-417d-9059-8b94a47197ed",
  "receiver_id": null,
  "master_enable": true,
  "activation": {
    "mode": "activate_immediate",
    "requested_time": null,
    "activation_time": "2026-06-24T12:00:37.000000000Z"
  },
  "transport_file": {
    "data": "{\"mxl_domain_id\":\"node-a\",\"mxl_flow_id\":\"5fbec3b1-1b0f-417d-9059-8b94a47197ed\"}",
    "type": "application/json"
  },
  "transport_params": [
    {
      "mxl_domain_id": "node-a",
      "mxl_flow_id": "5fbec3b1-1b0f-417d-9059-8b94a47197ed"
    }
  ]
}
```

The `transportfile` endpoint always returns `404`. Per
[BCP-007-03][bcp-007-03], an MXL IS-05 Sender's `/transportfile`
endpoint MUST always return a 404; MXL transport parameters are
conveyed via the `active`/`staged` resources and the
`transport_file` field above, not via a separate transport file.

## ID generation

All NMOS resource IDs are deterministic UUID v5 values, derived
from a fixed namespace UUID
(`1ee6477a-e0c7-5b1c-8af3-7f29e2a5444f`) and a colon-joined
string of identifying parts:

| Resource | ID input string |
| --- | --- |
| Node | `node:<nodeName>` |
| Device | `device:<nodeName>:<domainName>:<hostPath>` |
| Source | `source:<nodeName>:<flowID>` |
| Sender | `sender:<nodeName>:<flowID>` |

The flow ID itself comes from the flow definition JSON (`id`
field) or, if absent, from `MxlFlow.spec.id`. Because the inputs
are stable (node name, domain name, flow UUID), the same cluster
state always produces the same NMOS IDs across restarts.

## MXL domain identity

The `MxlDomain` CRD carries the node name and the host path of the
domain directory (`/run/mxl/domain` by default). The agent creates
this CRD on startup via `domain_publisher.EnsureExists`. The
domain CRD name is used as the `mxl_domain_id` in IS-05 transport
parameters.

## Configuration

NMOS is enabled by passing `--nmos-bind-address` to the agent. An
empty value (the default) disables the NMOS server entirely.

In the Helm chart, set the flag under `agent.flags`:

```yaml
agent:
  flags:
    nmosBindAddress: ":1080"
```

The address `:1080` listens on all interfaces on port 1080. For a
loopback-only bind, use `127.0.0.1:1080`.

The advertise host and port embedded in the Node resource's `href`
and `api.endpoints` are derived from the bind address. If the host
part is empty, `0.0.0.0`, or `::`, the agent falls back to the
Kubernetes node name.

## BCP-007-03 compliance

The implementation follows [BCP-007-03][bcp-007-03], the AMWA
best-practice document that defines the MXL transport binding for
NMOS. Key aspects:

- Transport type `urn:x-nmos:transport:mxl` on all senders.
- IS-05 transport parameters `mxl_domain_id` and `mxl_flow_id`
  carry the MXL domain and flow identifiers.
- The sender `/transportfile` endpoint always returns `404`, as
  required by BCP-007-03 for MXL IS-05 senders; the transport
  descriptor is carried by the `transport_file` field of the
  `active`/`staged` resources instead.
- Senders are always active (`activate_immediate`), reflecting
  that MXL flows are live once the flow directory exists on the
  domain.
- The `staged` endpoint accepts PATCH for controller compatibility
  but is effectively read-only, since MXL does not support
  pre-staging transport parameter changes.

For full BCP-007-03 compliance testing, the [nmos-testing][nmos-testing]
suite includes a BCP-007-03 test suite. See [KIND.md](./KIND.md) for
manual verification steps against a local cluster.

## Source layout

```
agent/internal/nmos/
  types/types.go       IS-04 / IS-05 Go types, BuildIS04Resources
  server/server.go     HTTP server, options, lifecycle
  server/router.go     Route registration (Go 1.22 ServeMux)
  server/handlers.go   Request handlers
  server/middleware.go CORS, logging, recovery, content-type
  watcher/watcher.go   CRD watcher with in-memory cache
```

[bcp-007-03]: https://specs.amwa.tv/bcp-007-03/
[nmos-testing]: https://github.com/AMWA-TV/nmos-testing
