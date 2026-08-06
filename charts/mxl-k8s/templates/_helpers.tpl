{{/* vim: set filetype=mustache: */}}
{{/*
Chart name + version label.
*/}}
{{- define "mxlk8s.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Sanitised app.kubernetes.io/version. K8s label values forbid '+',
which is legal in semver build-metadata; collapse to '_' to match
mxlk8s.chart's existing convention.
Call as: include "mxlk8s.appVersion" .Chart
*/}}
{{- define "mxlk8s.appVersion" -}}
{{- .AppVersion | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels applied to every object.
*/}}
{{- define "mxlk8s.labels" -}}
helm.sh/chart: {{ include "mxlk8s.chart" . }}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ include "mxlk8s.appVersion" .Chart | quote }}
{{- end }}
{{- with .Values.global.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
Per-component selector labels.
Call as: include "mxlk8s.selectorLabels" (dict "Context" . "component" "operator")
*/}}
{{- define "mxlk8s.selectorLabels" -}}
app.kubernetes.io/name: {{ printf "%s-%s" .Context.Chart.Name .component }}
app.kubernetes.io/instance: {{ .Context.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/*
Per-component object labels. Composes the chart-identity labels with
mxlk8s.selectorLabels so spec.template.metadata.labels and
spec.selector.matchLabels agree on app.kubernetes.io/name; mismatch
makes the apiserver reject the workload at install time.
*/}}
{{- define "mxlk8s.componentLabels" -}}
helm.sh/chart: {{ include "mxlk8s.chart" .Context }}
app.kubernetes.io/managed-by: {{ .Context.Release.Service }}
{{- if .Context.Chart.AppVersion }}
app.kubernetes.io/version: {{ include "mxlk8s.appVersion" .Context.Chart | quote }}
{{- end }}
{{ include "mxlk8s.selectorLabels" . }}
{{- with .Context.Values.global.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
Resolve the image reference for one component. Digest beats tag. An
explicit image.tag is used verbatim so digests, "pre", "latest", or
externally-mirrored tags pass through unchanged. With neither set, the
tag falls back to the chart appVersion prefixed with "v".

operator, agent, gateway and shim are one release-please package
together with this chart, so a chart at version X always bundles the
images published as vX. Deriving the default from appVersion rather
than carrying a pin per component is what keeps the two from drifting:
there is no second place to update, and no window in which the chart
names an image the release did not build.

Call as: include "mxlk8s.image" (dict "Context" . "image" .Values.operator.image)
*/}}
{{- define "mxlk8s.image" -}}
{{- $registry := .Context.Values.global.image.registry -}}
{{- $repo := .image.repository -}}
{{- if .image.digest -}}
{{- printf "%s/%s@%s" $registry $repo .image.digest -}}
{{- else -}}
{{- $default := printf "v%s" (required "chart appVersion is empty; set <component>.image.tag or .image.digest" .Context.Chart.AppVersion) -}}
{{- $tag := default $default .image.tag -}}
{{- printf "%s/%s:%s" $registry $repo $tag -}}
{{- end -}}
{{- end -}}

{{/*
Resolve the imagePullPolicy for one component, falling back to global.
*/}}
{{- define "mxlk8s.imagePullPolicy" -}}
{{- if .image.pullPolicy -}}{{ .image.pullPolicy }}{{- else -}}{{ .global.image.pullPolicy }}{{- end -}}
{{- end -}}

{{/*
ServiceAccount name for one component.
Call as: include "mxlk8s.serviceAccountName" (dict "Context" . "component" "operator" "values" .Values.operator)
*/}}
{{- define "mxlk8s.serviceAccountName" -}}
{{- if .values.serviceAccount.name -}}
{{ .values.serviceAccount.name }}
{{- else -}}
{{- printf "%s-%s" .Context.Chart.Name .component -}}
{{- end -}}
{{- end -}}

{{/*
Render a flags map as a sorted list of --key=value args. Boolean true
becomes --key (no value); boolean false omits the flag. An explicit
empty string emits --key= (bare equals) so a caller can suppress a
binary's downward-API fallback (for example,
gateway.flags.bindAddress: "" turns off the POD_IP default and lets
libfabric pick the interface). An absent key (parent map set to {})
still omits the flag entirely. Lists join on commas. Keys render in
lexical order so diff-on-rerender is stable.

Call as: include "mxlk8s.flags.render" (dict "flags" .Values.operator.flags)
*/}}
{{- define "mxlk8s.flags.render" -}}
{{- $flags := .flags -}}
{{- $lines := list -}}
{{- range $k := (keys $flags | sortAlpha) -}}
  {{- $v := index $flags $k -}}
  {{- $kebab := kebabcase $k -}}
  {{- if kindIs "bool" $v -}}
    {{- if $v -}}
      {{- $lines = append $lines (printf "- --%s" $kebab) -}}
    {{- end -}}
  {{- else if kindIs "slice" $v -}}
    {{- if gt (len $v) 0 -}}
      {{- $lines = append $lines (printf "- --%s=%s" $kebab (join "," $v)) -}}
    {{- end -}}
  {{- else if eq (kindOf $v) "string" -}}
    {{- $lines = append $lines (printf "- --%s=%s" $kebab $v) -}}
  {{- else -}}
    {{- $lines = append $lines (printf "- --%s=%v" $kebab $v) -}}
  {{- end -}}
{{- end -}}
{{ join "\n" $lines }}
{{- end -}}

{{/*
Namespace where the chart's namespaced objects live. Honours the
chart's namespace.name override; falls back to .Release.Namespace.
*/}}
{{- define "mxlk8s.namespace" -}}
{{- if .Values.namespace.name -}}{{ .Values.namespace.name }}{{- else -}}{{ .Release.Namespace }}{{- end -}}
{{- end -}}

{{/*
Name of one gateway DaemonSet. An empty variant yields the historical
name, so an install that adopts variants creates newly-named workloads
beside the old one rather than patching a DaemonSet's immutable
spec.selector.
Call as: include "mxlk8s.gateway.name" (dict "Context" . "variant" "rdma")
*/}}
{{- define "mxlk8s.gateway.name" -}}
{{- if .variant -}}
{{- printf "%s-gateway-%s" .Context.Chart.Name .variant | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-gateway" .Context.Chart.Name -}}
{{- end -}}
{{- end -}}

{{/*
Selector label distinguishing one gateway variant from another. Two
DaemonSets whose spec.selector matched the same pods would each try to
own them, so every variant carries this and the unvariegated case
carries nothing at all.

Deliberately absent from mxlk8s.selectorLabels: the gateway Service and
ServiceMonitor select on those, and must keep matching every variant so
metrics stay aggregated across the node classes.
Call as: include "mxlk8s.gateway.variantLabel" "rdma"
*/}}
{{- define "mxlk8s.gateway.variantLabel" -}}
mxl.qvest-digital.com/gateway-variant: {{ . }}
{{- end -}}

{{/*
Reject variant lists that cannot hold the one-gateway-per-node
invariant. Nothing at render time can read node labels, so overlap is
not decidable here; what is decidable is a list that guarantees it.

Two gateways on one node open the same MXL domain, reconcile the same
mirrors, and write the same MxlNodeCapabilities, which is named after
the node.
Call as: include "mxlk8s.gateway.validateVariants" .Values.gateway.variants
*/}}
{{- define "mxlk8s.gateway.validateVariants" -}}
{{- $variants := . | default dict -}}
{{- $docs := include "mxlk8s.gateway.variantDocs" . -}}
{{- $enabled := dict -}}
{{- $placed := dict -}}
{{- range $name, $variant := $variants -}}
  {{- if not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" $name) -}}
    {{- fail (printf "gateway.variants: %q is not a DNS-1123 label; a variant key names a DaemonSet and labels its pods. See %s" $name $docs) -}}
  {{- end -}}
  {{- if $variant.enabled -}}
    {{- $_ := set $enabled $name $variant -}}
  {{- else if or $variant.nodeSelector $variant.affinity $variant.tolerations -}}
    {{- $_ := set $placed $name $variant -}}
  {{- end -}}
{{- end -}}
{{- /*
Placement on a variant nobody enabled is the shape a forgotten
`enabled: true` leaves behind, and it fails quietly: the chart falls
back to the single unplaced DaemonSet, which comes up healthy on every
node and silently omits the device request the placed nodes needed.
The variants the chart ships carry no placement, so an untouched
install cannot reach this.
*/ -}}
{{- if and (eq (len $enabled) 0) (gt (len $placed) 0) -}}
  {{- fail (printf "gateway.variants: %s %s placement but no variant is enabled, so one gateway renders on every node with none of it applied. Set `enabled: true` on the variants this cluster needs. See %s" (keys $placed | sortAlpha | join ", ") (ternary "carry" "carries" (gt (len $placed) 1)) $docs) -}}
{{- end -}}
{{- if gt (len $enabled) 1 -}}
  {{- $placements := dict -}}
  {{- range $name, $variant := $enabled -}}
    {{- $placement := printf "%v|%v" ($variant.nodeSelector | default dict) ($variant.affinity | default dict) -}}
    {{- if eq $placement "map[]|map[]" -}}
      {{- fail (printf "gateway.variants: %q sets neither nodeSelector nor affinity, so it lands on every node and overlaps its siblings. See %s" $name $docs) -}}
    {{- end -}}
    {{- if hasKey $placements $placement -}}
      {{- fail (printf "gateway.variants: %q places pods identically to %q; complementary placement is what keeps one gateway per node. See %s" $name (get $placements $placement) $docs) -}}
    {{- end -}}
    {{- $_ := set $placements $placement $name -}}
  {{- end -}}
{{- end -}}
{{- end -}}

{{/*
Where a failed variant validation sends the reader. Pinned to main
rather than to the installed chart's tag: a chart rendered from a
working tree has no tag to resolve, and a link that 404s is worse than
one pointing at docs slightly ahead of the release.
*/}}
{{- define "mxlk8s.gateway.variantDocs" -}}
https://github.com/qvest-digital/mxl-k8s/blob/main/docs/RDMA.md#clusters-whose-nodes-differ
{{- end -}}
