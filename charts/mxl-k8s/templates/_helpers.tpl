{{/* vim: set filetype=mustache: */}}
{{/*
Chart name + version label.
*/}}
{{- define "mxlk8s.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
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
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
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
Per-component object labels = common labels + selector labels.
*/}}
{{- define "mxlk8s.componentLabels" -}}
{{ include "mxlk8s.labels" .Context }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/*
Resolve the image reference for one component. Digest beats tag. Tag
falls back to "v" prepended to chart appVersion, matching the
"v<VERSION>" tags that images.yml publishes to ghcr.io. An explicit
image.tag is used verbatim so digests, "pre", "latest", or
externally-mirrored tags pass through unchanged.

Call as: include "mxlk8s.image" (dict "global" .Values.global "image" .Values.operator.image "Chart" .Chart)
*/}}
{{- define "mxlk8s.image" -}}
{{- $registry := .global.image.registry -}}
{{- $repo := .image.repository -}}
{{- if .image.digest -}}
{{- printf "%s/%s@%s" $registry $repo .image.digest -}}
{{- else -}}
{{- $tag := default (printf "v%s" .Chart.AppVersion) .image.tag -}}
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
becomes --key (no value); boolean false omits the flag. Empty strings
omit the flag. Lists join on commas. Keys render in lexical order so
diff-on-rerender is stable.

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
    {{- if ne $v "" -}}
      {{- $lines = append $lines (printf "- --%s=%s" $kebab $v) -}}
    {{- end -}}
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
