{{/*
scalo-service.selectorLabelSet -- app.kubernetes.io/name ONLY, as JSON. A Deployment
selector is immutable, so nothing else may ever enter it.
*/}}
{{- define "scalo-service.selectorLabelSet" -}}
{{- dict "app.kubernetes.io/name" (include "scalo-service.fullname" .) | toJson -}}
{{- end -}}

{{/*
scalo-service.selectorLabels -- the selector label set as YAML.
*/}}
{{- define "scalo-service.selectorLabels" -}}
{{- include "scalo-service.selectorLabelSet" . | fromJson | toYaml -}}
{{- end -}}

{{/*
scalo-service.labelSet -- the standard labels over `commonLabels`, as JSON; the standard
keys win a collision so commonLabels cannot rename the workload.
*/}}
{{- define "scalo-service.labelSet" -}}
{{- $labels := dict -}}
{{- range $key, $value := .Values.commonLabels | default dict -}}
{{- $_ := set $labels $key (toString $value) -}}
{{- end -}}
{{- $_ := set $labels "app.kubernetes.io/name" (include "scalo-service.fullname" .) -}}
{{- $_ = set $labels "app.kubernetes.io/instance" .Release.Name -}}
{{- $_ = set $labels "app.kubernetes.io/version" (toString (.Chart.AppVersion | default .Chart.Version)) -}}
{{- $_ = set $labels "app.kubernetes.io/managed-by" .Release.Service -}}
{{- $_ = set $labels "helm.sh/chart" (printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-") -}}
{{- with .Values.partOf -}}{{- $_ = set $labels "app.kubernetes.io/part-of" (toString .) -}}{{- end -}}
{{- toJson $labels -}}
{{- end -}}

{{/*
scalo-service.podLabelSet -- the pod template's labels as JSON: the label set and
`podLabels`, with the selector label set last so the pods stay selected.
*/}}
{{- define "scalo-service.podLabelSet" -}}
{{- $labels := include "scalo-service.labelSet" . | fromJson -}}
{{- range $key, $value := .Values.podLabels | default dict -}}
{{- $_ := set $labels $key (toString $value) -}}
{{- end -}}
{{- $_ := set $labels "app.kubernetes.io/name" (include "scalo-service.fullname" .) -}}
{{- toJson $labels -}}
{{- end -}}

{{/*
scalo-service.metadata -- an object's name, release namespace, labels and annotations, with
`commonAnnotations` under the object's own. Takes (dict "ctx" . "name" NAME "annotations" MAP).
*/}}
{{- define "scalo-service.metadata" -}}
{{- $annotations := deepCopy (.ctx.Values.commonAnnotations | default dict) -}}
{{- $annotations = mergeOverwrite $annotations (deepCopy (.annotations | default dict)) -}}
{{- $metadata := dict "name" .name "namespace" .ctx.Release.Namespace "labels" (include "scalo-service.labelSet" .ctx | fromJson) -}}
{{- with $annotations }}{{ $_ := set $metadata "annotations" . }}{{ end -}}
{{- toYaml $metadata -}}
{{- end -}}
