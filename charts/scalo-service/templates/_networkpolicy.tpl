{{/*
scalo-service.networkpolicy -- with networkPolicy.enabled, ingress to the pod only on the
ports it serves: metrics and private ports from networkPolicy.from (default: pods in the
namespace), public ports from networkPolicy.publicFrom (default: any source).
*/}}
{{- define "scalo-service.networkpolicy" -}}
{{- $policy := .Values.networkPolicy | default dict -}}
{{- if $policy.enabled -}}
{{- $private := list (dict "port" "metrics" "protocol" "TCP") -}}
{{- $public := list -}}
{{- range (include "scalo-service.ports" . | fromJson).items -}}
{{- $entry := dict "port" .name "protocol" .protocol -}}
{{- if .public }}{{ $public = append $public $entry }}{{ else }}{{ $private = append $private $entry }}{{ end -}}
{{- end -}}
{{- $from := $policy.from | default (list (dict "podSelector" dict)) -}}
{{- $rules := list (dict "from" (tpl (toYaml $from) . | fromYamlArray) "ports" $private) -}}
{{- if $public -}}
{{- $rule := dict "ports" $public -}}
{{- with $policy.publicFrom }}{{ $_ := set $rule "from" (tpl (toYaml .) $ | fromYamlArray) }}{{ end -}}
{{- $rules = append $rules $rule -}}
{{- end -}}
{{- $spec := dict "podSelector" (dict "matchLabels" (include "scalo-service.selectorLabelSet" . | fromJson)) "policyTypes" (list "Ingress") "ingress" $rules -}}
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  {{- include "scalo-service.metadata" (dict "ctx" . "name" (include "scalo-service.fullname" .)) | nindent 2 }}
spec:
  {{- toYaml $spec | nindent 2 }}
{{- end -}}
{{- end -}}
