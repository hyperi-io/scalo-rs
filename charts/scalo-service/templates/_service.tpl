{{/*
scalo-service.servicePorts -- Service port entries targeting the container ports by name,
as {"items": [...]}. Takes (dict "ports" LIST).
*/}}
{{- define "scalo-service.servicePorts" -}}
{{- $items := list -}}
{{- range .ports -}}
{{- $entry := dict "name" .name "port" (int .port) "targetPort" .name "protocol" .protocol -}}
{{- with .appProtocol }}{{ $_ := set $entry "appProtocol" . }}{{ end -}}
{{- $items = append $items $entry -}}
{{- end -}}
{{- dict "items" $items | toJson -}}
{{- end -}}

{{/*
scalo-service.service -- the in-cluster Service <fullname> on the metrics port and every
port the app listens on.
*/}}
{{- define "scalo-service.service" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $service := .Values.service | default dict -}}
{{- $metrics := dict "name" "metrics" "port" (int $contract.metrics_port) "protocol" "TCP" -}}
{{- $ports := prepend (include "scalo-service.ports" . | fromJson).items $metrics -}}
{{- $spec := dict "type" ($service.type | default "ClusterIP") "selector" (include "scalo-service.selectorLabelSet" . | fromJson) -}}
{{- $_ := set $spec "ports" (include "scalo-service.servicePorts" (dict "ports" $ports) | fromJson).items -}}
apiVersion: v1
kind: Service
metadata:
  {{- include "scalo-service.metadata" (dict "ctx" . "name" (include "scalo-service.fullname" .) "annotations" $service.annotations) | nindent 2 }}
spec:
  {{- toYaml $spec | nindent 2 }}
{{- end -}}

{{/*
scalo-service.publicService -- with publicService.enabled, a load balancer for the ports
the contract marks public: <fullname>-public for TCP and SCTP, <fullname>-public-udp for
UDP, since many load balancers take one protocol each. Exposure is a deployment choice,
so it is off by default.
*/}}
{{- define "scalo-service.publicService" -}}
{{- $public := .Values.publicService | default dict -}}
{{- if $public.enabled -}}
{{- $groups := dict "public" list "public-udp" list -}}
{{- range (include "scalo-service.ports" . | fromJson).items -}}
{{- if .public -}}
{{- $suffix := ternary "public-udp" "public" (eq .protocol "UDP") -}}
{{- $_ := set $groups $suffix (append (get $groups $suffix) .) -}}
{{- end -}}
{{- end -}}
{{- if not (or (get $groups "public") (get $groups "public-udp")) -}}
{{- fail "scalo-service: publicService.enabled is set, and the contract marks no port public" -}}
{{- end -}}
{{- range $suffix := list "public" "public-udp" }}
{{- with get $groups $suffix }}
{{- $spec := dict "type" ($public.type | default "LoadBalancer") "selector" (include "scalo-service.selectorLabelSet" $ | fromJson) -}}
{{- range $key := list "loadBalancerClass" "loadBalancerIP" "loadBalancerSourceRanges" "externalTrafficPolicy" -}}
{{- with get $public $key }}{{ $_ := set $spec $key . }}{{ end -}}
{{- end -}}
{{- $_ := set $spec "ports" (include "scalo-service.servicePorts" (dict "ports" .) | fromJson).items }}
---
apiVersion: v1
kind: Service
metadata:
  {{- include "scalo-service.metadata" (dict "ctx" $ "name" (printf "%s-%s" (include "scalo-service.fullname" $) $suffix) "annotations" $public.annotations) | nindent 2 }}
spec:
  {{- toYaml $spec | nindent 2 }}
{{- end }}
{{- end }}
{{- end -}}
{{- end -}}
