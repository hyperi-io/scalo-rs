{{/*
scalo-service.kedaEnabled -- "true" when KEDA scales the workload: keda.enabled from
values, else the contract's keda.enabled. A singleton never scales, and asking it to
fails the render.
*/}}
{{- define "scalo-service.kedaEnabled" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $keda := .Values.keda | default dict -}}
{{- $on := false -}}
{{- if $contract.keda -}}
{{- $on = true -}}
{{- if hasKey $contract.keda "enabled" -}}{{- $on = $contract.keda.enabled -}}{{- end -}}
{{- end -}}
{{- if hasKey $keda "enabled" -}}{{- $on = $keda.enabled -}}{{- end -}}
{{- if include "scalo-service.singleton" . -}}
{{- if and (hasKey $keda "enabled") $keda.enabled -}}
{{- fail "scalo-service: keda.enabled is set, and the contract marks this app a singleton that runs exactly one pod" -}}
{{- end -}}
{{- $on = false -}}
{{- end -}}
{{- if $on }}true{{ end -}}
{{- end -}}

{{/*
scalo-service.hpaEnabled -- "true" when the fallback HorizontalPodAutoscaler scales the
workload: autoscaling.enabled with KEDA off. A singleton refuses it.
*/}}
{{- define "scalo-service.hpaEnabled" -}}
{{- $hpa := .Values.autoscaling | default dict -}}
{{- if $hpa.enabled -}}
{{- if include "scalo-service.singleton" . -}}
{{- fail "scalo-service: autoscaling.enabled is set, and the contract marks this app a singleton that runs exactly one pod" -}}
{{- end -}}
{{- if not (include "scalo-service.kedaEnabled" .) }}true{{ end -}}
{{- end -}}
{{- end -}}

{{/*
scalo-service.replicas -- the replica count while no autoscaler owns it, so a GitOps sync
never reverts a scale; empty while one does. A singleton is pinned at one.
*/}}
{{- define "scalo-service.replicas" -}}
{{- if include "scalo-service.singleton" . -}}
1
{{- else if not (or (include "scalo-service.kedaEnabled" .) (include "scalo-service.hpaEnabled" .)) -}}
{{- .Values.replicaCount | default 1 | int -}}
{{- end -}}
{{- end -}}
