{{/*
scalo-service.strategy -- the Deployment strategy as JSON: `strategy` from values, else
Recreate for a singleton or a pod holding a claim, which cannot run beside its
replacement, else surge-first, so the new pod passes readiness before the old one goes.
*/}}
{{- define "scalo-service.strategy" -}}
{{- if .Values.strategy -}}
{{- toJson .Values.strategy -}}
{{- else if or (include "scalo-service.singleton" .) (include "scalo-service.persistent" .) -}}
{{- dict "type" "Recreate" | toJson -}}
{{- else -}}
{{- dict "type" "RollingUpdate" "rollingUpdate" (dict "maxUnavailable" 0 "maxSurge" 1) | toJson -}}
{{- end -}}
{{- end -}}
