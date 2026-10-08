{{/*
scalo-service.reloadAnnotations -- Stakater Reloader annotations, so a rotated Secret
reaches a running pod; env read from a Secret resolves only at pod start. Off unless
reload.enabled.
*/}}
{{- define "scalo-service.reloadAnnotations" -}}
{{- $reload := .Values.reload | default dict -}}
{{- $out := dict -}}
{{- if $reload.enabled -}}
{{- $_ := set $out "reloader.stakater.com/auto" "true" -}}
{{- with $reload.secrets -}}{{- $_ = set $out "secret.reloader.stakater.com/reload" (join "," .) -}}{{- end -}}
{{- with $reload.configmaps -}}{{- $_ = set $out "configmap.reloader.stakater.com/reload" (join "," .) -}}{{- end -}}
{{- end -}}
{{- toJson $out -}}
{{- end -}}
