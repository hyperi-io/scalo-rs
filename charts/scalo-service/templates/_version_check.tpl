{{/*
scalo-service.versionCheckEnv -- <env_prefix>_VERSION_CHECK__* overrides as
{"items": [...]}, each only when its versionCheck value is set, so an unset block leaves
the app's own default alone. An empty env_prefix writes the bare names.
*/}}
{{- define "scalo-service.versionCheckEnv" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $prefix := "" -}}
{{- with $contract.env_prefix }}{{ $prefix = printf "%s_" . }}{{ end -}}
{{- $check := .Values.versionCheck | default dict -}}
{{- $items := list -}}
{{- if hasKey $check "enabled" -}}
{{- $items = append $items (dict "name" (printf "%sVERSION_CHECK__ENABLED" $prefix) "value" (toString $check.enabled)) -}}
{{- end -}}
{{- if hasKey $check "sendInstanceId" -}}
{{- $items = append $items (dict "name" (printf "%sVERSION_CHECK__SEND_INSTANCE_ID" $prefix) "value" (toString $check.sendInstanceId)) -}}
{{- end -}}
{{- with $check.apiUrl -}}
{{- $items = append $items (dict "name" (printf "%sVERSION_CHECK__API_URL" $prefix) "value" (toString .)) -}}
{{- end -}}
{{- with $check.instanceId -}}
{{- $items = append $items (dict "name" (printf "%sVERSION_CHECK__INSTANCE_ID" $prefix) "value" (toString .)) -}}
{{- end -}}
{{- dict "items" $items | toJson -}}
{{- end -}}
