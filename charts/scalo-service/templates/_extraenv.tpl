{{/*
scalo-service.extraEnv -- the `extraEnv` map as env entries in name order, as
{"items": [...]}, minus any name the chart derives itself, so one name never appears
twice in a container. A scalar value is rendered through `tpl`; a map is a `valueFrom`,
also through `tpl`. Takes (dict "ctx" . "derived" LIST-OF-NAMES).
*/}}
{{- define "scalo-service.extraEnv" -}}
{{- $ctx := .ctx -}}
{{- $derived := .derived | default list -}}
{{- $items := list -}}
{{- range $name, $value := $ctx.Values.extraEnv | default dict -}}
{{- if not (has $name $derived) -}}
{{- if kindIs "map" $value -}}
{{- $items = append $items (dict "name" $name "valueFrom" (tpl (toYaml $value.valueFrom) $ctx | fromYaml)) -}}
{{- else -}}
{{- $items = append $items (dict "name" $name "value" (tpl (toString $value) $ctx)) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- dict "items" $items | toJson -}}
{{- end -}}
