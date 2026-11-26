{{/*
scalo-service.extraEnv -- an env map (`extraEnv` or `integrationEnv`) as env entries in
name order, as {"items": [...]}, minus each name in `drop`, so one name never appears
twice in a container. A scalar value is rendered through `tpl`, and a map is a `valueFrom`,
also through `tpl`. Takes (dict "ctx" . "env" MAP "drop" LIST-OF-NAMES).
*/}}
{{- define "scalo-service.extraEnv" -}}
{{- $ctx := .ctx -}}
{{- $drop := .drop | default list -}}
{{- $items := list -}}
{{- range $name, $value := .env | default dict -}}
{{- if not (has $name $drop) -}}
{{- if kindIs "map" $value -}}
{{- $items = append $items (dict "name" $name "valueFrom" (tpl (toYaml $value.valueFrom) $ctx | fromYaml)) -}}
{{- else -}}
{{- $items = append $items (dict "name" $name "value" (tpl (toString $value) $ctx)) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- dict "items" $items | toJson -}}
{{- end -}}
