{{/*
scalo-service.extraEnv -- the `extraEnv` map as env entries in name order, as
{"items": [...]}, minus any name the chart derives itself, so one name never appears
twice in a container. Takes (dict "ctx" . "derived" LIST-OF-NAMES).
*/}}
{{- define "scalo-service.extraEnv" -}}
{{- $derived := .derived | default list -}}
{{- $items := list -}}
{{- range $name, $value := .ctx.Values.extraEnv | default dict -}}
{{- if not (has $name $derived) -}}
{{- $items = append $items (dict "name" $name "value" (toString $value)) -}}
{{- end -}}
{{- end -}}
{{- dict "items" $items | toJson -}}
{{- end -}}
