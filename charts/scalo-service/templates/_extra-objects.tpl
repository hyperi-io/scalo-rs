{{/*
scalo-service.extraObjects -- each `extraObjects` entry as its own document, through tpl,
for the objects a deployment adds beside the service. An entry is a map or a string.
*/}}
{{- define "scalo-service.extraObjects" -}}
{{- range .Values.extraObjects | default list }}
---
{{ if kindIs "string" . }}{{ tpl . $ }}{{ else }}{{ tpl (toYaml .) $ }}{{ end }}
{{- end -}}
{{- end -}}
