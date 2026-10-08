{{/*
scalo-service.configmap -- <fullname>-config holding the app's config file, named after
the contract's config_mount_path; nothing when the contract names no config file.
*/}}
{{- define "scalo-service.configmap" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- with $contract.config_mount_path -}}
apiVersion: v1
kind: ConfigMap
metadata:
  {{- include "scalo-service.metadata" (dict "ctx" $ "name" (printf "%s-config" (include "scalo-service.fullname" $))) | nindent 2 }}
data:
  {{- dict (base .) (include "scalo-service.configFile" $) | toYaml | nindent 2 }}
{{- end -}}
{{- end -}}

{{/*
scalo-service.fileSets -- one ConfigMap <fullname>-<set> per fileSets entry, each file
keyed by name. The data goes through toYaml, so a file's indentation and line endings
survive intact.
*/}}
{{- define "scalo-service.fileSets" -}}
{{- $fullname := include "scalo-service.fullname" . -}}
{{- range $name, $set := .Values.fileSets | default dict }}
{{- $data := dict -}}
{{- range $file := $set.files | default list -}}
{{- $_ := set $data $file.name (toString $file.content) -}}
{{- end }}
---
apiVersion: v1
kind: ConfigMap
metadata:
  {{- include "scalo-service.metadata" (dict "ctx" $ "name" (printf "%s-%s" $fullname $name)) | nindent 2 }}
{{- with $data }}
data:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}
{{- end -}}
