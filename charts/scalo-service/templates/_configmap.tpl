{{/*
scalo-service.configmap -- <fullname>-config holding the app's config file under
binaryData, named after the contract's config_mount_path, or nothing when the contract
names no config file. Helm deletes the text `<no value>` from everything it renders, so
the file is base64-encoded to reach the pod unchanged.
*/}}
{{- define "scalo-service.configmap" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- with $contract.config_mount_path -}}
apiVersion: v1
kind: ConfigMap
metadata:
  {{- include "scalo-service.metadata" (dict "ctx" $ "name" (printf "%s-config" (include "scalo-service.fullname" $))) | nindent 2 }}
binaryData:
  {{- dict (base .) (include "scalo-service.configFile" $ | b64enc) | toYaml | nindent 2 }}
{{- end -}}
{{- end -}}

{{/*
scalo-service.fileSets -- one ConfigMap <fullname>-<set> per fileSets entry, each file
keyed by name under binaryData, base64-encoded for the same reason as the config file.
*/}}
{{- define "scalo-service.fileSets" -}}
{{- $fullname := include "scalo-service.fullname" . -}}
{{- range $name, $set := .Values.fileSets | default dict }}
{{- $data := dict -}}
{{- range $file := $set.files | default list -}}
{{- $_ := set $data $file.name (toString $file.content | b64enc) -}}
{{- end }}
---
apiVersion: v1
kind: ConfigMap
metadata:
  {{- include "scalo-service.metadata" (dict "ctx" $ "name" (printf "%s-%s" $fullname $name)) | nindent 2 }}
{{- with $data }}
binaryData:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}
{{- end -}}
