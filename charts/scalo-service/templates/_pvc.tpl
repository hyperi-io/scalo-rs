{{/*
scalo-service.pvc -- a PersistentVolumeClaim <fullname>-<name> for each persistent
writable path without an existingClaim. The name carries no release or chart version, so
an upgrade keeps binding the same volume.
*/}}
{{- define "scalo-service.pvc" -}}
{{- $fullname := include "scalo-service.fullname" . -}}
{{- range $path := (include "scalo-service.writablePaths" . | fromJson).items }}
{{- if and $path.persistent (not $path.existingClaim) }}
{{- $spec := dict "accessModes" $path.accessModes "resources" (dict "requests" (dict "storage" $path.size)) -}}
{{- with $path.storageClass }}{{ $_ := set $spec "storageClassName" . }}{{ end }}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  {{- include "scalo-service.metadata" (dict "ctx" $ "name" (printf "%s-%s" $fullname $path.name) "annotations" $path.annotations) | nindent 2 }}
spec:
  {{- toYaml $spec | nindent 2 }}
{{- end }}
{{- end -}}
{{- end -}}
