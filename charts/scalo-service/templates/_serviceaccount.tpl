{{/*
scalo-service.serviceaccount -- the pod's ServiceAccount unless serviceAccount.create is
false, with its token unmounted unless serviceAccount.mountToken is on, so an account
the operator supplies and this one follow the same setting.
*/}}
{{- define "scalo-service.serviceaccount" -}}
{{- $sa := .Values.serviceAccount | default dict -}}
{{- $create := true -}}
{{- if hasKey $sa "create" }}{{ $create = $sa.create }}{{ end -}}
{{- if $create -}}
apiVersion: v1
kind: ServiceAccount
metadata:
  {{- include "scalo-service.metadata" (dict "ctx" . "name" (include "scalo-service.serviceAccountName" .) "annotations" $sa.annotations) | nindent 2 }}
automountServiceAccountToken: {{ not (empty (include "scalo-service.tokenMounted" .)) }}
{{- end -}}
{{- end -}}
