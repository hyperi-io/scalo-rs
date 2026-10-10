{{/*
scalo-service.serviceAccountCreated -- "true" when the chart creates the pod's
ServiceAccount: serviceAccount.create when values set it, else unless the contract's
service_account is none. Empty otherwise.
*/}}
{{- define "scalo-service.serviceAccountCreated" -}}
{{- $sa := .Values.serviceAccount | default dict -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $mode := $contract.service_account | default "own" -}}
{{- if not (has $mode (list "own" "none")) -}}
{{- fail (printf "scalo-service: files/contract.json has service_account %v, and this library reads own and none" $mode) -}}
{{- end -}}
{{- $create := eq $mode "own" -}}
{{- if hasKey $sa "create" }}{{ $create = $sa.create }}{{ end -}}
{{- if $create }}true{{ end -}}
{{- end -}}

{{/*
scalo-service.podServiceAccountName -- the account the pod runs as: the one the chart
creates, else serviceAccount.name. Empty when the chart creates none and values name
none, so the pod runs as the namespace's default account instead of naming one that
does not exist.
*/}}
{{- define "scalo-service.podServiceAccountName" -}}
{{- $sa := .Values.serviceAccount | default dict -}}
{{- if include "scalo-service.serviceAccountCreated" . -}}
{{- include "scalo-service.serviceAccountName" . -}}
{{- else -}}
{{- $sa.name | default "" -}}
{{- end -}}
{{- end -}}

{{/*
scalo-service.serviceaccount -- the pod's ServiceAccount when the chart creates one,
with its token unmounted unless serviceAccount.mountToken is on, so an account the
operator supplies and this one follow the same setting.
*/}}
{{- define "scalo-service.serviceaccount" -}}
{{- $sa := .Values.serviceAccount | default dict -}}
{{- if include "scalo-service.serviceAccountCreated" . -}}
apiVersion: v1
kind: ServiceAccount
metadata:
  {{- include "scalo-service.metadata" (dict "ctx" . "name" (include "scalo-service.serviceAccountName" .) "annotations" $sa.annotations) | nindent 2 }}
automountServiceAccountToken: {{ not (empty (include "scalo-service.tokenMounted" .)) }}
{{- end -}}
{{- end -}}
