{{/*
scalo-service.tokenMounted -- "true" when serviceAccount.mountToken is on. Off by
default: the app calls no Kubernetes API unless it says so.
*/}}
{{- define "scalo-service.tokenMounted" -}}
{{- $sa := .Values.serviceAccount | default dict -}}
{{- if $sa.mountToken }}true{{ end -}}
{{- end -}}

{{/*
scalo-service.podNamespaceEnv -- POD_NAMESPACE from the downward API, as JSON; a pod
without a mounted token has no other way to learn its namespace.
*/}}
{{- define "scalo-service.podNamespaceEnv" -}}
{{- dict "name" "POD_NAMESPACE" "valueFrom" (dict "fieldRef" (dict "fieldPath" "metadata.namespace")) | toJson -}}
{{- end -}}

{{/*
scalo-service.serviceAccountFiles -- ca.crt and namespace at the service-account path with
no token, as {"volumes": [...], "mounts": [...]}. Empty while the token is mounted,
because the automount puts the same files at the same path.
*/}}
{{- define "scalo-service.serviceAccountFiles" -}}
{{- $out := dict "volumes" list "mounts" list -}}
{{- if not (include "scalo-service.tokenMounted" .) -}}
{{- $sources := list (dict "configMap" (dict "name" "kube-root-ca.crt" "items" (list (dict "key" "ca.crt" "path" "ca.crt")))) (dict "downwardAPI" (dict "items" (list (dict "path" "namespace" "fieldRef" (dict "fieldPath" "metadata.namespace"))))) -}}
{{- $_ := set $out "volumes" (list (dict "name" "serviceaccount-files" "projected" (dict "sources" $sources))) -}}
{{- $_ = set $out "mounts" (list (dict "name" "serviceaccount-files" "mountPath" "/var/run/secrets/kubernetes.io/serviceaccount" "readOnly" true)) -}}
{{- end -}}
{{- toJson $out -}}
{{- end -}}
