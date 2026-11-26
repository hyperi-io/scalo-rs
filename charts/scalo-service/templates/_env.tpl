{{/*
scalo-service.secretEnv -- one secretKeyRef per variable of each contract secret group,
as {"items": [...]}. secrets.<group>.existingSecret names the Secret, through `tpl`
(default <fullname>-<group>), secrets.<group>.keys.<key_name> renames a key,
secrets.<group>.optional overrides the contract, and secrets.<group>.enabled: false
leaves the group out.
*/}}
{{- define "scalo-service.secretEnv" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $fullname := include "scalo-service.fullname" . -}}
{{- $values := .Values.secrets | default dict -}}
{{- $items := list -}}
{{- range $group := $contract.secrets | default list -}}
{{- $override := get $values $group.group_name | default dict -}}
{{- $enabled := true -}}
{{- if hasKey $override "enabled" }}{{ $enabled = $override.enabled }}{{ end -}}
{{- if $enabled -}}
{{- $optional := $group.optional | default false -}}
{{- if hasKey $override "optional" }}{{ $optional = $override.optional }}{{ end -}}
{{- $secret := printf "%s-%s" $fullname $group.group_name -}}
{{- with $override.existingSecret }}{{ $secret = tpl (toString .) $ }}{{ end -}}
{{- $keys := $override.keys | default dict -}}
{{- range $var := $group.env_vars -}}
{{- $ref := dict "name" $secret "key" (get $keys $var.key_name | default $var.secret_key) -}}
{{- if $optional }}{{ $_ := set $ref "optional" true }}{{ end -}}
{{- $items = append $items (dict "name" $var.env_var "valueFrom" (dict "secretKeyRef" $ref)) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- dict "items" $items | toJson -}}
{{- end -}}

{{/*
scalo-service.writableLimitsEnv -- SCALO_WRITABLE_PATH_LIMITS as {"items": [...]}: each
emptyDir writable path with a sizeLimit, as path=quantity joined by commas, so the app
caps what it writes there below the size kubelet evicts the pod at. No item when no
emptyDir path has a sizeLimit.
*/}}
{{- define "scalo-service.writableLimitsEnv" -}}
{{- $pairs := list -}}
{{- range $path := (include "scalo-service.writablePaths" . | fromJson).items -}}
{{- if and (not $path.persistent) $path.sizeLimit -}}
{{- $pairs = append $pairs (printf "%s=%s" $path.path $path.sizeLimit) -}}
{{- end -}}
{{- end -}}
{{- $items := list -}}
{{- if $pairs -}}
{{- $items = append $items (dict "name" "SCALO_WRITABLE_PATH_LIMITS" "value" (join "," $pairs)) -}}
{{- end -}}
{{- dict "items" $items | toJson -}}
{{- end -}}

{{/*
scalo-service.opensslEnv -- OPENSSL_CONF at the contract's openssl_conf (default
/tmp/scalo-openssl.cnf), the file a Kafka client writes its OpenSSL TLS floor into, as
{"items": [...]}. No item when openssl_conf is empty, or when extraEnv or integrationEnv
sets OPENSSL_CONF, so an operator's own OpenSSL config wins.
*/}}
{{- define "scalo-service.opensslEnv" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $path := "/tmp/scalo-openssl.cnf" -}}
{{- if hasKey $contract "openssl_conf" }}{{ $path = $contract.openssl_conf }}{{ end -}}
{{- $items := list -}}
{{- $set := or (hasKey (.Values.extraEnv | default dict) "OPENSSL_CONF") (hasKey (.Values.integrationEnv | default dict) "OPENSSL_CONF") -}}
{{- if and $path (not $set) -}}
{{- $items = append $items (dict "name" "OPENSSL_CONF" "value" $path) -}}
{{- end -}}
{{- dict "items" $items | toJson -}}
{{- end -}}

{{/*
scalo-service.env -- the container env as {"items": [...]}: `extraEnv` first, then
`integrationEnv`, then the namespace, OTel identity, version-check overrides, writable
path limits, the OpenSSL config and secret references. Each name renders once: a name
the chart derives is dropped from both maps, and an extraEnv name from integrationEnv,
so a server-side apply never meets one name twice.
*/}}
{{- define "scalo-service.env" -}}
{{- $derived := list (include "scalo-service.podNamespaceEnv" . | fromJson) -}}
{{- $derived = concat $derived (include "scalo-service.otelEnv" . | fromJson).items -}}
{{- $derived = concat $derived (include "scalo-service.versionCheckEnv" . | fromJson).items -}}
{{- $derived = concat $derived (include "scalo-service.writableLimitsEnv" . | fromJson).items -}}
{{- $derived = concat $derived (include "scalo-service.opensslEnv" . | fromJson).items -}}
{{- $derived = concat $derived (include "scalo-service.secretEnv" . | fromJson).items -}}
{{- $names := list -}}
{{- range $derived }}{{ $names = append $names .name }}{{ end -}}
{{- $extra := (include "scalo-service.extraEnv" (dict "ctx" . "env" .Values.extraEnv "drop" $names) | fromJson).items -}}
{{- $operator := keys (.Values.extraEnv | default dict) -}}
{{- $integration := (include "scalo-service.extraEnv" (dict "ctx" . "env" .Values.integrationEnv "drop" (concat $names $operator)) | fromJson).items -}}
{{- dict "items" (concat $extra $integration $derived) | toJson -}}
{{- end -}}
