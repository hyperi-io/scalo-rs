{{/*
scalo-service.gateRoot -- .Values with `config` replaced by the config the app ends up
running: the contract's default_config, then `config`, then `configOverrides`. Port and
path conditions read this, so a gate holds whenever the app's own default turns it on.
Nothing here is rendered into the ConfigMap. Returns JSON.
*/}}
{{- define "scalo-service.gateRoot" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $effective := deepCopy ($contract.default_config | default dict) -}}
{{- $effective = mergeOverwrite $effective (deepCopy (.Values.config | default dict)) -}}
{{- $effective = mergeOverwrite $effective (deepCopy (.Values.configOverrides | default dict)) -}}
{{- $root := deepCopy .Values -}}
{{- $_ := set $root "config" $effective -}}
{{- toJson $root -}}
{{- end -}}

{{/*
scalo-service.valueAt -- the value at a dotted path in `root`, as {"v": value}; a
missing step yields null. Takes (dict "root" ROOT "path" "config.a.b").
*/}}
{{- define "scalo-service.valueAt" -}}
{{- $node := .root -}}
{{- $found := true -}}
{{- range $part := splitList "." .path -}}
{{- if and $found (kindIs "map" $node) (hasKey $node $part) -}}
{{- $node = index $node $part -}}
{{- else -}}
{{- $found = false -}}
{{- end -}}
{{- end -}}
{{- if $found -}}{{- dict "v" $node | toJson -}}{{- else -}}{"v":null}{{- end -}}
{{- end -}}

{{/*
scalo-service.holds -- "true" when a contract condition holds against `root`, empty
otherwise; no condition always holds. A missing or null value never satisfies one.
Takes (dict "root" ROOT "when" CONDITION).
*/}}
{{- define "scalo-service.holds" -}}
{{- $when := .when -}}
{{- if not $when -}}
true
{{- else -}}
{{- $value := (include "scalo-service.valueAt" (dict "root" .root "path" $when.path) | fromJson).v -}}
{{- $set := not (kindIs "invalid" $value) -}}
{{- if eq $when.kind "enabled" -}}
{{- if $value }}true{{ end -}}
{{- else if eq $when.kind "equals" -}}
{{- if and $set (eq (toString $value) (toString $when.value)) }}true{{ end -}}
{{- else if eq $when.kind "one_of" -}}
{{- if and $set (has (toString $value) ($when.values | default list)) }}true{{ end -}}
{{- else -}}
{{- fail (printf "scalo-service: a contract condition has kind %v, and this library reads enabled, equals and one_of" $when.kind) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
scalo-service.ports -- every port the app listens on beyond metrics: the contract's
ports whose condition holds, then `extraPorts`. Returns {"items": [...]} with name,
port, protocol, appProtocol and public on each.
*/}}
{{- define "scalo-service.ports" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $root := include "scalo-service.gateRoot" . | fromJson -}}
{{- $items := list -}}
{{- range $port := $contract.extra_ports | default list -}}
{{- if include "scalo-service.holds" (dict "root" $root "when" $port.when) -}}
{{- $items = append $items (dict "name" $port.name "port" (int $port.port) "protocol" (upper ($port.protocol | default "TCP")) "appProtocol" ($port.app_protocol | default "") "public" ($port.public | default false)) -}}
{{- end -}}
{{- end -}}
{{- range $port := .Values.extraPorts | default list -}}
{{- $items = append $items (dict "name" $port.name "port" (int $port.port) "protocol" (upper ($port.protocol | default "TCP")) "appProtocol" ($port.appProtocol | default "") "public" ($port.public | default false)) -}}
{{- end -}}
{{- dict "items" $items | toJson -}}
{{- end -}}

{{/*
scalo-service.writablePaths -- every directory the pod mounts writable: the contract's
paths whose condition holds and `writablePaths.<name>.enabled` leaves on, then /tmp
unless the contract mounts it. Values override size, sizeLimit and persistence per
name. Returns {"items": [...]}.
*/}}
{{- define "scalo-service.writablePaths" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $root := include "scalo-service.gateRoot" . | fromJson -}}
{{- $overrides := .Values.writablePaths | default dict -}}
{{- $items := list -}}
{{- $hasTmp := false -}}
{{- range $path := $contract.writable_paths | default list -}}
{{- $override := get $overrides $path.name | default dict -}}
{{- $enabled := true -}}
{{- if hasKey $override "enabled" -}}{{- $enabled = $override.enabled -}}{{- end -}}
{{- if and $enabled (include "scalo-service.holds" (dict "root" $root "when" $path.when)) -}}
{{- $persistence := $override.persistence | default dict -}}
{{- $persistent := $path.persistent | default false -}}
{{- if hasKey $persistence "enabled" -}}{{- $persistent = $persistence.enabled -}}{{- end -}}
{{- if eq (trimSuffix "/" $path.path) "/tmp" -}}{{- $hasTmp = true -}}{{- end -}}
{{- $items = append $items (dict "name" $path.name "path" $path.path "sizeLimit" ($override.sizeLimit | default $path.size_limit | default "") "persistent" $persistent "size" ($persistence.size | default $path.size | default "1Gi") "storageClass" ($persistence.storageClass | default "") "accessModes" ($persistence.accessModes | default (list "ReadWriteOnce")) "existingClaim" ($persistence.existingClaim | default "") "annotations" ($persistence.annotations | default dict)) -}}
{{- end -}}
{{- end -}}
{{- if not $hasTmp -}}
{{- $items = append $items (dict "name" "tmp" "path" "/tmp" "sizeLimit" "" "persistent" false) -}}
{{- end -}}
{{- dict "items" $items | toJson -}}
{{- end -}}

{{/*
scalo-service.persistent -- "true" when any writable path the pod mounts is a claim.
*/}}
{{- define "scalo-service.persistent" -}}
{{- range $path := (include "scalo-service.writablePaths" . | fromJson).items -}}
{{- if $path.persistent }}true{{ end -}}
{{- end -}}
{{- end -}}

{{/*
scalo-service.singleton -- "true" when the contract allows exactly one pod.
*/}}
{{- define "scalo-service.singleton" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- if $contract.singleton }}true{{ end -}}
{{- end -}}
