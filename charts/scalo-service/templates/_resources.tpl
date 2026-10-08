{{/*
scalo-service.resources -- requests and limits as JSON: the library default, then the
contract's resources, then `resources` in values. An empty string in values drops that
entry, for a deployment that runs without, say, a CPU limit.
*/}}
{{- define "scalo-service.resources" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $resources := dict "requests" (dict "cpu" "100m" "memory" "128Mi") "limits" (dict "cpu" "500m" "memory" "512Mi") -}}
{{- $fromContract := $contract.resources | default dict -}}
{{- $fromValues := .Values.resources | default dict -}}
{{- range $kind := list "requests" "limits" -}}
{{- $target := get $resources $kind -}}
{{- range $name, $quantity := get $fromContract $kind | default dict -}}
{{- if $quantity }}{{ $_ := set $target $name (toString $quantity) }}{{ end -}}
{{- end -}}
{{- range $name, $quantity := get $fromValues $kind | default dict -}}
{{- if eq (toString $quantity) "" }}{{ $_ := unset $target $name }}{{ else }}{{ $_ := set $target $name (toString $quantity) }}{{ end -}}
{{- end -}}
{{- if not $target }}{{ $_ := unset $resources $kind }}{{ end -}}
{{- end -}}
{{- toJson $resources -}}
{{- end -}}
