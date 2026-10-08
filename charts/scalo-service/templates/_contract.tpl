{{/*
scalo-service.contract -- the parent chart's files/contract.json, as JSON. A library
template reads the PARENT chart's files, so every thin chart carries its own contract.
Callers parse it with: include "scalo-service.contract" . | fromJson
*/}}
{{- define "scalo-service.contract" -}}
{{- $raw := .Files.Get "files/contract.json" -}}
{{- if not $raw -}}
{{- fail "scalo-service: the chart has no files/contract.json; copy the app's deployment-contract.json there" -}}
{{- end -}}
{{- $contract := fromJson $raw -}}
{{- if and (hasKey $contract "Error") (not (hasKey $contract "app_name")) -}}
{{- fail (printf "scalo-service: files/contract.json is not a JSON object: %v" $contract.Error) -}}
{{- end -}}
{{- $supported := list 4 -}}
{{- $version := 0 -}}
{{- if hasKey $contract "schema_version" -}}{{- $version = int $contract.schema_version -}}{{- end -}}
{{- if not (has $version $supported) -}}
{{- fail (printf "scalo-service: files/contract.json has schema_version %v, and this library reads %v" ($contract.schema_version | default "none") $supported) -}}
{{- end -}}
{{- range $field := list "app_name" "metrics_port" "health" "env_prefix" "metric_prefix" -}}
{{- if not (hasKey $contract $field) -}}
{{- fail (printf "scalo-service: files/contract.json has no %s, which every contract carries" $field) -}}
{{- end -}}
{{- end -}}
{{- range $field := list "liveness_path" "readiness_path" "metrics_path" -}}
{{- if not (hasKey $contract.health $field) -}}
{{- fail (printf "scalo-service: files/contract.json has no health.%s" $field) -}}
{{- end -}}
{{- end -}}
{{- toJson $contract -}}
{{- end -}}
