{{/*
scalo-service.prometheusAnnotations -- scrape annotations on the contract's metrics port
and path, as JSON. On unless telemetry.prometheus.scrape is false, because an app serves
/metrics whatever its OTLP push is set to.
*/}}
{{- define "scalo-service.prometheusAnnotations" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $telemetry := .Values.telemetry | default dict -}}
{{- $prometheus := $telemetry.prometheus | default dict -}}
{{- $scrape := true -}}
{{- if hasKey $prometheus "scrape" }}{{ $scrape = $prometheus.scrape }}{{ end -}}
{{- $out := dict -}}
{{- if $scrape -}}
{{- $_ := set $out "prometheus.io/scrape" "true" -}}
{{- $_ = set $out "prometheus.io/port" (toString (int $contract.metrics_port)) -}}
{{- $_ = set $out "prometheus.io/path" $contract.health.metrics_path -}}
{{- end -}}
{{- toJson $out -}}
{{- end -}}

{{/*
scalo-service.otelEnv -- OTEL_SERVICE_NAME always, and the OTLP endpoint and protocol only
when otel.endpoint is set, as {"items": [...]}; an empty endpoint leaves the app's own
default in place.
*/}}
{{- define "scalo-service.otelEnv" -}}
{{- $otel := .Values.otel | default dict -}}
{{- $items := list (dict "name" "OTEL_SERVICE_NAME" "value" ($otel.serviceName | default (include "scalo-service.fullname" .))) -}}
{{- with $otel.endpoint -}}
{{- $items = append $items (dict "name" "OTEL_EXPORTER_OTLP_ENDPOINT" "value" .) -}}
{{- $items = append $items (dict "name" "OTEL_EXPORTER_OTLP_PROTOCOL" "value" ($otel.protocol | default "grpc")) -}}
{{- end -}}
{{- dict "items" $items | toJson -}}
{{- end -}}
