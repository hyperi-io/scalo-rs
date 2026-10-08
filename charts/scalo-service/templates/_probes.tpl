{{/*
scalo-service.probes -- startup, liveness and readiness on the metrics port, as JSON. The
startup probe targets the liveness path and allows health.startup_budget_seconds before
a restart; liveness is suspended until it passes. startupProbe, livenessProbe and
readinessProbe in values merge over each.
*/}}
{{- define "scalo-service.probes" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $health := $contract.health -}}
{{- $budget := 150 -}}
{{- if hasKey $health "startup_budget_seconds" }}{{ $budget = int $health.startup_budget_seconds }}{{ end -}}
{{- $period := 5 -}}
{{- $failures := max 1 (div (add $budget (sub $period 1)) $period) -}}
{{- $startup := dict "httpGet" (dict "path" $health.liveness_path "port" "metrics") "periodSeconds" $period "timeoutSeconds" 3 "failureThreshold" $failures -}}
{{- $liveness := dict "httpGet" (dict "path" $health.liveness_path "port" "metrics") "periodSeconds" 10 "timeoutSeconds" 3 "failureThreshold" 3 -}}
{{- $readiness := dict "httpGet" (dict "path" $health.readiness_path "port" "metrics") "periodSeconds" 5 "timeoutSeconds" 3 "failureThreshold" 2 -}}
{{- $out := dict -}}
{{- $_ := set $out "startupProbe" (mergeOverwrite $startup (deepCopy (.Values.startupProbe | default dict))) -}}
{{- $_ = set $out "livenessProbe" (mergeOverwrite $liveness (deepCopy (.Values.livenessProbe | default dict))) -}}
{{- $_ = set $out "readinessProbe" (mergeOverwrite $readiness (deepCopy (.Values.readinessProbe | default dict))) -}}
{{- toJson $out -}}
{{- end -}}
