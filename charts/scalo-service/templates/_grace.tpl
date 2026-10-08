{{/*
scalo-service.terminationGrace -- seconds between SIGTERM and SIGKILL: values, else the
contract's termination_grace_seconds, else 45. Read with hasKey so an explicit 0 holds.
*/}}
{{- define "scalo-service.terminationGrace" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $grace := 45 -}}
{{- if hasKey $contract "termination_grace_seconds" -}}{{- $grace = int $contract.termination_grace_seconds -}}{{- end -}}
{{- if hasKey .Values "terminationGracePeriodSeconds" -}}{{- $grace = int .Values.terminationGracePeriodSeconds -}}{{- end -}}
{{- $grace -}}
{{- end -}}
