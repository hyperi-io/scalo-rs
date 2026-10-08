{{/*
scalo-service.fullname -- every object's name: fullnameOverride, else the chart name.
The release name is never part of it, so a rename never orphans a selector or a claim.
*/}}
{{- define "scalo-service.fullname" -}}
{{- .Values.fullnameOverride | default .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
scalo-service.serviceAccountName -- serviceAccount.name, else the fullname.
*/}}
{{- define "scalo-service.serviceAccountName" -}}
{{- $sa := .Values.serviceAccount | default dict -}}
{{- $sa.name | default (include "scalo-service.fullname" .) -}}
{{- end -}}
