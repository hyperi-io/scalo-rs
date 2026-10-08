{{/*
scalo-service.image -- registry/app_name:tag@digest. image.repository replaces the
whole repository; otherwise the registry is image.registry, global.registry, then the
contract's image_registry. The tag defaults to the chart's appVersion, and a digest,
when set, pins the bytes the tag names.
*/}}
{{- define "scalo-service.image" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $image := .Values.image | default dict -}}
{{- $global := .Values.global | default dict -}}
{{- $repository := $image.repository | default "" -}}
{{- if not $repository -}}
{{- $registry := $image.registry | default $global.registry | default $contract.image_registry | default "" -}}
{{- if not $registry -}}
{{- fail "scalo-service: no image registry; set image.registry, global.registry or image_registry in the contract" -}}
{{- end -}}
{{- $repository = printf "%s/%s" (trimSuffix "/" $registry) $contract.app_name -}}
{{- end -}}
{{- $tag := toString ($image.tag | default .Chart.AppVersion | default "") -}}
{{- if not $tag -}}
{{- fail "scalo-service: no image tag; set image.tag or the chart's appVersion" -}}
{{- end -}}
{{- $reference := printf "%s:%s" $repository $tag -}}
{{- with $image.digest -}}{{- $reference = printf "%s@%s" $reference . -}}{{- end -}}
{{- $reference -}}
{{- end -}}
