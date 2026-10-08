{{/*
scalo-service.scheduling -- imagePullSecrets, priorityClassName, nodeSelector,
tolerations, affinity and topologySpreadConstraints for the pod spec, as JSON, each only
when set. imagePullSecrets takes names or {name} entries.
*/}}
{{- define "scalo-service.scheduling" -}}
{{- $out := dict -}}
{{- with .Values.imagePullSecrets -}}
{{- $secrets := list -}}
{{- range . -}}
{{- if kindIs "map" . -}}
{{- $secrets = append $secrets (dict "name" .name) -}}
{{- else -}}
{{- $secrets = append $secrets (dict "name" (toString .)) -}}
{{- end -}}
{{- end -}}
{{- $_ := set $out "imagePullSecrets" $secrets -}}
{{- end -}}
{{- with .Values.priorityClassName }}{{ $_ := set $out "priorityClassName" . }}{{ end -}}
{{- with .Values.nodeSelector }}{{ $_ := set $out "nodeSelector" . }}{{ end -}}
{{- with .Values.tolerations }}{{ $_ := set $out "tolerations" . }}{{ end -}}
{{- with .Values.affinity }}{{ $_ := set $out "affinity" . }}{{ end -}}
{{- with .Values.topologySpreadConstraints }}{{ $_ := set $out "topologySpreadConstraints" (tpl (toYaml .) $ | fromYamlArray) }}{{ end -}}
{{- toJson $out -}}
{{- end -}}
