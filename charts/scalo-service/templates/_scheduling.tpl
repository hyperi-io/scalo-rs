{{/*
scalo-service.scheduling -- imagePullSecrets, priorityClassName, nodeSelector,
tolerations, affinity and topologySpreadConstraints for the pod spec, as JSON, each only
when set. imagePullSecrets takes names or {name} entries. With `affinity` unset, a
workload that can run more than one pod gets scalo-service.defaultAffinity.
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
{{- if .Values.affinity -}}
{{- $_ := set $out "affinity" .Values.affinity -}}
{{- else -}}
{{- with include "scalo-service.defaultAffinity" . }}{{ $_ := set $out "affinity" (fromJson .) }}{{ end -}}
{{- end -}}
{{- with .Values.topologySpreadConstraints }}{{ $_ := set $out "topologySpreadConstraints" (tpl (toYaml .) $ | fromYamlArray) }}{{ end -}}
{{- toJson $out -}}
{{- end -}}

{{/*
scalo-service.defaultAffinity -- a preferred pod anti-affinity on the node hostname over
the selector labels, as JSON, when more than one pod can run: a replica count above one,
KEDA or the HPA. Preferred, so a single-node cluster still schedules every replica.
Empty for a singleton.
*/}}
{{- define "scalo-service.defaultAffinity" -}}
{{- if not (include "scalo-service.singleton" .) -}}
{{- if or (gt (.Values.replicaCount | default 1 | int) 1) (include "scalo-service.kedaEnabled" .) (include "scalo-service.hpaEnabled" .) -}}
{{- $term := dict "topologyKey" "kubernetes.io/hostname" "labelSelector" (dict "matchLabels" (include "scalo-service.selectorLabelSet" . | fromJson)) -}}
{{- dict "podAntiAffinity" (dict "preferredDuringSchedulingIgnoredDuringExecution" (list (dict "weight" 100 "podAffinityTerm" $term))) | toJson -}}
{{- end -}}
{{- end -}}
{{- end -}}
