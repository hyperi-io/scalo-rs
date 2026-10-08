{{/*
scalo-service.pdb -- a PodDisruptionBudget once replicaCount or the autoscaler floor is two
or more, never for a singleton, where any budget blocks every node drain. Its form follows
the floor, the fewest pods anything can leave: minAvailable 1 at two or more, else
maxUnavailable 1, which still lets a scaled-to-one pod move. pdb.minAvailable or
pdb.maxUnavailable pins the form; pdb.enabled: false turns it off.
*/}}
{{- define "scalo-service.pdb" -}}
{{- $pdb := .Values.pdb | default dict -}}
{{- $enabled := true -}}
{{- if hasKey $pdb "enabled" }}{{ $enabled = $pdb.enabled }}{{ end -}}
{{- $replicas := int (.Values.replicaCount | default 1) -}}
{{- $floor := $replicas -}}
{{- $autoscalerFloor := 0 -}}
{{- if include "scalo-service.kedaEnabled" . -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $keda := .Values.keda | default dict -}}
{{- $autoscalerFloor = 1 -}}
{{- with $contract.keda }}{{ if hasKey . "min_replicas" }}{{ $autoscalerFloor = int .min_replicas }}{{ end }}{{ end -}}
{{- if hasKey $keda "minReplicaCount" }}{{ $autoscalerFloor = int $keda.minReplicaCount }}{{ end -}}
{{- $floor = $autoscalerFloor -}}
{{- else if include "scalo-service.hpaEnabled" . -}}
{{- $autoscalerFloor = int (.Values.autoscaling.minReplicas | default 1) -}}
{{- $floor = $autoscalerFloor -}}
{{- end -}}
{{- if and $enabled (not (include "scalo-service.singleton" .)) (or (ge $replicas 2) (ge $autoscalerFloor 2)) -}}
{{- $spec := dict "selector" (dict "matchLabels" (include "scalo-service.selectorLabelSet" . | fromJson)) -}}
{{- if $pdb.maxUnavailable -}}
{{- $_ := set $spec "maxUnavailable" $pdb.maxUnavailable -}}
{{- else if $pdb.minAvailable -}}
{{- $_ := set $spec "minAvailable" $pdb.minAvailable -}}
{{- else if ge $floor 2 -}}
{{- $_ := set $spec "minAvailable" 1 -}}
{{- else -}}
{{- $_ := set $spec "maxUnavailable" 1 -}}
{{- end -}}
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  {{- include "scalo-service.metadata" (dict "ctx" . "name" (include "scalo-service.fullname" .)) | nindent 2 }}
spec:
  {{- toYaml $spec | nindent 2 }}
{{- end -}}
{{- end -}}
