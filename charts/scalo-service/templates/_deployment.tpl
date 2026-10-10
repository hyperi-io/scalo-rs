{{/*
scalo-service.containerPorts -- the metrics port, then every port the app listens on, as
{"items": [...]}.
*/}}
{{- define "scalo-service.containerPorts" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $items := list (dict "name" "metrics" "containerPort" (int $contract.metrics_port) "protocol" "TCP") -}}
{{- range (include "scalo-service.ports" . | fromJson).items -}}
{{- $items = append $items (dict "name" .name "containerPort" (int .port) "protocol" .protocol) -}}
{{- end -}}
{{- dict "items" $items | toJson -}}
{{- end -}}

{{/*
scalo-service.deployment -- the app's Deployment. Its selector is the name label alone,
and its pod template carries a checksum of the config file and file sets, so a config
change rolls the pods.
*/}}
{{- define "scalo-service.deployment" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $image := .Values.image | default dict -}}
{{- $volumes := include "scalo-service.volumes" . | fromJson -}}
{{- $probes := include "scalo-service.probes" . | fromJson -}}
{{- $container := dict "name" .Chart.Name "image" (include "scalo-service.image" .) "imagePullPolicy" ($image.pullPolicy | default "IfNotPresent") -}}
{{- with .Values.args | default $contract.entrypoint_args }}{{ $_ := set $container "args" . }}{{ end -}}
{{- with .Values.workingDir }}{{ $_ := set $container "workingDir" . }}{{ end -}}
{{- $_ := set $container "securityContext" (include "scalo-service.containerSecurityContext" . | fromJson) -}}
{{- $_ = set $container "ports" (include "scalo-service.containerPorts" . | fromJson).items -}}
{{- $_ = set $container "env" (include "scalo-service.env" . | fromJson).items -}}
{{- with .Values.extraEnvFrom }}{{ $_ = set $container "envFrom" (tpl (toYaml .) $ | fromYamlArray) }}{{ end -}}
{{- $_ = set $container "startupProbe" $probes.startupProbe -}}
{{- $_ = set $container "livenessProbe" $probes.livenessProbe -}}
{{- $_ = set $container "readinessProbe" $probes.readinessProbe -}}
{{- $_ = set $container "resources" (include "scalo-service.resources" . | fromJson) -}}
{{- $_ = set $container "volumeMounts" $volumes.mounts -}}
{{- $containers := list $container -}}
{{- with .Values.sidecars }}{{ $containers = concat $containers (tpl (toYaml .) $ | fromYamlArray) }}{{ end -}}
{{- $pod := include "scalo-service.scheduling" . | fromJson -}}
{{- with include "scalo-service.podServiceAccountName" . }}{{ $_ = set $pod "serviceAccountName" . }}{{ end -}}
{{- $_ = set $pod "automountServiceAccountToken" (not (empty (include "scalo-service.tokenMounted" .))) -}}
{{- $_ = set $pod "enableServiceLinks" false -}}
{{- $_ = set $pod "terminationGracePeriodSeconds" (int (include "scalo-service.terminationGrace" .)) -}}
{{- with (include "scalo-service.podSecurityContext" . | fromJson).v }}{{ $_ = set $pod "securityContext" . }}{{ end -}}
{{- with .Values.initContainers }}{{ $_ = set $pod "initContainers" (tpl (toYaml .) $ | fromYamlArray) }}{{ end -}}
{{- $_ = set $pod "containers" $containers -}}
{{- $_ = set $pod "volumes" $volumes.volumes -}}
{{- $podAnnotations := include "scalo-service.prometheusAnnotations" . | fromJson -}}
{{- if $contract.config_mount_path -}}
{{- $_ = set $podAnnotations "checksum/config" (include "scalo-service.configFile" . | sha256sum) -}}
{{- end -}}
{{- with .Values.fileSets -}}
{{- $_ = set $podAnnotations "checksum/files" (toJson . | sha256sum) -}}
{{- end -}}
{{- $podAnnotations = mergeOverwrite $podAnnotations (deepCopy (.Values.podAnnotations | default dict)) -}}
{{- $podMetadata := dict "labels" (include "scalo-service.podLabelSet" . | fromJson) -}}
{{- with $podAnnotations }}{{ $_ = set $podMetadata "annotations" . }}{{ end -}}
{{- $spec := dict "selector" (dict "matchLabels" (include "scalo-service.selectorLabelSet" . | fromJson)) -}}
{{- with include "scalo-service.replicas" . }}{{ $_ = set $spec "replicas" (int .) }}{{ end -}}
{{- $_ = set $spec "strategy" (include "scalo-service.strategy" . | fromJson) -}}
{{- $_ = set $spec "template" (dict "metadata" $podMetadata "spec" $pod) -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  {{- include "scalo-service.metadata" (dict "ctx" . "name" (include "scalo-service.fullname" .) "annotations" (include "scalo-service.reloadAnnotations" . | fromJson)) | nindent 2 }}
spec:
  {{- toYaml $spec | nindent 2 }}
{{- end -}}
