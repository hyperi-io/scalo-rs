{{/*
scalo-service.kedaTriggers -- the ScaledObject's triggers as {"items": [...]}:
keda.triggers verbatim when set, else CPU and the contract's Kafka lag trigger, then
keda.extraTriggers appended. Lists from values pass through tpl, so they can name the
release namespace.
*/}}
{{- define "scalo-service.kedaTriggers" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $contractKeda := $contract.keda | default dict -}}
{{- $keda := .Values.keda | default dict -}}
{{- $items := list -}}
{{- if $keda.triggers -}}
{{- $items = tpl (toYaml $keda.triggers) . | fromYamlArray -}}
{{- else -}}
{{- $cpu := $keda.cpu | default dict -}}
{{- $cpuOn := true -}}
{{- if hasKey $contractKeda "cpu_enabled" }}{{ $cpuOn = $contractKeda.cpu_enabled }}{{ end -}}
{{- if hasKey $cpu "enabled" }}{{ $cpuOn = $cpu.enabled }}{{ end -}}
{{- if $cpuOn -}}
{{- $target := 80 -}}
{{- if hasKey $contractKeda "cpu_threshold" }}{{ $target = int $contractKeda.cpu_threshold }}{{ end -}}
{{- if hasKey $cpu "targetUtilization" }}{{ $target = int $cpu.targetUtilization }}{{ end -}}
{{- $items = append $items (dict "type" "cpu" "metricType" "Utilization" "metadata" (dict "value" (toString $target))) -}}
{{- end -}}
{{- $trigger := $contractKeda.kafka_trigger | default (dict "enabled" true "brokers_path" "config.kafka.brokers" "group_path" "config.kafka.group_id" "topics_path" "config.kafka.topics") -}}
{{- $kafka := $keda.kafka | default dict -}}
{{- $kafkaOn := false -}}
{{- if $contract.keda -}}
{{- $kafkaOn = true -}}
{{- if hasKey $trigger "enabled" }}{{ $kafkaOn = $trigger.enabled }}{{ end -}}
{{- end -}}
{{- if hasKey $kafka "enabled" }}{{ $kafkaOn = $kafka.enabled }}{{ end -}}
{{- if $kafkaOn -}}
{{- $root := include "scalo-service.gateRoot" . | fromJson -}}
{{- $metadata := dict -}}
{{- range $pair := list (list "bootstrapServers" $trigger.brokers_path) (list "consumerGroup" $trigger.group_path) (list "topic" $trigger.topics_path) -}}
{{- $value := (include "scalo-service.valueAt" (dict "root" $root "path" (index $pair 1)) | fromJson).v -}}
{{- if kindIs "invalid" $value -}}
{{- fail (printf "scalo-service: the Kafka lag trigger reads %s, which neither values nor the contract's default_config sets; set it, or keda.kafka.enabled: false" (index $pair 1)) -}}
{{- end -}}
{{- if kindIs "slice" $value -}}
{{- if eq (index $pair 0) "topic" }}{{ $value = first $value }}{{ else }}{{ $value = join "," $value }}{{ end -}}
{{- else if eq (index $pair 0) "topic" -}}
{{- $value = first (splitList "," (toString $value)) -}}
{{- end -}}
{{- $_ := set $metadata (index $pair 0) (toString $value) -}}
{{- end -}}
{{- $lag := 1000 -}}
{{- if hasKey $contractKeda "kafka_lag_threshold" }}{{ $lag = int64 $contractKeda.kafka_lag_threshold }}{{ end -}}
{{- if hasKey $kafka "lagThreshold" }}{{ $lag = int64 $kafka.lagThreshold }}{{ end -}}
{{- $_ := set $metadata "lagThreshold" (toString $lag) -}}
{{- $activation := 0 -}}
{{- if hasKey $contractKeda "activation_lag_threshold" }}{{ $activation = int64 $contractKeda.activation_lag_threshold }}{{ end -}}
{{- if hasKey $kafka "activationLagThreshold" }}{{ $activation = int64 $kafka.activationLagThreshold }}{{ end -}}
{{- $_ = set $metadata "activationLagThreshold" (toString $activation) -}}
{{- $entry := dict "type" "kafka" "metadata" $metadata -}}
{{- if ($keda.triggerAuthentication | default dict).secretTargetRef -}}
{{- $_ = set $entry "authenticationRef" (dict "name" (printf "%s-trigger-auth" (include "scalo-service.fullname" .))) -}}
{{- end -}}
{{- $items = append $items $entry -}}
{{- end -}}
{{- end -}}
{{- with $keda.extraTriggers -}}
{{- $items = concat $items (tpl (toYaml .) $ | fromYamlArray) -}}
{{- end -}}
{{- dict "items" $items | toJson -}}
{{- end -}}

{{/*
scalo-service.scaledobject -- a KEDA ScaledObject named <fullname>-scaler while KEDA is
on. A ScaledObject with no trigger is refused by KEDA, and KEDA's CPU scaler cannot wake
a workload from zero, so either fails the render here.
*/}}
{{- define "scalo-service.scaledobject" -}}
{{- if include "scalo-service.kedaEnabled" . -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $contractKeda := $contract.keda | default dict -}}
{{- $keda := .Values.keda | default dict -}}
{{- $min := 1 -}}
{{- if hasKey $contractKeda "min_replicas" }}{{ $min = int $contractKeda.min_replicas }}{{ end -}}
{{- if hasKey $keda "minReplicaCount" }}{{ $min = int $keda.minReplicaCount }}{{ end -}}
{{- $max := 10 -}}
{{- if hasKey $contractKeda "max_replicas" }}{{ $max = int $contractKeda.max_replicas }}{{ end -}}
{{- if hasKey $keda "maxReplicaCount" }}{{ $max = int $keda.maxReplicaCount }}{{ end -}}
{{- $polling := 30 -}}
{{- if hasKey $contractKeda "polling_interval" }}{{ $polling = int $contractKeda.polling_interval }}{{ end -}}
{{- if hasKey $keda "pollingInterval" }}{{ $polling = int $keda.pollingInterval }}{{ end -}}
{{- $cooldown := 300 -}}
{{- if hasKey $contractKeda "cooldown_period" }}{{ $cooldown = int $contractKeda.cooldown_period }}{{ end -}}
{{- if hasKey $keda "cooldownPeriod" }}{{ $cooldown = int $keda.cooldownPeriod }}{{ end -}}
{{- $triggers := (include "scalo-service.kedaTriggers" . | fromJson).items -}}
{{- if not $triggers -}}
{{- fail "scalo-service: KEDA is on with no trigger; turn keda.cpu.enabled on, keep the Kafka lag trigger, or set keda.triggers" -}}
{{- end -}}
{{- $cpuOnly := true -}}
{{- range $triggers }}{{ if ne .type "cpu" }}{{ $cpuOnly = false }}{{ end }}{{ end -}}
{{- if and $cpuOnly (eq $min 0) -}}
{{- fail "scalo-service: CPU is the only KEDA trigger and minReplicaCount is 0, and KEDA's CPU scaler cannot wake a workload from zero" -}}
{{- end -}}
{{- $spec := dict "scaleTargetRef" (dict "name" (include "scalo-service.fullname" .)) "minReplicaCount" $min "maxReplicaCount" $max "pollingInterval" $polling "cooldownPeriod" $cooldown "triggers" $triggers -}}
{{- if hasKey $keda "idleReplicaCount" }}{{ $_ := set $spec "idleReplicaCount" (int $keda.idleReplicaCount) }}{{ end -}}
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  {{- include "scalo-service.metadata" (dict "ctx" . "name" (printf "%s-scaler" (include "scalo-service.fullname" .))) | nindent 2 }}
spec:
  {{- toYaml $spec | nindent 2 }}
{{- end -}}
{{- end -}}

{{/*
scalo-service.triggerauthentication -- a KEDA TriggerAuthentication named
<fullname>-trigger-auth while KEDA is on and keda.triggerAuthentication.secretTargetRef
is set; the Kafka lag trigger references it.
*/}}
{{- define "scalo-service.triggerauthentication" -}}
{{- $keda := .Values.keda | default dict -}}
{{- $auth := $keda.triggerAuthentication | default dict -}}
{{- if and (include "scalo-service.kedaEnabled" .) $auth.secretTargetRef -}}
apiVersion: keda.sh/v1alpha1
kind: TriggerAuthentication
metadata:
  {{- include "scalo-service.metadata" (dict "ctx" . "name" (printf "%s-trigger-auth" (include "scalo-service.fullname" .))) | nindent 2 }}
spec:
  secretTargetRef:
    {{- tpl (toYaml $auth.secretTargetRef) . | nindent 4 }}
{{- end -}}
{{- end -}}
