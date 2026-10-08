{{/*
scalo-service.podSecurityContext -- the pod securityContext from the contract's security
block, as {"v": ...}: its uid, gid and fsGroup, non-root unless uid is 0, and the
runtime's seccomp profile. podSecurityContext in values merges over it, and enabled:
false makes it null, for a workload that manages its own.
*/}}
{{- define "scalo-service.podSecurityContext" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $security := $contract.security | default dict -}}
{{- $uid := 1000 -}}
{{- if hasKey $security "run_as_user" }}{{ $uid = int $security.run_as_user }}{{ end -}}
{{- $gid := 1000 -}}
{{- if hasKey $security "run_as_group" }}{{ $gid = int $security.run_as_group }}{{ end -}}
{{- $fsGroup := 1000 -}}
{{- if hasKey $security "fs_group" }}{{ $fsGroup = int $security.fs_group }}{{ end -}}
{{- $context := dict "runAsNonRoot" (ne $uid 0) "runAsUser" $uid "runAsGroup" $gid "fsGroup" $fsGroup "seccompProfile" (dict "type" "RuntimeDefault") -}}
{{- $override := deepCopy (.Values.podSecurityContext | default dict) -}}
{{- $enabled := true -}}
{{- if hasKey $override "enabled" }}{{ $enabled = $override.enabled }}{{ $_ := unset $override "enabled" }}{{ end -}}
{{- if $enabled -}}
{{- dict "v" (mergeOverwrite $context $override) | toJson -}}
{{- else -}}
{"v":null}
{{- end -}}
{{- end -}}

{{/*
scalo-service.containerSecurityContext -- no privilege escalation, every capability dropped
and the contract's capabilities_add added back, and a read-only root filesystem unless
the contract says otherwise, as JSON. containerSecurityContext merges over it.
*/}}
{{- define "scalo-service.containerSecurityContext" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $security := $contract.security | default dict -}}
{{- $readOnly := true -}}
{{- if hasKey $security "read_only_root_filesystem" }}{{ $readOnly = $security.read_only_root_filesystem }}{{ end -}}
{{- $capabilities := dict "drop" (list "ALL") -}}
{{- with $security.capabilities_add }}{{ $_ := set $capabilities "add" . }}{{ end -}}
{{- $context := dict "allowPrivilegeEscalation" false "readOnlyRootFilesystem" $readOnly "capabilities" $capabilities -}}
{{- mergeOverwrite $context (deepCopy (.Values.containerSecurityContext | default dict)) | toJson -}}
{{- end -}}
