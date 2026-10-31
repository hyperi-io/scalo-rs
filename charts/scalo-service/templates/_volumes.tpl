{{/*
scalo-service.dropNulls -- removes every null-valued key from a map in place, in nested
maps and in maps inside lists too, so a deployer who nulls a key gets the app's default
rather than a null written into its config file. Renders nothing.
*/}}
{{- define "scalo-service.dropNulls" -}}
{{- $node := . -}}
{{- range $key, $value := $node -}}
{{- if kindIs "invalid" $value -}}
{{- $_ := unset $node $key -}}
{{- else if kindIs "map" $value -}}
{{- include "scalo-service.dropNulls" $value -}}
{{- else if kindIs "slice" $value -}}
{{- range $item := $value -}}
{{- if kindIs "map" $item }}{{ include "scalo-service.dropNulls" $item }}{{ end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
scalo-service.configFile -- the rendered app config: `config` with `configOverrides`
merged over it, null keys dropped. The contract's default_config is never merged in, so
the app's own defaults stay authoritative and no stale key is frozen into a deployment.
*/}}
{{- define "scalo-service.configFile" -}}
{{- $config := mergeOverwrite (deepCopy (.Values.config | default dict)) (deepCopy (.Values.configOverrides | default dict)) -}}
{{- include "scalo-service.dropNulls" $config -}}
{{- printf "%s\n" (ternary (toYaml $config) "{}" (not (empty $config))) -}}
{{- end -}}

{{/*
scalo-service.configChecksumInput -- the rendered app config with the contract's hot_paths
removed, which the pod's checksum/config hashes: the app reloads those paths from the
mounted file, so an edit there must not roll the pods. A parent map the removal leaves
empty goes too, so adding the first entry under a hot path rolls nothing either. A subPath
mount never sees the file change, so with configMount.subPath every path counts.
*/}}
{{- define "scalo-service.configChecksumInput" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $config := mergeOverwrite (deepCopy (.Values.config | default dict)) (deepCopy (.Values.configOverrides | default dict)) -}}
{{- include "scalo-service.dropNulls" $config -}}
{{- if not (.Values.configMount | default dict).subPath -}}
{{- range $path := $contract.hot_paths | default list -}}
{{- $segments := splitList "." $path -}}
{{- range $depth := untilStep (len $segments) 0 -1 -}}
{{- $prefix := slice $segments 0 $depth -}}
{{- $node := $config -}}
{{- range $segment := initial $prefix -}}
{{- if and (kindIs "map" $node) (hasKey $node $segment) -}}
{{- $node = get $node $segment -}}
{{- else -}}
{{- $node = dict -}}
{{- end -}}
{{- end -}}
{{- $key := last $prefix -}}
{{- if and (kindIs "map" $node) (hasKey $node $key) -}}
{{- $child := get $node $key -}}
{{- if or (eq $depth (len $segments)) (and (kindIs "map" $child) (empty $child)) -}}
{{- $_ := unset $node $key -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- printf "%s\n" (ternary (toYaml $config) "{}" (not (empty $config))) -}}
{{- end -}}

{{/*
scalo-service.volumes -- the pod's volumes and the app container's mounts, as
{"volumes": [...], "mounts": [...]}: the config ConfigMap when the contract names a
config file, each writable path as an emptyDir or its claim, each file set, the
service-account files, then `extraVolumes` and `extraVolumeMounts`. The config mounts as
its directory, or as the one file when configMount.subPath is set, for an app whose
config directory holds other files.
*/}}
{{- define "scalo-service.volumes" -}}
{{- $contract := include "scalo-service.contract" . | fromJson -}}
{{- $fullname := include "scalo-service.fullname" . -}}
{{- $configMount := .Values.configMount | default dict -}}
{{- $volumes := list -}}
{{- $mounts := list -}}
{{- with $contract.config_mount_path -}}
{{- $volumes = append $volumes (dict "name" "config" "configMap" (dict "name" (printf "%s-config" $fullname))) -}}
{{- if $configMount.subPath -}}
{{- $mounts = append $mounts (dict "name" "config" "mountPath" . "subPath" (base .) "readOnly" true) -}}
{{- else -}}
{{- $mounts = append $mounts (dict "name" "config" "mountPath" (dir .) "readOnly" true) -}}
{{- end -}}
{{- end -}}
{{- range $path := (include "scalo-service.writablePaths" . | fromJson).items -}}
{{- $volume := dict "name" (printf "writable-%s" $path.name) -}}
{{- if $path.persistent -}}
{{- $_ := set $volume "persistentVolumeClaim" (dict "claimName" ($path.existingClaim | default (printf "%s-%s" $fullname $path.name))) -}}
{{- else if $path.sizeLimit -}}
{{- $_ := set $volume "emptyDir" (dict "sizeLimit" $path.sizeLimit) -}}
{{- else -}}
{{- $_ := set $volume "emptyDir" dict -}}
{{- end -}}
{{- $volumes = append $volumes $volume -}}
{{- $mounts = append $mounts (dict "name" (printf "writable-%s" $path.name) "mountPath" $path.path) -}}
{{- end -}}
{{- range $name, $set := .Values.fileSets | default dict -}}
{{- if not $set.mountPath -}}
{{- fail (printf "scalo-service: fileSets.%s has no mountPath" $name) -}}
{{- end -}}
{{- $volumes = append $volumes (dict "name" (printf "fileset-%s" $name) "configMap" (dict "name" (printf "%s-%s" $fullname $name))) -}}
{{- $mounts = append $mounts (dict "name" (printf "fileset-%s" $name) "mountPath" $set.mountPath "readOnly" true) -}}
{{- end -}}
{{- $serviceAccount := include "scalo-service.serviceAccountFiles" . | fromJson -}}
{{- $volumes = concat $volumes $serviceAccount.volumes -}}
{{- $mounts = concat $mounts $serviceAccount.mounts -}}
{{- with .Values.extraVolumes }}{{ $volumes = concat $volumes (tpl (toYaml .) $ | fromYamlArray) }}{{ end -}}
{{- with .Values.extraVolumeMounts }}{{ $mounts = concat $mounts (tpl (toYaml .) $ | fromYamlArray) }}{{ end -}}
{{- dict "volumes" $volumes "mounts" $mounts | toJson -}}
{{- end -}}
