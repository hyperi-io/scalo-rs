{{/*
scalo-service.hpa -- the fallback HorizontalPodAutoscaler on CPU, for a cluster without
KEDA; it renders only while autoscaling.enabled is set and KEDA is off.
*/}}
{{- define "scalo-service.hpa" -}}
{{- if include "scalo-service.hpaEnabled" . -}}
{{- $hpa := .Values.autoscaling -}}
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  {{- include "scalo-service.metadata" (dict "ctx" . "name" (include "scalo-service.fullname" .)) | nindent 2 }}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: {{ include "scalo-service.fullname" . }}
  minReplicas: {{ $hpa.minReplicas | default 1 | int }}
  maxReplicas: {{ $hpa.maxReplicas | default 10 | int }}
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: {{ $hpa.targetCPUUtilizationPercentage | default 80 | int }}
{{- end -}}
{{- end -}}
