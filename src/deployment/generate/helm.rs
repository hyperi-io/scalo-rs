// Project:   scalo
// File:      src/deployment/generate/helm.rs
// Purpose:   Helm chart generation (Chart.yaml, values, templates)
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

#![allow(clippy::format_push_string)]

use std::path::Path;

use crate::deployment::contract::{DeploymentContract, PortCondition};
use crate::deployment::error::DeploymentError;
use crate::deployment::keda::{KafkaLagTrigger, KedaContract};

use super::common::{is_go_identifier, safe_template_lookup, to_camel_suffix, write_file};

// ============================================================================
// Helm chart
// ============================================================================

/// Generate a complete Helm chart directory from the deployment contract.
///
/// Writes `Chart.yaml`, `values.yaml`, and all template files to `output_dir`.
///
/// `identity`, when provided, stamps the three `io.hyperi.contract.*`
/// annotations into `Chart.yaml`'s top-level `annotations:` block per the
/// Contract Identity Annotation Scheme v1. Phase 1 rollout: optional;
/// callers SHOULD pass `Some(&identity)`.
///
/// # Errors
///
/// Returns `DeploymentError` if files or directories cannot be created, or
/// [`DeploymentError::InvalidContract`] if the KEDA contract or a port's `when`
/// names a values path that is not a dotted chain of Go identifiers, a `one_of`
/// port condition lists no values, or the KEDA contract turns off both the
/// Kafka lag trigger and the CPU trigger while leaving KEDA on.
pub fn generate_chart(
    contract: &DeploymentContract,
    output_dir: impl AsRef<Path>,
    identity: Option<&crate::deployment::ContractIdentity>,
) -> Result<(), DeploymentError> {
    let dir = output_dir.as_ref();
    let templates_dir = dir.join("templates");

    // Rendered before anything is written, so a rejected contract leaves no
    // half-generated chart behind.
    let files = chart_files(contract, identity)?;

    std::fs::create_dir_all(&templates_dir).map_err(|e| DeploymentError::CreateDir {
        path: templates_dir.display().to_string(),
        source: e,
    })?;
    for (name, content) in &files {
        write_file(dir.join(name), content)?;
    }
    Ok(())
}

/// Every file of the chart as its path under the chart root and its content,
/// rendered in memory so a drift check needs no scratch directory.
///
/// # Errors
///
/// [`DeploymentError::InvalidContract`] for the contracts
/// [`generate_chart`] rejects.
pub(crate) fn chart_files(
    contract: &DeploymentContract,
    identity: Option<&crate::deployment::ContractIdentity>,
) -> Result<Vec<(&'static str, String)>, DeploymentError> {
    let gates = port_gates(contract)?;
    let keda_templates = match contract.enabled_keda() {
        Some(keda) => Some((
            gen_keda_scaledobject_yaml(contract, keda)?,
            gen_keda_triggerauth_yaml(contract, keda),
        )),
        None => None,
    };

    let mut files = vec![
        ("Chart.yaml", gen_chart_yaml(contract, identity)),
        ("values.yaml", gen_values_yaml(contract)),
        ("templates/_helpers.tpl", gen_helpers_tpl(contract)),
        (
            "templates/deployment.yaml",
            gen_deployment_yaml(contract, &gates),
        ),
        ("templates/service.yaml", gen_service_yaml(contract, &gates)),
        (
            "templates/serviceaccount.yaml",
            gen_serviceaccount_yaml(contract),
        ),
        ("templates/configmap.yaml", gen_configmap_yaml(contract)),
        ("templates/secret.yaml", gen_secret_yaml(contract)),
        ("templates/hpa.yaml", gen_hpa_yaml(contract)),
    ];
    if let Some((scaled_object, trigger_auth)) = keda_templates {
        files.push(("templates/keda-scaledobject.yaml", scaled_object));
        files.push(("templates/keda-triggerauth.yaml", trigger_auth));
    }
    files.push(("templates/NOTES.txt", gen_notes_txt(contract)));
    Ok(files)
}

// ============================================================================
// Chart file generators
// ============================================================================

fn gen_chart_yaml(
    c: &DeploymentContract,
    identity: Option<&crate::deployment::ContractIdentity>,
) -> String {
    // Contract Identity Annotation Scheme v1 -- top-level annotations block.
    let identity_block = identity
        .map(|id| format!("\nannotations:\n{ann}\n", ann = id.as_yaml_annotations(2)))
        .unwrap_or_default();

    format!(
        "apiVersion: v2\n\
         name: {name}\n\
         description: {desc}\n\
         type: application\n\
         version: 0.1.0\n\
         appVersion: \"1.0.0\"\n\
         {identity_block}\n\
         keywords:\n\
         \x20 - {name}\n",
        name = c.app_name,
        desc = if c.description.is_empty() {
            &c.app_name
        } else {
            &c.description
        },
        identity_block = identity_block,
    )
}

#[allow(clippy::too_many_lines)]
fn gen_values_yaml(c: &DeploymentContract) -> String {
    let mut out = String::with_capacity(2048);

    // Header comment
    out.push_str(&format!(
        "# {app} Helm chart values\n\
         #\n\
         # Generated by scalo deployment module.\n\
         # Contract points validated by cargo test.\n\
         \n",
        app = c.app_name,
    ));

    // Replicas, image, overrides
    out.push_str(&format!(
        "# -- Number of replicas. Ignored while a KEDA ScaledObject or the HPA\n\
         # fallback renders, since that then owns the replica count.\n\
         replicaCount: 1\n\
         \n\
         image:\n\
         \x20 repository: {registry}/{app}\n\
         \x20 # -- Defaults to Chart appVersion\n\
         \x20 tag: \"\"\n\
         \x20 pullPolicy: IfNotPresent\n\
         \n\
         imagePullSecrets: []\n\
         nameOverride: \"\"\n\
         fullnameOverride: \"\"\n\
         \n",
        registry = c.image_registry,
        app = c.app_name,
    ));

    // Service account
    out.push_str(
        "serviceAccount:\n\
         \x20 create: true\n\
         \x20 annotations: {}\n\
         \x20 # -- If not set, name is generated from fullname\n\
         \x20 name: \"\"\n\
         \n",
    );

    // Pod annotations (Prometheus)
    out.push_str(&format!(
        "# -- Pod annotations (Prometheus scrape config included by default)\n\
         podAnnotations:\n\
         \x20 prometheus.io/scrape: \"true\"\n\
         \x20 prometheus.io/port: \"{port}\"\n\
         \x20 prometheus.io/path: \"{metrics_path}\"\n\
         \n\
         podLabels: {{}}\n\
         \n",
        port = c.metrics_port,
        metrics_path = c.health.metrics_path,
    ));

    // OTLP export.
    //
    // The scrape annotations above cover metrics; this covers traces. Leaving
    // endpoint empty does NOT disable OTel -- it only declines to override
    // whatever the app itself defaults to, and it only has any effect at all on
    // an app built with the otel features. `service.name` and the k8s.*
    // resource attrs are set on the container env, not here: those are derived,
    // not operator choices.
    out.push_str(
        "# -- OTLP export target. Only consulted by an app built with scalo's\n\
         # otel features; ignored otherwise. Empty leaves the app's own default\n\
         # in place rather than switching anything off. Example:\n\
         # http://opentelemetry-collector.observability:4317\n\
         # Spans can carry request attributes, so keep this in-cluster. To leave\n\
         # the cluster use https:// and set the OTel SDK's certificate env vars.\n\
         otel:\n\
         \x20 endpoint: \"\"\n\
         \x20 protocol: grpc\n\
         \n",
    );

    // Security contexts.
    //
    // The generated image already creates and switches to `appuser` (uid 1000),
    // so pinning the same uid here asserts what the image does rather than
    // changing it -- a mismatch is worth failing on, not papering over.
    //
    // readOnlyRootFilesystem is deliberately FALSE. The spool and DLQ write to
    // the container filesystem, and the only volume this chart mounts is the
    // read-only config map, so turning it on would break every app that spools.
    // An app that does not spool can set it true without forking the chart.
    out.push_str(
        "# -- Pod-level security context. Matches the uid the generated image\n\
         # switches to; change both together or the container will not start.\n\
         podSecurityContext:\n\
         \x20 runAsNonRoot: true\n\
         \x20 runAsUser: 1000\n\
         \x20 runAsGroup: 1000\n\
         \x20 fsGroup: 1000\n\
         \x20 seccompProfile:\n\
         \x20   type: RuntimeDefault\n\
         \n\
         # -- Container-level security context. readOnlyRootFilesystem stays\n\
         # false because the spool and DLQ write to disk and the only volume\n\
         # mounted here is the read-only config map; set it true only for an\n\
         # app that spools nowhere.\n\
         securityContext:\n\
         \x20 allowPrivilegeEscalation: false\n\
         \x20 privileged: false\n\
         \x20 readOnlyRootFilesystem: false\n\
         \x20 capabilities:\n\
         \x20   drop:\n\
         \x20     - ALL\n\
         \n",
    );

    // Resources
    out.push_str(
        "resources:\n\
         \x20 requests:\n\
         \x20   cpu: 250m\n\
         \x20   memory: 256Mi\n\
         \x20 limits:\n\
         \x20   cpu: \"2\"\n\
         \x20   memory: 1Gi\n\
         \n",
    );

    // Service
    out.push_str(&format!(
        "# -- Metrics and health endpoint service\n\
         service:\n\
         \x20 type: ClusterIP\n\
         \x20 port: {port}\n\
         \n",
        port = c.metrics_port,
    ));

    // App config section
    out.push_str(&format!(
        "# -- Application configuration (mounted as {})\n",
        c.config_mount_path
    ));
    if let Some(ref config) = c.default_config {
        out.push_str("config:\n");
        // Serialise the config value as YAML and indent by 2
        if let Ok(yaml) = serde_yaml_ng::to_string(config) {
            for line in yaml.lines() {
                if line == "---" {
                    continue;
                }
                out.push_str(&format!("  {line}\n"));
            }
        }
    } else {
        out.push_str("config: {}\n");
    }
    out.push('\n');

    // Secret sections
    for group in &c.secrets {
        out.push_str(&format!(
            "# -- {} credentials\n\
             {}:\n\
             \x20 existingSecret: \"\"\n\
             \x20 secretKeys:\n",
            group.group_name, group.group_name,
        ));
        for env in &group.env_vars {
            out.push_str(&format!("    {}: {}\n", env.key_name, env.secret_key));
        }
        for env in &group.env_vars {
            out.push_str(&format!("  {}: \"\"\n", env.key_name));
        }
        out.push('\n');
    }

    // KEDA section. Always emit a `keda:` block in values.yaml even when
    // the contract has no KEDA config -- templates reference
    // `.Values.keda.enabled` unconditionally, so the key must exist or
    // `helm lint` panics with "nil pointer evaluating interface
    // {}.enabled". When the contract opts out, the block is just
    // `enabled: false`.
    if let Some(keda) = c.enabled_keda() {
        out.push_str(&format!(
            "# -- KEDA autoscaling (requires KEDA operator installed)\n\
             keda:\n\
             \x20 enabled: true\n\
             \x20 minReplicaCount: {min}\n\
             \x20 maxReplicaCount: {max}\n\
             \x20 pollingInterval: {poll}\n\
             \x20 cooldownPeriod: {cool}\n",
            min = keda.min_replicas,
            max = keda.max_replicas,
            poll = keda.polling_interval,
            cool = keda.cooldown_period,
        ));
        // No Kafka lag trigger means nothing reads these, so none are offered.
        if keda.kafka_trigger.enabled {
            out.push_str(&format!(
                "\x20 kafka:\n\
                 \x20   # -- Scale when consumer group lag exceeds this per partition\n\
                 \x20   lagThreshold: \"{lag}\"\n\
                 \x20   # -- Wake from zero replicas when lag exceeds this\n\
                 \x20   activationLagThreshold: \"{activation}\"\n\
                 \x20   # -- Override topic (default: first topic from config)\n\
                 \x20   topic: \"\"\n\
                 \x20   # -- Override consumer group (default: from config)\n\
                 \x20   consumerGroup: \"\"\n",
                lag = keda.kafka_lag_threshold,
                activation = keda.activation_lag_threshold,
            ));
        }
        out.push_str(&format!(
            "\x20 cpu:\n\
             \x20   enabled: {cpu_enabled}\n\
             \x20   # -- CPU utilisation percentage threshold\n\
             \x20   threshold: \"{cpu_threshold}\"\n\
             \n",
            cpu_enabled = keda.cpu_enabled,
            cpu_threshold = keda.cpu_threshold,
        ));
    } else {
        out.push_str(
            "# -- KEDA autoscaling disabled by contract; HPA fallback below.\n\
             keda:\n\
             \x20 enabled: false\n\
             \n",
        );
    }

    // HPA fallback
    out.push_str(
        "# -- Standard HPA fallback (when KEDA is not installed)\n\
         # Mutually exclusive with keda.enabled\n\
         autoscaling:\n\
         \x20 enabled: false\n\
         \x20 minReplicas: 1\n\
         \x20 maxReplicas: 10\n\
         \x20 targetCPUUtilizationPercentage: 80\n\
         \n\
         nodeSelector: {}\n\
         tolerations: []\n\
         affinity: {}\n",
    );

    out
}

fn gen_helpers_tpl(c: &DeploymentContract) -> String {
    let app = &c.app_name;
    let mut out = String::with_capacity(2048);

    // Standard helpers
    out.push_str(&format!(
        r#"{{{{/*
Expand the name of the chart.
*/}}}}
{{{{- define "{app}.name" -}}}}
{{{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}}}
{{{{- end }}}}

{{{{/*
Create a default fully qualified app name.
Truncated at 63 chars because some K8s name fields are limited.
*/}}}}
{{{{- define "{app}.fullname" -}}}}
{{{{- if .Values.fullnameOverride }}}}
{{{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}}}
{{{{- else }}}}
{{{{- $name := default .Chart.Name .Values.nameOverride }}}}
{{{{- if contains $name .Release.Name }}}}
{{{{- .Release.Name | trunc 63 | trimSuffix "-" }}}}
{{{{- else }}}}
{{{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}}}
{{{{- end }}}}
{{{{- end }}}}
{{{{- end }}}}

{{{{/*
Create chart name and version as used by the chart label.
*/}}}}
{{{{- define "{app}.chart" -}}}}
{{{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}}}
{{{{- end }}}}

{{{{/*
Common labels.
*/}}}}
{{{{- define "{app}.labels" -}}}}
helm.sh/chart: {{{{ include "{app}.chart" . }}}}
{{{{ include "{app}.selectorLabels" . }}}}
{{{{- if .Chart.AppVersion }}}}
app.kubernetes.io/version: {{{{ .Chart.AppVersion | quote }}}}
{{{{- end }}}}
app.kubernetes.io/managed-by: {{{{ .Release.Service }}}}
{{{{- end }}}}

{{{{/*
Selector labels.
*/}}}}
{{{{- define "{app}.selectorLabels" -}}}}
app.kubernetes.io/name: {{{{ include "{app}.name" . }}}}
app.kubernetes.io/instance: {{{{ .Release.Name }}}}
{{{{- end }}}}

{{{{/*
Service account name.
*/}}}}
{{{{- define "{app}.serviceAccountName" -}}}}
{{{{- if .Values.serviceAccount.create }}}}
{{{{- default (include "{app}.fullname" .) .Values.serviceAccount.name }}}}
{{{{- else }}}}
{{{{- default "default" .Values.serviceAccount.name }}}}
{{{{- end }}}}
{{{{- end }}}}
"#,
    ));

    // Secret name helpers -- one per secret group
    for group in &c.secrets {
        let helper_name = format!("{}SecretName", to_camel_suffix(&group.group_name));
        out.push_str(&format!(
            r#"
{{{{/*
{group} secret name -- use existing or generate from fullname.
*/}}}}
{{{{- define "{app}.{helper}" -}}}}
{{{{- if .Values.{group}.existingSecret }}}}
{{{{- .Values.{group}.existingSecret }}}}
{{{{- else }}}}
{{{{- printf "%s-{group}" (include "{app}.fullname" .) }}}}
{{{{- end }}}}
{{{{- end }}}}
"#,
            app = app,
            group = group.group_name,
            helper = helper_name,
        ));
    }

    out
}

/// The container `env:` entries that make a pod identifiable to observability.
///
/// The three container probes, all pointed at the metrics port.
///
/// `startupProbe` targets the LIVENESS path. There is no startup endpoint:
/// Kubernetes suspends liveness until the startup probe passes, so one path
/// gives both a generous boot budget and a tight liveness period without the
/// two drifting apart.
fn gen_probes(c: &DeploymentContract) -> String {
    format!(
        "          livenessProbe:\n\
         \x20           httpGet:\n\
         \x20             path: {liveness}\n\
         \x20             port: metrics\n\
         \x20           initialDelaySeconds: 10\n\
         \x20           periodSeconds: 10\n\
         \x20           failureThreshold: 3\n\
         \x20         readinessProbe:\n\
         \x20           httpGet:\n\
         \x20             path: {readiness}\n\
         \x20             port: metrics\n\
         \x20           initialDelaySeconds: 5\n\
         \x20           periodSeconds: 5\n\
         \x20           failureThreshold: 2\n\
         \x20         startupProbe:\n\
         \x20           httpGet:\n\
         \x20             path: {liveness}\n\
         \x20             port: metrics\n\
         \x20           failureThreshold: 30\n\
         \x20           periodSeconds: 5\n",
        liveness = c.health.liveness_path,
        readiness = c.health.readiness_path,
    )
}

/// Per-pod / per-app differentiation rides on STANDARD OTel env vars plus
/// platform enrichment (Prometheus scrape labels, collector k8sattributes),
/// NEVER on metric names. The OTel SDK reads `OTEL_SERVICE_NAME` and
/// `OTEL_RESOURCE_ATTRIBUTES` natively, and the k8s Downward API feeds the
/// resource attrs the SDK cannot derive itself -- `k8s.pod.uid` is the anchor
/// the collector's k8sattributes processor keys on.
///
/// The OTLP endpoint is the other half of the picture: the scrape annotations
/// get metrics to Prometheus, this gets traces to a collector. Both pathways,
/// not one. It stays behind an `if` so that leaving `otel.endpoint` unset emits
/// no env var at all, leaving whatever the app itself defaults to untouched --
/// note that is NOT the same as switching OTel off, and none of it has any
/// effect on an app built without the otel features.
fn gen_observability_env(app: &str) -> String {
    let mut out = String::with_capacity(1024);
    out.push_str(&format!(
        "            - name: OTEL_SERVICE_NAME\n\
         \x20             value: \"{app}\"\n\
         \x20           - name: POD_NAME\n\
         \x20             valueFrom:\n\
         \x20               fieldRef:\n\
         \x20                 fieldPath: metadata.name\n\
         \x20           - name: POD_NAMESPACE\n\
         \x20             valueFrom:\n\
         \x20               fieldRef:\n\
         \x20                 fieldPath: metadata.namespace\n\
         \x20           - name: POD_UID\n\
         \x20             valueFrom:\n\
         \x20               fieldRef:\n\
         \x20                 fieldPath: metadata.uid\n\
         \x20           - name: NODE_NAME\n\
         \x20             valueFrom:\n\
         \x20               fieldRef:\n\
         \x20                 fieldPath: spec.nodeName\n\
         \x20           - name: OTEL_RESOURCE_ATTRIBUTES\n\
         \x20             value: k8s.pod.name=$(POD_NAME),k8s.namespace.name=$(POD_NAMESPACE),k8s.pod.uid=$(POD_UID),k8s.node.name=$(NODE_NAME)\n"
    ));
    out.push_str(
        // Parenthesised lookup so an operator who sets `otel: null`, or drops
        // the key from their values file, gets no env rather than a nil-pointer
        // template error.
        "            {{- if (.Values.otel).endpoint }}\n\
         \x20           - name: OTEL_EXPORTER_OTLP_ENDPOINT\n\
         \x20             value: {{ .Values.otel.endpoint | quote }}\n\
         \x20           - name: OTEL_EXPORTER_OTLP_PROTOCOL\n\
         \x20             value: {{ .Values.otel.protocol | quote }}\n\
         \x20           {{- end }}\n",
    );
    out
}

/// The template condition each extra port renders under, in `extra_ports`
/// order; `None` for a port that always listens.
fn port_gates(c: &DeploymentContract) -> Result<Vec<Option<String>>, DeploymentError> {
    c.extra_ports
        .iter()
        .map(|port| {
            port.when
                .as_ref()
                .map(|when| port_gate(&port.name, when))
                .transpose()
        })
        .collect()
}

/// Render a port condition as a Go template expression over a nil-safe lookup,
/// so a missing or null key reads as off rather than failing the render.
fn port_gate(port: &str, condition: &PortCondition) -> Result<String, DeploymentError> {
    let field = format!("extra_ports[{port}].when");
    let lookup = nil_safe_values_ref(&field, condition.path())?;
    Ok(match condition {
        PortCondition::Enabled { .. } => lookup,
        PortCondition::Equals { value, .. } => {
            format!("eq (toString {lookup}) {}", go_string_literal(value))
        }
        PortCondition::OneOf { values, .. } => {
            if values.is_empty() {
                return Err(DeploymentError::InvalidContract {
                    field,
                    reason: "a one_of condition with no values never holds, so the port would \
                             never render"
                        .to_string(),
                });
            }
            let choices: Vec<String> = values.iter().map(|v| go_string_literal(v)).collect();
            format!("has (toString {lookup}) (list {})", choices.join(" "))
        }
    })
}

/// A double-quoted Go template string for `s`; every JSON string escape is also
/// a valid Go escape, so a quote or backslash in a value cannot end the literal.
fn go_string_literal(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_default()
}

/// Wrap one rendered list entry in its port's template condition, if it has one.
fn push_gated(out: &mut String, indent: &str, gate: Option<&String>, entry: &str) {
    if let Some(gate) = gate {
        out.push_str(&format!("{indent}{{{{- if {gate} }}}}\n"));
    }
    out.push_str(entry);
    if gate.is_some() {
        out.push_str(&format!("{indent}{{{{- end }}}}\n"));
    }
}

/// When the fallback HPA renders: asked for, and KEDA not in charge.
const HPA_GATE: &str = "and .Values.autoscaling.enabled (not .Values.keda.enabled)";

/// When the ScaledObject renders. With no Kafka lag trigger the CPU trigger is
/// the only one, and a ScaledObject with no triggers is rejected, so it exists
/// only while CPU scaling is on.
fn scaled_object_gate(keda: &KedaContract) -> &'static str {
    if keda.kafka_trigger.enabled {
        ".Values.keda.enabled"
    } else {
        "and .Values.keda.enabled .Values.keda.cpu.enabled"
    }
}

/// When the Deployment sets `replicas` itself: exactly when neither the
/// ScaledObject nor the HPA renders, because whichever renders owns the count
/// and a Deployment without `replicas` runs one pod.
fn replicas_gate(c: &DeploymentContract) -> String {
    match c.enabled_keda() {
        // The HPA renders only while KEDA is off, so beside a ScaledObject
        // gated on KEDA alone the two conditions fold into one `or`.
        Some(keda) if keda.kafka_trigger.enabled => {
            "not (or .Values.keda.enabled .Values.autoscaling.enabled)".to_string()
        }
        Some(keda) => format!("not (or ({}) ({HPA_GATE}))", scaled_object_gate(keda)),
        None => format!("not ({HPA_GATE})"),
    }
}

fn gen_deployment_yaml(c: &DeploymentContract, gates: &[Option<String>]) -> String {
    let app = &c.app_name;
    let replicas_if = replicas_gate(c);
    let mut out = String::with_capacity(4096);

    // Header
    out.push_str(&format!(
        r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{{{ include "{app}.fullname" . }}}}
  labels:
    {{{{- include "{app}.labels" . | nindent 4 }}}}
spec:
  {{{{- if {replicas_if} }}}}
  replicas: {{{{ .Values.replicaCount }}}}
  {{{{- end }}}}
  selector:
    matchLabels:
      {{{{- include "{app}.selectorLabels" . | nindent 6 }}}}
  template:
    metadata:
      annotations:
        checksum/config: {{{{ include (print $.Template.BasePath "/configmap.yaml") . | sha256sum }}}}
        {{{{- with .Values.podAnnotations }}}}
        {{{{- toYaml . | nindent 8 }}}}
        {{{{- end }}}}
      labels:
        {{{{- include "{app}.labels" . | nindent 8 }}}}
        {{{{- with .Values.podLabels }}}}
        {{{{- toYaml . | nindent 8 }}}}
        {{{{- end }}}}
    spec:
      {{{{- with .Values.imagePullSecrets }}}}
      imagePullSecrets:
        {{{{- toYaml . | nindent 8 }}}}
      {{{{- end }}}}
      serviceAccountName: {{{{ include "{app}.serviceAccountName" . }}}}
      {{{{- with .Values.podSecurityContext }}}}
      securityContext:
        {{{{- toYaml . | nindent 8 }}}}
      {{{{- end }}}}
      containers:
        - name: {{{{ .Chart.Name }}}}
          image: "{{{{ .Values.image.repository }}}}:{{{{ .Values.image.tag | default .Chart.AppVersion }}}}"
          imagePullPolicy: {{{{ .Values.image.pullPolicy }}}}
"#,
    ));

    // Container security context. Kept separate from the pod-level block
    // above because the two settle different things: the pod block decides
    // WHO the process runs as, the container block decides what it may then
    // do. Both are values-driven so an app with a genuine need (a capability,
    // a writable root) can opt back out without forking the chart.
    out.push_str(
        "          {{- with .Values.securityContext }}\n\
         \x20         securityContext:\n\
         \x20           {{- toYaml . | nindent 12 }}\n\
         \x20         {{- end }}\n",
    );

    // Args
    if !c.entrypoint_args.is_empty() {
        out.push_str("          args:\n");
        for arg in &c.entrypoint_args {
            out.push_str(&format!("            - \"{arg}\"\n"));
        }
    }

    // Ports
    out.push_str(
        "          ports:\n\
         \x20           - name: metrics\n\
         \x20             containerPort: {{ .Values.service.port }}\n\
         \x20             protocol: TCP\n",
    );
    for (port, gate) in c.extra_ports.iter().zip(gates) {
        // Kubernetes accepts only TCP, UDP and SCTP, in upper case.
        let entry = format!(
            "            - name: {name}\n\
             \x20             containerPort: {port}\n\
             \x20             protocol: {proto}\n",
            name = port.name,
            port = port.port,
            proto = port.protocol.to_ascii_uppercase(),
        );
        push_gated(&mut out, "            ", gate.as_ref(), &entry);
    }

    // Env: observability identity first, then secret-backed credentials.
    out.push_str("          env:\n");
    out.push_str(&gen_observability_env(app));

    // Env vars from secrets. No emptiness guard: it used to suppress the `env:`
    // header, which now always precedes this, so it would only be wrapping a
    // loop that already iterates zero times.
    for group in &c.secrets {
        let helper_name = format!("{}SecretName", to_camel_suffix(&group.group_name));
        out.push_str(&format!(
            "            # {} credentials via Secret (figment env cascade overrides file config)\n",
            group.group_name
        ));
        for env in &group.env_vars {
            // See gen_secret_yaml -- hyphenated keys must use index form.
            let key_lookup = safe_template_lookup(
                &format!(".Values.{}.secretKeys", group.group_name),
                &env.key_name,
            );
            out.push_str(&format!(
                "            - name: {env_var}\n\
                 \x20             valueFrom:\n\
                 \x20               secretKeyRef:\n\
                 \x20                 name: {{{{ include \"{app}.{helper}\" . }}}}\n\
                 \x20                 key: {{{{ {key_lookup} }}}}\n",
                env_var = env.env_var,
                app = app,
                helper = helper_name,
            ));
        }
    }

    out.push_str(&gen_probes(c));

    // Volume mounts
    out.push_str(&format!(
        "          volumeMounts:\n\
         \x20           - name: config\n\
         \x20             mountPath: {config_dir}\n\
         \x20             readOnly: true\n",
        config_dir = c.config_dir(),
    ));

    // Resources
    out.push_str(
        "          {{- with .Values.resources }}\n\
         \x20         resources:\n\
         \x20           {{- toYaml . | nindent 12 }}\n\
         \x20         {{- end }}\n",
    );

    // Volumes
    out.push_str(&format!(
        "      volumes:\n\
         \x20       - name: config\n\
         \x20         configMap:\n\
         \x20           name: {{{{ include \"{app}.fullname\" . }}}}-config\n",
    ));

    // Node selector, affinity, tolerations
    out.push_str(
        "      {{- with .Values.nodeSelector }}\n\
         \x20     nodeSelector:\n\
         \x20       {{- toYaml . | nindent 8 }}\n\
         \x20     {{- end }}\n\
         \x20     {{- with .Values.affinity }}\n\
         \x20     affinity:\n\
         \x20       {{- toYaml . | nindent 8 }}\n\
         \x20     {{- end }}\n\
         \x20     {{- with .Values.tolerations }}\n\
         \x20     tolerations:\n\
         \x20       {{- toYaml . | nindent 8 }}\n\
         \x20     {{- end }}\n",
    );

    out
}

fn gen_service_yaml(c: &DeploymentContract, gates: &[Option<String>]) -> String {
    let app = &c.app_name;
    let mut out = format!(
        r#"apiVersion: v1
kind: Service
metadata:
  name: {{{{ include "{app}.fullname" . }}}}
  labels:
    {{{{- include "{app}.labels" . | nindent 4 }}}}
spec:
  type: {{{{ .Values.service.type }}}}
  ports:
    - port: {{{{ .Values.service.port }}}}
      targetPort: metrics
      protocol: TCP
      name: metrics
"#,
    );

    // Extra ports
    for (port, gate) in c.extra_ports.iter().zip(gates) {
        let entry = format!(
            "    - port: {port}\n\
             \x20     targetPort: {port}\n\
             \x20     protocol: {proto}\n\
             \x20     name: {name}\n",
            port = port.port,
            proto = port.protocol.to_ascii_uppercase(),
            name = port.name,
        );
        push_gated(&mut out, "    ", gate.as_ref(), &entry);
    }

    out.push_str(&format!(
        "  selector:\n\
         \x20   {{{{- include \"{app}.selectorLabels\" . | nindent 4 }}}}\n",
    ));

    out
}

fn gen_serviceaccount_yaml(c: &DeploymentContract) -> String {
    let app = &c.app_name;
    format!(
        r#"{{{{- if .Values.serviceAccount.create -}}}}
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{{{ include "{app}.serviceAccountName" . }}}}
  labels:
    {{{{- include "{app}.labels" . | nindent 4 }}}}
  {{{{- with .Values.serviceAccount.annotations }}}}
  annotations:
    {{{{- toYaml . | nindent 4 }}}}
  {{{{- end }}}}
automountServiceAccountToken: false
{{{{- end }}}}
"#,
    )
}

fn gen_configmap_yaml(c: &DeploymentContract) -> String {
    let app = &c.app_name;

    let mut out = format!(
        r#"apiVersion: v1
kind: ConfigMap
metadata:
  name: {{{{ include "{app}.fullname" . }}}}-config
  labels:
    {{{{- include "{app}.labels" . | nindent 4 }}}}
data:
  {filename}: |
    {{{{- toYaml .Values.config | nindent 4 }}}}
"#,
        app = app,
        filename = c.config_filename(),
    );

    let _ = &mut out; // keep borrow checker happy
    out
}

fn gen_secret_yaml(c: &DeploymentContract) -> String {
    let app = &c.app_name;
    let mut out = String::new();
    let mut first = true;

    for group in &c.secrets {
        if !first {
            out.push_str("---\n");
        }
        first = false;

        let helper_name = format!("{}SecretName", to_camel_suffix(&group.group_name));

        out.push_str(&format!(
            "{{{{- if not .Values.{group}.existingSecret }}}}\n\
             apiVersion: v1\n\
             kind: Secret\n\
             metadata:\n\
             \x20 name: {{{{ include \"{app}.{helper}\" . }}}}\n\
             \x20 labels:\n\
             \x20   {{{{- include \"{app}.labels\" . | nindent 4 }}}}\n\
             type: Opaque\n\
             data:\n",
            group = group.group_name,
            app = app,
            helper = helper_name,
        ));

        for env in &group.env_vars {
            // Hyphenated or otherwise non-Go-identifier-safe key names
            // require `(index .Values.x "key")` form instead of
            // `.Values.x.key` -- Go-template parser rejects hyphens etc.
            let key_lookup = safe_template_lookup(
                &format!(".Values.{}.secretKeys", group.group_name),
                &env.key_name,
            );
            let val_lookup =
                safe_template_lookup(&format!(".Values.{}", group.group_name), &env.key_name);
            out.push_str(&format!(
                "  {{{{ {key_lookup} }}}}: {{{{ {val_lookup} | b64enc | quote }}}}\n"
            ));
        }

        out.push_str("{{- end }}\n");
    }

    if c.secrets.is_empty() {
        out.push_str("# No secrets defined in deployment contract\n");
    }

    out
}

fn gen_hpa_yaml(c: &DeploymentContract) -> String {
    let app = &c.app_name;
    format!(
        r#"{{{{- if {HPA_GATE} }}}}
# Standard HPA fallback -- use when KEDA operator is not installed.
# Mutually exclusive with keda.enabled (KEDA creates its own HPA).
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {{{{ include "{app}.fullname" . }}}}
  labels:
    {{{{- include "{app}.labels" . | nindent 4 }}}}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: {{{{ include "{app}.fullname" . }}}}
  minReplicas: {{{{ .Values.autoscaling.minReplicas }}}}
  maxReplicas: {{{{ .Values.autoscaling.maxReplicas }}}}
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: {{{{ .Values.autoscaling.targetCPUUtilizationPercentage }}}}
{{{{- end }}}}
"#,
    )
}

/// Render a dotted `.Values`-relative path as a parenthesised lookup, so a
/// missing or null parent renders nothing instead of a nil-pointer error.
///
/// `config.source.brokers` becomes `((.Values.config).source).brokers`.
fn nil_safe_values_ref(field: &str, path: &str) -> Result<String, DeploymentError> {
    let mut lookup = String::from(".Values");
    for (depth, segment) in path.split('.').enumerate() {
        if !is_go_identifier(segment) {
            return Err(DeploymentError::InvalidContract {
                field: field.to_string(),
                reason: format!(
                    "`{path}` is not a dotted path of Go identifiers, so the chart cannot address it"
                ),
            });
        }
        lookup = if depth == 0 {
            format!("{lookup}.{segment}")
        } else {
            format!("({lookup}).{segment}")
        };
    }
    Ok(lookup)
}

fn gen_keda_scaledobject_yaml(
    c: &DeploymentContract,
    keda: &KedaContract,
) -> Result<String, DeploymentError> {
    let app = &c.app_name;
    let kafka_enabled = keda.kafka_trigger.enabled;

    if !kafka_enabled && !keda.cpu_enabled {
        return Err(DeploymentError::InvalidContract {
            field: "keda".to_string(),
            reason: "the Kafka lag trigger and the CPU trigger are both off, so KEDA would \
                     have nothing to scale on; set `keda: None` to turn autoscaling off"
                .to_string(),
        });
    }

    let gate = scaled_object_gate(keda);
    let (kafka_trigger, cpu_role) = if kafka_enabled {
        (
            gen_keda_kafka_trigger(c, &keda.kafka_trigger)?,
            "secondary scaler",
        )
    } else {
        (String::new(), "only scaler")
    };

    Ok(format!(
        r#"{{{{- if {gate} }}}}
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: {{{{ include "{app}.fullname" . }}}}
  labels:
    {{{{- include "{app}.labels" . | nindent 4 }}}}
spec:
  scaleTargetRef:
    name: {{{{ include "{app}.fullname" . }}}}
  minReplicaCount: {{{{ .Values.keda.minReplicaCount }}}}
  maxReplicaCount: {{{{ .Values.keda.maxReplicaCount }}}}
  pollingInterval: {{{{ .Values.keda.pollingInterval }}}}
  cooldownPeriod: {{{{ .Values.keda.cooldownPeriod }}}}
  triggers:
{kafka_trigger}    {{{{- if .Values.keda.cpu.enabled }}}}
    # CPU utilisation ({cpu_role})
    - type: cpu
      metricType: Utilization
      metadata:
        value: {{{{ .Values.keda.cpu.threshold | quote }}}}
    {{{{- end }}}}
{{{{- end }}}}
"#,
    ))
}

/// The Kafka consumer-group lag trigger, reading its connection details from
/// the values paths the contract names.
fn gen_keda_kafka_trigger(
    c: &DeploymentContract,
    trigger: &KafkaLagTrigger,
) -> Result<String, DeploymentError> {
    let app = &c.app_name;
    let brokers = nil_safe_values_ref("keda.kafka_trigger.brokers_path", &trigger.brokers_path)?;
    let group = nil_safe_values_ref("keda.kafka_trigger.group_path", &trigger.group_path)?;
    let topics = nil_safe_values_ref("keda.kafka_trigger.topics_path", &trigger.topics_path)?;

    // A TriggerAuthentication exists only for a kafka secret group, and a SASL
    // mechanism without its credentials cannot authenticate.
    let has_kafka_secret = c.secrets.iter().any(|g| g.group_name == "kafka");
    let (auth_ref, sasl) = if has_kafka_secret {
        (
            format!(
                "      authenticationRef:\n\
                 \x20       name: {{{{ include \"{app}.fullname\" . }}}}-kafka-auth\n"
            ),
            "        # `sasl`, not `saslType`: the kafka trigger replaced the old `authMode`\n\
             \x20       # property with `sasl` + `tls`, and an unrecognised key is ignored, so\n\
             \x20       # the mechanism was being supplied nowhere at all.\n\
             \x20       sasl: scram_sha512\n",
        )
    } else {
        (String::new(), "")
    };

    Ok(format!(
        r#"    # Kafka consumer group lag (primary scaler)
    - type: kafka
{auth_ref}      metadata:
        bootstrapServers: {{{{ join "," {brokers} | quote }}}}
        consumerGroup: {{{{ .Values.keda.kafka.consumerGroup | default {group} | quote }}}}
        {{{{- /* A conditional, not `default`, which evaluates both operands; and the
            topics are joined then split, not indexed, as `index` on a string yields a byte. */}}}}
        {{{{- $topics := join "," {topics} }}}}
        {{{{- if .Values.keda.kafka.topic }}}}
        topic: {{{{ .Values.keda.kafka.topic | quote }}}}
        {{{{- else if $topics }}}}
        topic: {{{{ splitList "," $topics | first | quote }}}}
        {{{{- else }}}}
        topic: ""
        {{{{- end }}}}
        lagThreshold: {{{{ .Values.keda.kafka.lagThreshold | quote }}}}
        activationLagThreshold: {{{{ .Values.keda.kafka.activationLagThreshold | quote }}}}
{sasl}        tls: disable
"#,
    ))
}

fn gen_keda_triggerauth_yaml(c: &DeploymentContract, keda: &KedaContract) -> String {
    let app = &c.app_name;

    // Written as a stub rather than omitted, so the chart's file set does not
    // depend on which triggers are on.
    if !keda.kafka_trigger.enabled {
        return "# No Kafka lag trigger -- KEDA TriggerAuthentication not generated\n".to_string();
    }

    // Find the kafka secret group
    let kafka_group = c.secrets.iter().find(|g| g.group_name == "kafka");

    if kafka_group.is_none() {
        return "# No kafka secret group -- KEDA TriggerAuthentication not generated\n".to_string();
    }

    let helper_name = format!("{}SecretName", to_camel_suffix("kafka"));

    format!(
        r#"{{{{- if .Values.keda.enabled }}}}
apiVersion: keda.sh/v1alpha1
kind: TriggerAuthentication
metadata:
  name: {{{{ include "{app}.fullname" . }}}}-kafka-auth
  labels:
    {{{{- include "{app}.labels" . | nindent 4 }}}}
spec:
  secretTargetRef:
    # KEDA's `sasl` parameter is the MECHANISM, not the username -- it takes
    # plaintext | scram_sha256 | scram_sha512 | none, and the username is its
    # own parameter. Supplying no `username` at all means SASL authentication
    # cannot succeed, so the scaler never reads consumer-group lag and the app
    # never scales. The mechanism itself rides the trigger metadata.
    - parameter: username
      name: {{{{ include "{app}.{helper_name}" . }}}}
      key: {{{{ .Values.kafka.secretKeys.username }}}}
    - parameter: password
      name: {{{{ include "{app}.{helper_name}" . }}}}
      key: {{{{ .Values.kafka.secretKeys.password }}}}
{{{{- end }}}}
"#,
    )
}

fn gen_notes_txt(c: &DeploymentContract) -> String {
    let app = &c.app_name;

    format!(
        r#"{app} has been deployed.

1. Get the metrics/health endpoint:
   kubectl port-forward svc/{{{{ include "{app}.fullname" . }}}} {{{{ .Values.service.port }}}}:{{{{ .Values.service.port }}}}
   curl http://localhost:{{{{ .Values.service.port }}}}{liveness}
   curl http://localhost:{{{{ .Values.service.port }}}}{metrics}

{{{{- if .Values.keda.enabled }}}}

2. Check KEDA autoscaling status:
   kubectl get scaledobject {{{{ include "{app}.fullname" . }}}}
   kubectl get hpa
{{{{- end }}}}

3. View logs:
   kubectl logs -l app.kubernetes.io/name={{{{ include "{app}.name" . }}}} -f
"#,
        app = app,
        liveness = c.health.liveness_path,
        metrics = c.health.metrics_path,
    )
}
