// Project:   scalo
// File:      src/otel_resource.rs
// Purpose:   Replica identity on the OTLP resource
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Replica identity for the OTLP resource metrics and spans carry.
//!
//! Without `service.instance.id` every replica of a service pushes one series,
//! the replicas overwrite each other, and a rate over the pushed copy is
//! meaningless. The id comes from the first of:
//!
//! 1. `OTEL_RESOURCE_ATTRIBUTES`, which the SDK's own detector reads
//! 2. `POD_NAME`, set by a downward-API env entry
//! 3. `HOSTNAME`, the pod name in Kubernetes unless the pod spec sets `hostname`
//! 4. an id generated once per process, so metrics and spans agree
//!
//! `k8s.pod.name` comes from `OTEL_RESOURCE_ATTRIBUTES`, then `POD_NAME`, then
//! `HOSTNAME` when running in Kubernetes, and is left off otherwise.

use std::sync::OnceLock;

use opentelemetry::{Key, KeyValue};
use opentelemetry_sdk::resource::{EnvResourceDetector, ResourceDetector};

/// Semantic-convention key for the replica's identity.
const SERVICE_INSTANCE_ID: &str = "service.instance.id";

/// Semantic-convention key for the Kubernetes pod name.
const K8S_POD_NAME: &str = "k8s.pod.name";

/// `service.instance.id` and, when the pod can be named, `k8s.pod.name`,
/// leaving out any key `OTEL_RESOURCE_ATTRIBUTES` already sets so the
/// operator's value stays.
#[must_use]
pub(crate) fn instance_attributes() -> Vec<KeyValue> {
    let from_env = EnvResourceDetector::new().detect();
    let pod_name = non_empty_var("POD_NAME");
    let hostname = non_empty_var("HOSTNAME");

    let mut attributes = Vec::with_capacity(2);
    if from_env
        .get(&Key::from_static_str(SERVICE_INSTANCE_ID))
        .is_none()
    {
        let id = pod_name
            .clone()
            .or_else(|| hostname.clone())
            .unwrap_or_else(|| generated_instance_id().to_string());
        attributes.push(KeyValue::new(SERVICE_INSTANCE_ID, id));
    }
    if from_env.get(&Key::from_static_str(K8S_POD_NAME)).is_none()
        && let Some(pod) = pod_name
            .or_else(|| hostname.filter(|_| crate::env::Environment::detect().is_kubernetes()))
    {
        attributes.push(KeyValue::new(K8S_POD_NAME, pod));
    }
    attributes
}

/// The variable's trimmed value, or `None` when it is unset or blank.
fn non_empty_var(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// A random id for this process, the same on every call.
fn generated_instance_id() -> &'static str {
    static ID: OnceLock<String> = OnceLock::new();
    ID.get_or_init(|| {
        use std::hash::{BuildHasher, Hasher};

        // RandomState is keyed from the OS's randomness, so two replicas differ.
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_u32(std::process::id());
        format!("{:016x}", hasher.finish())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every variable the identity reads, so the host's own cannot leak in.
    fn cleared() -> Vec<(&'static str, Option<&'static str>)> {
        vec![
            ("OTEL_RESOURCE_ATTRIBUTES", None),
            ("POD_NAME", None),
            ("HOSTNAME", None),
            ("KUBERNETES_SERVICE_HOST", None),
        ]
    }

    /// `cleared()` with `set` applied on top.
    fn vars(set: &[(&'static str, &'static str)]) -> Vec<(&'static str, Option<&'static str>)> {
        let mut all = cleared();
        for &(name, value) in set {
            all.retain(|&(n, _)| n != name);
            all.push((name, Some(value)));
        }
        all
    }

    fn get(attributes: &[KeyValue], key: &str) -> Option<String> {
        attributes
            .iter()
            .find(|kv| kv.key.as_str() == key)
            .map(|kv| kv.value.to_string())
    }

    #[test]
    fn otel_resource_attributes_names_the_instance_first() {
        temp_env::with_vars(
            vars(&[
                (
                    "OTEL_RESOURCE_ATTRIBUTES",
                    "service.instance.id=from-otel,k8s.pod.name=pod-otel",
                ),
                ("POD_NAME", "pod-a"),
                ("HOSTNAME", "host-b"),
            ]),
            || {
                let attributes = instance_attributes();
                assert_eq!(
                    get(&attributes, SERVICE_INSTANCE_ID),
                    None,
                    "left to the SDK detector, which already carries from-otel"
                );
                assert_eq!(get(&attributes, K8S_POD_NAME), None);

                let resource = opentelemetry_sdk::Resource::builder()
                    .with_attributes(attributes)
                    .build();
                let id = resource.get(&Key::from_static_str(SERVICE_INSTANCE_ID));
                assert_eq!(id.map(|v| v.to_string()).as_deref(), Some("from-otel"));
                let pod = resource.get(&Key::from_static_str(K8S_POD_NAME));
                assert_eq!(pod.map(|v| v.to_string()).as_deref(), Some("pod-otel"));
            },
        );
    }

    #[test]
    fn pod_name_is_next_when_otel_resource_attributes_names_no_instance() {
        temp_env::with_vars(
            vars(&[
                ("OTEL_RESOURCE_ATTRIBUTES", "team=data-plane"),
                ("POD_NAME", "pod-a"),
                ("HOSTNAME", "host-b"),
            ]),
            || {
                let attributes = instance_attributes();
                assert_eq!(
                    get(&attributes, SERVICE_INSTANCE_ID).as_deref(),
                    Some("pod-a")
                );
                assert_eq!(get(&attributes, K8S_POD_NAME).as_deref(), Some("pod-a"));
            },
        );
    }

    #[test]
    fn hostname_is_next_and_names_the_pod_in_kubernetes() {
        temp_env::with_vars(
            vars(&[
                ("HOSTNAME", "receiver-6f8d-q2w3e"),
                ("KUBERNETES_SERVICE_HOST", "10.0.0.1"),
            ]),
            || {
                let attributes = instance_attributes();
                assert_eq!(
                    get(&attributes, SERVICE_INSTANCE_ID).as_deref(),
                    Some("receiver-6f8d-q2w3e")
                );
                assert_eq!(
                    get(&attributes, K8S_POD_NAME).as_deref(),
                    Some("receiver-6f8d-q2w3e")
                );
            },
        );
    }

    #[test]
    fn hostname_outside_kubernetes_names_the_instance_but_no_pod() {
        temp_env::with_vars(vars(&[("HOSTNAME", "build-box")]), || {
            let attributes = instance_attributes();
            assert_eq!(
                get(&attributes, SERVICE_INSTANCE_ID).as_deref(),
                Some("build-box")
            );
            // A runner inside a pod is still in Kubernetes, so only assert off it.
            if !crate::env::Environment::detect().is_kubernetes() {
                assert_eq!(get(&attributes, K8S_POD_NAME), None);
            }
        });
    }

    #[test]
    fn a_generated_id_is_last_and_stable_within_the_process() {
        temp_env::with_vars(vars(&[("POD_NAME", "  ")]), || {
            let first =
                get(&instance_attributes(), SERVICE_INSTANCE_ID).expect("an id is always set");
            assert_eq!(first.len(), 16, "16 hex digits, got {first:?}");
            assert!(first.bytes().all(|b| b.is_ascii_hexdigit()), "{first:?}");
            assert_eq!(
                get(&instance_attributes(), SERVICE_INSTANCE_ID).as_deref(),
                Some(first.as_str()),
                "metrics and spans build their resources separately and must agree"
            );
        });
    }
}
