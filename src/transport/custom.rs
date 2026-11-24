// Project:   scalo
// File:      src/transport/custom.rs
// Purpose:   Dyn-compatible sender trait and senders registered by type name
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Senders an app writes itself, built from config like a built-in transport.
//!
//! [`TransportSender`] returns `impl Future`, so it has no `dyn` form.
//! [`DynTransportSender`] is the same surface with boxed futures, implemented
//! for every `TransportSender` by a blanket impl, so an app implements only
//! `TransportSender` and never this trait. [`AnySender::Custom`](super::AnySender::Custom)
//! holds one behind an `Arc`, and routing and `TieredSink` take it like any
//! other `AnySender`.
//!
//! [`register_sender`] puts a constructor under a type name. Config whose
//! `type` names it builds through [`AnySender::from_config`](super::AnySender::from_config),
//! and the constructor reads its own settings from the section named after
//! the type. The guide with worked examples is `docs/transport/custom.md`.
//!
//! ```
//! use scalo::transport::{
//!     AnySender, DeliveryGuarantee, SendResult, TransportBase, TransportConfig,
//!     TransportResult, TransportSender, TransportType, register_sender,
//! };
//!
//! /// Counts what it is given and keeps nothing.
//! struct Counter;
//!
//! impl TransportBase for Counter {
//!     async fn close(&self) -> TransportResult<()> {
//!         Ok(())
//!     }
//!     fn is_healthy(&self) -> bool {
//!         true
//!     }
//!     fn name(&self) -> &'static str {
//!         "counter"
//!     }
//!     fn delivery_guarantee(&self) -> DeliveryGuarantee {
//!         DeliveryGuarantee::Acknowledged
//!     }
//! }
//!
//! impl TransportSender for Counter {
//!     async fn send(&self, _destination: &str, _payload: bytes::Bytes) -> SendResult {
//!         SendResult::Ok
//!     }
//! }
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> TransportResult<()> {
//! register_sender("counter", |_config| async { Ok(Counter) })?;
//!
//! let config = TransportConfig {
//!     transport_type: TransportType::Custom("counter".into()),
//!     ..TransportConfig::default()
//! };
//! let sender = AnySender::from_transport_config(&config).await?;
//! assert!(sender.send("events", bytes::Bytes::from_static(b"{}")).await.is_ok());
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use serde::de::DeserializeOwned;

use super::ack::{DeadLetterReason, SinkConfirmation};
use super::delivery::DeliveryGuarantee;
use super::error::{TransportError, TransportResult};
use super::traits::TransportSender;
use super::types::{SendResult, TransportType};
use super::work_batch::Record;
use crate::registry::Registry;

/// A boxed, `Send` future borrowing for `'a`.
pub type BoxSendFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// [`TransportBase`](super::TransportBase) plus [`TransportSender`] with boxed futures, so a sender
/// can sit behind `Arc<dyn DynTransportSender>`.
///
/// Implemented for every `TransportSender` by the blanket impl below. Each
/// method is the trait method of the same name with a `dyn_` prefix, so the two
/// never clash where both traits are in scope. Call it through
/// [`AnySender::Custom`](super::AnySender::Custom), which implements
/// `TransportSender` again.
pub trait DynTransportSender: Send + Sync {
    /// [`TransportBase::close`](super::TransportBase::close).
    fn dyn_close(&self) -> BoxSendFuture<'_, TransportResult<()>>;
    /// [`TransportBase::is_healthy`](super::TransportBase::is_healthy).
    fn dyn_is_healthy(&self) -> bool;
    /// [`TransportBase::name`](super::TransportBase::name).
    fn dyn_name(&self) -> &'static str;
    /// [`TransportBase::healthcheck`](super::TransportBase::healthcheck).
    fn dyn_healthcheck(&self) -> BoxSendFuture<'_, TransportResult<()>>;
    /// [`TransportBase::delivery_guarantee`](super::TransportBase::delivery_guarantee).
    fn dyn_delivery_guarantee(&self) -> DeliveryGuarantee;
    /// [`TransportSender::send`].
    fn dyn_send<'a>(
        &'a self,
        destination: &'a str,
        payload: bytes::Bytes,
    ) -> BoxSendFuture<'a, SendResult>;
    /// [`TransportSender::send_record`].
    fn dyn_send_record<'a>(&'a self, record: &'a Record) -> BoxSendFuture<'a, SendResult>;
    /// [`TransportSender::send_batch`].
    fn dyn_send_batch<'a>(&'a self, records: &'a [Record]) -> BoxSendFuture<'a, SendResult>;
    /// [`TransportSender::send_batch_by`].
    fn dyn_send_batch_by<'a>(
        &'a self,
        records: &'a [Record],
        deadline: Instant,
    ) -> BoxSendFuture<'a, SendResult>;
    /// [`TransportSender::confirms_delivery`].
    fn dyn_confirms_delivery(&self) -> SinkConfirmation;
    /// [`TransportSender::dead_letter_reason`].
    fn dyn_dead_letter_reason(&self, record: &Record) -> Option<DeadLetterReason>;
    /// [`TransportSender::screen`].
    fn dyn_screen(&self, record: &Record) -> super::ack::Screened;
    /// [`TransportSender::send_screened`].
    fn dyn_send_screened<'a>(
        &'a self,
        records: &'a [Record],
        deadline: Option<Instant>,
    ) -> BoxSendFuture<'a, SendResult>;
}

impl<T: TransportSender> DynTransportSender for T {
    fn dyn_close(&self) -> BoxSendFuture<'_, TransportResult<()>> {
        Box::pin(self.close())
    }
    fn dyn_is_healthy(&self) -> bool {
        self.is_healthy()
    }
    fn dyn_name(&self) -> &'static str {
        self.name()
    }
    fn dyn_healthcheck(&self) -> BoxSendFuture<'_, TransportResult<()>> {
        Box::pin(self.healthcheck())
    }
    fn dyn_delivery_guarantee(&self) -> DeliveryGuarantee {
        self.delivery_guarantee()
    }
    fn dyn_send<'a>(
        &'a self,
        destination: &'a str,
        payload: bytes::Bytes,
    ) -> BoxSendFuture<'a, SendResult> {
        Box::pin(self.send(destination, payload))
    }
    fn dyn_send_record<'a>(&'a self, record: &'a Record) -> BoxSendFuture<'a, SendResult> {
        Box::pin(self.send_record(record))
    }
    fn dyn_send_batch<'a>(&'a self, records: &'a [Record]) -> BoxSendFuture<'a, SendResult> {
        Box::pin(self.send_batch(records))
    }
    fn dyn_send_batch_by<'a>(
        &'a self,
        records: &'a [Record],
        deadline: Instant,
    ) -> BoxSendFuture<'a, SendResult> {
        Box::pin(self.send_batch_by(records, deadline))
    }
    fn dyn_confirms_delivery(&self) -> SinkConfirmation {
        self.confirms_delivery()
    }
    fn dyn_dead_letter_reason(&self, record: &Record) -> Option<DeadLetterReason> {
        self.dead_letter_reason(record)
    }
    fn dyn_screen(&self, record: &Record) -> super::ack::Screened {
        self.screen(record)
    }
    fn dyn_send_screened<'a>(
        &'a self,
        records: &'a [Record],
        deadline: Option<Instant>,
    ) -> BoxSendFuture<'a, SendResult> {
        Box::pin(self.send_screened(records, deadline))
    }
}

/// What a registered constructor is given: its type name and its config section.
#[derive(Debug, Clone)]
pub struct CustomSenderConfig {
    type_name: String,
    section: Option<serde_json::Value>,
}

impl CustomSenderConfig {
    /// The config for `type_name`, with the section read under that name.
    #[must_use]
    pub fn new(type_name: impl Into<String>, section: Option<serde_json::Value>) -> Self {
        Self {
            type_name: type_name.into(),
            section,
        }
    }

    /// The type name the config selected.
    #[must_use]
    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    /// Whether the config had a section under the type name.
    #[must_use]
    pub fn has_section(&self) -> bool {
        self.section.is_some()
    }

    /// The section under the type name, as `T`. An absent section reads as an
    /// empty map, so a `T` whose fields all have serde defaults gets them.
    ///
    /// # Errors
    ///
    /// [`TransportError::Config`] naming the type when the section does not
    /// deserialise into `T`.
    pub fn section<T: DeserializeOwned>(&self) -> TransportResult<T> {
        let value = self
            .section
            .clone()
            .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
        serde_json::from_value(value).map_err(|e| {
            TransportError::Config(format!(
                "the `{}` section does not parse: {e}",
                self.type_name
            ))
        })
    }
}

/// The constructor type the sender registry holds.
pub type SenderConstructor = dyn Fn(CustomSenderConfig) -> BoxSendFuture<'static, TransportResult<Arc<dyn DynTransportSender>>>
    + Send
    + Sync;

/// Every constructor registered with [`register_sender`].
static SENDERS: Registry<SenderConstructor> = Registry::new("transport", TransportType::BUILT_IN);

/// Register `constructor` as the sender for config `type: <type_name>`.
///
/// Register at startup, before building any sender from config. The name is
/// lowercase letters, digits and `_`, starting with a letter, so it also works
/// as an env var segment, and must not be a built-in type.
///
/// # Errors
///
/// [`TransportError::Config`] when the name is malformed, built in, or taken.
pub fn register_sender<F, Fut, S>(type_name: &str, constructor: F) -> TransportResult<()>
where
    F: Fn(CustomSenderConfig) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = TransportResult<S>> + Send + 'static,
    S: TransportSender + 'static,
{
    let erased: Arc<SenderConstructor> = Arc::new(move |config| {
        let built = constructor(config);
        Box::pin(async move {
            let sender: Arc<dyn DynTransportSender> = Arc::new(built.await?);
            Ok(sender)
        })
    });
    SENDERS
        .register(type_name, erased)
        .map_err(|e| TransportError::Config(e.to_string()))
}

/// The type names registered with [`register_sender`], sorted.
#[must_use]
pub fn registered_senders() -> Vec<String> {
    SENDERS.names()
}

/// Build the sender registered under `type_name` from its section.
pub(super) async fn build_sender(
    type_name: &str,
    section: Option<serde_json::Value>,
) -> TransportResult<Arc<dyn DynTransportSender>> {
    let Some(constructor) = SENDERS.get(type_name) else {
        let registered = SENDERS.names();
        return Err(TransportError::Config(format!(
            "no sender is registered for transport type '{type_name}': built-in types are {}, registered types are [{}]",
            TransportType::BUILT_IN.join(", "),
            registered.join(", ")
        )));
    };
    constructor(CustomSenderConfig::new(type_name, section)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{
        AnyReceiver, AnySender, DeliveryRequirement, PayloadFormat, RecordMeta, TransportBase,
        TransportConfig,
    };
    use std::sync::Mutex;

    /// A sender that keeps every payload, with the guarantee it was built with.
    struct Kept {
        guarantee: DeliveryGuarantee,
        payloads: Arc<Mutex<Vec<bytes::Bytes>>>,
    }

    impl Kept {
        fn new(guarantee: DeliveryGuarantee) -> Self {
            Self {
                guarantee,
                payloads: Arc::default(),
            }
        }
    }

    impl TransportBase for Kept {
        async fn close(&self) -> TransportResult<()> {
            Ok(())
        }
        fn is_healthy(&self) -> bool {
            true
        }
        fn name(&self) -> &'static str {
            "kept"
        }
        fn delivery_guarantee(&self) -> DeliveryGuarantee {
            self.guarantee
        }
    }

    impl TransportSender for Kept {
        async fn send(&self, _destination: &str, payload: bytes::Bytes) -> SendResult {
            self.payloads
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(payload);
            SendResult::Ok
        }
    }

    /// The settings `kept_settings` reads from its section.
    #[derive(serde::Deserialize)]
    struct KeptSettings {
        #[allow(dead_code, reason = "required, so its absence is a parse error")]
        label: String,
        guarantee: DeliveryGuarantee,
    }

    /// Register the `kept_*` types once per process: one that reads its
    /// section, and one per guarantee.
    fn registered() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            register_sender("kept_settings", |config| async move {
                let settings: KeptSettings = config.section()?;
                Ok(Kept::new(settings.guarantee))
            })
            .unwrap();
            for (name, guarantee) in [
                ("kept_acked", DeliveryGuarantee::Acknowledged),
                ("kept_written", DeliveryGuarantee::Written),
                ("kept_unconfirmed", DeliveryGuarantee::FireAndForget),
            ] {
                register_sender(name, move |_config| async move { Ok(Kept::new(guarantee)) })
                    .unwrap();
            }
        });
    }

    fn custom(name: &str, delivery: DeliveryRequirement) -> TransportConfig {
        TransportConfig {
            transport_type: TransportType::Custom(name.into()),
            delivery,
            ..TransportConfig::default()
        }
    }

    fn record(payload: &'static [u8]) -> Record {
        Record {
            payload: bytes::Bytes::from_static(payload),
            key: Some(Arc::from("events")),
            headers: Vec::new(),
            metadata: RecordMeta {
                timestamp_ms: None,
                format: PayloadFormat::Json,
            },
        }
    }

    #[tokio::test]
    async fn the_factory_builds_a_registered_type_only_when_its_guarantee_meets_delivery() {
        use DeliveryRequirement::{Ack, Flush, None};
        registered();
        // (type, delivery, built, text a refusal carries)
        let cases = [
            ("kept_acked", Ack, true, ""),
            (
                "kept_written",
                Ack,
                false,
                "kept transport writes and flushes but has no acknowledgement: set delivery: flush to accept that",
            ),
            ("kept_written", Flush, true, ""),
            (
                "kept_unconfirmed",
                Flush,
                false,
                "set delivery: none to accept that",
            ),
            ("kept_unconfirmed", None, true, ""),
            (
                "kept_missing",
                Ack,
                false,
                "no sender is registered for transport type 'kept_missing'",
            ),
        ];
        for (name, delivery, built, text) in cases {
            match AnySender::from_transport_config(&custom(name, delivery)).await {
                Ok(sender) => {
                    assert!(built, "{name} under {delivery} must be refused");
                    assert!(
                        sender
                            .send("x", bytes::Bytes::from_static(b"{}"))
                            .await
                            .is_ok()
                    );
                }
                Err(e) => {
                    assert!(!built, "{name} under {delivery}: {e}");
                    assert!(e.to_string().contains(text), "{name}: {e}");
                }
            }
        }
    }

    #[tokio::test]
    async fn a_registered_type_reads_its_own_section() {
        registered();
        let mut config = custom("kept_settings", DeliveryRequirement::Flush);
        config.custom.insert(
            "kept_settings".into(),
            serde_json::json!({"label": "audit", "guarantee": "written"}),
        );
        let sender = AnySender::from_transport_config(&config).await.unwrap();
        assert_eq!(
            sender.delivery_guarantee(),
            DeliveryGuarantee::Written,
            "built from its section"
        );

        config.custom.insert(
            "kept_settings".into(),
            serde_json::json!({"label": 5, "guarantee": "written"}),
        );
        let bad = AnySender::from_transport_config(&config).await;
        assert!(
            matches!(&bad, Err(TransportError::Config(m)) if m.contains("`kept_settings` section")),
            "a section that does not parse names the type"
        );

        config.custom.clear();
        let missing = AnySender::from_transport_config(&config).await;
        assert!(
            matches!(&missing, Err(TransportError::Config(m)) if m.contains("label")),
            "a required setting of an absent section is named"
        );
    }

    #[tokio::test]
    async fn registration_refuses_a_built_in_or_taken_name_and_a_receiver_refuses_custom() {
        registered();
        for name in ["kafka", "kept_acked", "Kept", "kept-acked"] {
            let refused = register_sender(name, |_config| async {
                Ok(Kept::new(DeliveryGuarantee::Acknowledged))
            });
            assert!(refused.is_err(), "{name}");
        }
        assert!(registered_senders().contains(&"kept_acked".to_string()));
        let receiver =
            AnyReceiver::from_transport_config(&custom("kept_acked", DeliveryRequirement::Ack))
                .await;
        assert!(
            matches!(&receiver, Err(TransportError::Config(m)) if m.contains("send only")),
            "custom transports send only"
        );
    }

    #[tokio::test]
    async fn a_custom_sender_forwards_every_method_through_any_sender() {
        let kept = Kept::new(DeliveryGuarantee::Written);
        let payloads = Arc::clone(&kept.payloads);
        let sender = AnySender::custom(kept);
        assert_eq!(sender.name(), "kept");
        assert!(sender.is_healthy());
        assert!(sender.healthcheck().await.is_ok());
        assert_eq!(sender.delivery_guarantee(), DeliveryGuarantee::Written);
        assert_eq!(sender.confirms_delivery(), SinkConfirmation::None);
        assert!(sender.dead_letter_reason(&record(b"a")).is_none());
        assert!(
            sender
                .send("x", bytes::Bytes::from_static(b"one"))
                .await
                .is_ok()
        );
        assert!(sender.send_record(&record(b"two")).await.is_ok());
        assert!(sender.send_batch(&[record(b"three")]).await.is_ok());
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        assert!(
            sender
                .send_batch_by(&[record(b"four")], deadline)
                .await
                .is_ok()
        );
        assert!(sender.close().await.is_ok());
        let sent: Vec<bytes::Bytes> = payloads.lock().unwrap().clone();
        assert_eq!(sent, [&b"one"[..], b"two", b"three", b"four"]);
    }

    /// The built-in senders under each `delivery`: file and pipe write and
    /// flush, memory is acknowledged.
    #[cfg(all(
        feature = "transport-file",
        feature = "transport-pipe",
        feature = "transport-memory"
    ))]
    #[tokio::test]
    async fn the_factory_refuses_a_file_or_pipe_sender_until_delivery_is_flush() {
        use DeliveryRequirement::{Ack, Flush};
        let dir = tempfile::tempdir().unwrap();
        let file = || TransportConfig {
            transport_type: TransportType::File,
            file: Some(crate::transport::FileTransportConfig {
                path: dir.path().join("out.ndjson").display().to_string(),
                ..Default::default()
            }),
            ..TransportConfig::default()
        };
        let pipe = || TransportConfig {
            transport_type: TransportType::Pipe,
            ..TransportConfig::default()
        };
        let memory = || TransportConfig {
            transport_type: TransportType::Memory,
            ..TransportConfig::default()
        };
        let with = |mut config: TransportConfig, delivery| {
            config.delivery = delivery;
            config
        };
        // (case, config, built, text a refusal carries)
        let cases = [
            (
                "file under ack",
                file(),
                false,
                "file transport writes and flushes but has no acknowledgement: set delivery: flush to accept that",
            ),
            ("file under flush", with(file(), Flush), true, ""),
            (
                "pipe under ack",
                pipe(),
                false,
                "pipe transport writes and flushes but has no acknowledgement: set delivery: flush to accept that",
            ),
            ("pipe under flush", with(pipe(), Flush), true, ""),
            ("memory under ack", with(memory(), Ack), true, ""),
        ];
        for (case, config, built, text) in cases {
            match AnySender::from_transport_config(&config).await {
                Ok(sender) => {
                    assert!(built, "{case} must be refused");
                    sender.close().await.unwrap();
                }
                Err(e) => {
                    assert!(!built, "{case}: {e}");
                    assert!(e.to_string().contains(text), "{case}: {e}");
                }
            }
        }
    }

    /// `type`, `delivery` and a custom section each set through the cascade's
    /// YAML and env layers, and read back by the factory.
    #[cfg(feature = "config")]
    #[tokio::test]
    async fn the_cascade_sets_the_type_delivery_and_section_of_a_custom_sender() {
        registered();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("settings.yaml"),
            "transport:\n  output:\n    type: kept_settings\n    delivery: flush\n    kept_settings:\n      label: from-yaml\n      guarantee: written\n  refused:\n    type: kept_written\n",
        )
        .unwrap();
        temp_env::with_vars(
            [
                ("CUSTOMCASCADE__TRANSPORT__OUTPUT__DELIVERY", Some("ack")),
                (
                    "CUSTOMCASCADE__TRANSPORT__OUTPUT__KEPT_SETTINGS__GUARANTEE",
                    Some("acknowledged"),
                ),
            ],
            || {
                crate::config::setup(crate::config::ConfigOptions {
                    env_prefix: "CUSTOMCASCADE".into(),
                    config_paths: vec![dir.path().to_path_buf()],
                    load_dotenv: false,
                    ..crate::config::ConfigOptions::default()
                })
                .unwrap();
            },
        );

        let read: TransportConfig = crate::config::try_get()
            .unwrap()
            .unmarshal_key("transport.output")
            .unwrap();
        assert_eq!(
            read.transport_type,
            TransportType::Custom("kept_settings".into())
        );
        assert_eq!(read.delivery, DeliveryRequirement::Ack, "env over YAML");
        assert_eq!(
            read.custom.get("kept_settings"),
            Some(&serde_json::json!({"label": "from-yaml", "guarantee": "acknowledged"})),
            "the section takes YAML and env"
        );

        let sender = AnySender::from_config("transport.output").await.unwrap();
        assert_eq!(sender.delivery_guarantee(), DeliveryGuarantee::Acknowledged);

        let refused = AnySender::from_config("transport.refused").await;
        assert!(
            matches!(&refused, Err(TransportError::Config(m)) if m.contains("set transport.refused.delivery: flush to accept that")),
            "the refusal names the full key: {:?}",
            refused.err()
        );
    }

    #[cfg(feature = "transport-memory")]
    #[tokio::test]
    async fn routing_sends_to_a_custom_route_and_states_the_weakest_guarantee() {
        registered();
        let mut table = std::collections::HashMap::new();
        table.insert(
            "audit".to_string(),
            custom("kept_written", DeliveryRequirement::Flush),
        );
        let fallback = TransportConfig {
            transport_type: TransportType::Memory,
            ..TransportConfig::default()
        };
        let sender = crate::transport::RoutedSender::from_route_configs(table, Some(fallback))
            .await
            .unwrap();
        assert_eq!(sender.delivery_guarantee(), DeliveryGuarantee::Written);
        assert!(
            sender
                .send("audit", bytes::Bytes::from_static(b"{}"))
                .await
                .is_ok()
        );
        assert!(
            sender
                .send("other", bytes::Bytes::from_static(b"{}"))
                .await
                .is_ok()
        );
    }

    #[cfg(feature = "tiered-sink")]
    #[tokio::test]
    async fn a_tiered_sink_delivers_through_a_custom_sender() {
        let dir = tempfile::tempdir().unwrap();
        let kept = Kept::new(DeliveryGuarantee::Acknowledged);
        let payloads = Arc::clone(&kept.payloads);
        let tiered = crate::tiered_sink::TieredSink::new(
            AnySender::custom(kept),
            crate::tiered_sink::TieredSinkConfig::new(dir.path().join("spool")),
        )
        .await
        .unwrap();
        tiered.send(&record(b"{\"a\":1}")).await.unwrap();
        assert_eq!(payloads.lock().unwrap().len(), 1, "sent on the hot path");
        tiered.shutdown().await;
    }
}
