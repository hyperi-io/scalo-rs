# Testing

The general rules are hyperi-ai's testing standard (`standards/universal/testing.md` in hyperi-ai). This page covers what scalo-rs does on top of them.

## Levels

| Level | Runs | Command |
|---|---|---|
| core | locally, every push and PR | `cargo nextest run --all-features` |
| full | nightly (`schedule` in `.github/workflows/ci.yml`) and before every release (`test.full.required_for_release: true`) | `cargo nextest run --all-features --run-ignored all --ignore-default-filter` |

Core is nextest's default selection. The `default-filter` in `.config/nextest.toml` holds every test that needs a real broker, OpenBao or a docker build out of it, bar one main-cases test per service. Full adds those and the `#[ignore]` tests. hyperi-ci runs core on push and PR, and full on the schedule, on a release, or on a dispatch with `test-tier: full`.

Core holds:

- the unit tests in `src/`, the smoke test and the integration tests that need no server
- `e2e::kafka::test_kafka_send_receive_batch`: records sent through `KafkaTransport`, received back and committed, on the shared broker
- `secrets_vault_resolve`: a `vault:` spec resolving a real secret, a missing path and a refused token, on the shared OpenBao

Those four are core's only container-backed tests, on two containers per run.

Full adds the rest of the real-broker tests (`kafka_send_batch`, `kafka_recv_runtime`, `kafka_consumer_outage`, `dlq_kafka_durable`, `kafka_tls`, the `e2e::kafka` modules), the OpenBao rotation tests in `secrets_refresh`, the `e2e::contract_artefacts` docker builds, and the ignored OTLP collector, Prometheus and operational tests.

## Services, one instance each per run

| Service | Image | Container port |
|---|---|---|
| Kafka | `apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` | 9092 PLAINTEXT |
| OpenBao | `openbao/openbao:2.6.1@sha256:5b2486ab0fb90bbc788cc345b0a08616dfb375873ee8be5df3a2fd4d378a67e0` | 8200, dev mode, root token `root` |

The Kafka pin is `transport::kafka::test_support::KAFKA_IMAGE`, the line the DFE stack deploys. The OpenBao pin lives in `tests/common/openbao.rs`.

`deployment::test_support::shared_service` gives every test process in a run the same container per service, the way it does for every consumer of scalo's test support:

- The first process to ask creates it, named `<prefix>-integration-<service>-r<run>`, where `<run>` is the start of `NEXTEST_RUN_ID` and `<prefix>` is `HYPERI_TEST_CONTAINER_PREFIX`, else `scalo-rs-test`. Every other process joins it by that name.
- The creator starts a watcher in its own process group that removes the container once the nextest process exits, whether the run passed, failed or was interrupted.
- Containers carry `io.hyperi.test.suite=scalo-rs-integration`, `io.hyperi.test.run`, and the owner: `owner-host`, `owner-pidns`, `owner-pid`, `owner-start` and `lease-until`. Each create sweeps the suite's containers whose owner has gone on this host, and another host's only once their 4-hour lease has lapsed.
- Ports bind 127.0.0.1 on the daemon host. An `ssh://` daemon gets an `ssh -L` tunnel per port, and a remote `tcp://` daemon is refused unless `HYPERI_TEST_DOCKER_BIND` names a private address on it.

Tests on the shared instances keep to their own topics, consumer groups and secret paths, unique to the test as well as the process because `cargo test` runs a whole file in one process (`common::unique`, `common::openbao::kv_path(scalo::test_name!())`). A test that stops or pauses its broker holds one alone (`common::kafka::broker_alone`, `shared_service::start_alone`), named `<prefix>-integration-<test>-kafka-p<pid>` on a host port kept across the restart: the outage tests, the DLQ tests that take the broker away, and the broker-down-at-start test. nextest runs at most three of those at once (`own-broker`). `TlsBroker` is one of its own too, with the certificates from a named openssl container.

The OTLP collector and Prometheus tests start their containers through testcontainers, and the operational tests through `docker run` in their scripts. Each pauses, configures or builds a container for one test, so none can share, and none is yet named for the suite.
