# Changelog

Rendered by CI and committed back at the end of a release -- do not edit by
hand. Release notes also appear on the GitHub Releases page, one per tag.

## [2.12.1](https://github.com/hyperi-io/scalo-rs/compare/v2.12.0...v2.12.1) (2026-09-09)

### Bug Fixes

* a routed batch is grouped by destination and sent once per sink ([7997f80](https://github.com/hyperi-io/scalo-rs/commit/7997f80c7c7169521eacb59d72ef929b87aff35f))

## [2.12.0](https://github.com/hyperi-io/scalo-rs/compare/v2.11.1...v2.12.0) (2026-09-09)

### Features

* named destinations with fan-out, and an idle gate every app can adopt ([66f1478](https://github.com/hyperi-io/scalo-rs/commit/66f1478fa28ef547515275f348f330f6eb88c13b))

## [2.11.1](https://github.com/hyperi-io/scalo-rs/compare/v2.11.0...v2.11.1) (2026-09-01)

### Bug Fixes

* **geoip-download:** derive PartialEq, and stop the distro env tests racing ([2c66ad5](https://github.com/hyperi-io/scalo-rs/commit/2c66ad5014ce3452f7199cfc973a8ed122ac44bc))
* **metrics:** honour a readiness callback set after the server started ([010f9d2](https://github.com/hyperi-io/scalo-rs/commit/010f9d24a16893f4f5278f56e7441f8beac6c8ad))

## [2.11.0](https://github.com/hyperi-io/scalo-rs/compare/v2.10.15...v2.11.0) (2026-08-31)

### Features

* **geoip-download:** MMDB provisioning shared instead of buried in dfe-loader ([080ae5f](https://github.com/hyperi-io/scalo-rs/commit/080ae5f5d5e159814a22f7e5a42ebbb6231fdece))

### Bug Fixes

* name the send() parameter what it is -- a destination, not a key ([07444ad](https://github.com/hyperi-io/scalo-rs/commit/07444ad3d2ad03f96af7fc26c1cc3026249a117a))

## [2.10.15](https://github.com/hyperi-io/scalo-rs/compare/v2.10.14...v2.10.15) (2026-08-27)

### Bug Fixes

* version check on by default with app-supplied endpoint defaults ([92fdd61](https://github.com/hyperi-io/scalo-rs/commit/92fdd610f399e4cad23d59d6b1ac8dd69e1a7544))

## [2.10.14](https://github.com/hyperi-io/scalo-rs/compare/v2.10.13...v2.10.14) (2026-08-27)

### Bug Fixes

* platform-derived version-check instance id, cascade-gated, shared contract with scalo-py ([6288191](https://github.com/hyperi-io/scalo-rs/commit/6288191292067787401ae6519b5b1017d44c313d))

## [2.10.13](https://github.com/hyperi-io/scalo-rs/compare/v2.10.12...v2.10.13) (2026-08-23)

### Bug Fixes

* **cli:** metrics-addr falls through to config, log format derives from otel ([#38](https://github.com/hyperi-io/scalo-rs/issues/38)) ([b47b191](https://github.com/hyperi-io/scalo-rs/commit/b47b191d935029a06461b791be416fcce03316af)), closes [#36](https://github.com/hyperi-io/scalo-rs/issues/36) [#37](https://github.com/hyperi-io/scalo-rs/issues/37)
* **logger:** quiet rdkafka INFO spam by default, RUST_LOG still overrides ([#35](https://github.com/hyperi-io/scalo-rs/issues/35)) ([f6dcb4d](https://github.com/hyperi-io/scalo-rs/commit/f6dcb4d90002592f4c2f885a207c4c64d862856d))

## [2.10.12](https://github.com/hyperi-io/scalo-rs/compare/v2.10.11...v2.10.12) (2026-08-18)

## [2.10.11](https://github.com/hyperi-io/scalo-rs/compare/v2.10.10...v2.10.11) (2026-08-18)

## [2.10.10](https://github.com/hyperi-io/scalo-rs/compare/v2.10.9...v2.10.10) (2026-08-18)

## [2.10.9](https://github.com/hyperi-io/scalo-rs/compare/v2.10.8...v2.10.9) (2026-08-17)

## [2.10.8](https://github.com/hyperi-io/scalo-rs/compare/v2.10.7...v2.10.8) (2026-08-03)

# Changelog

All notable changes to scalo are recorded here, following
[Semantic Versioning](https://semver.org/). History prior to the
open-source release is intentionally not included.

## 2.9.2 (unreleased)

- Kafka images source librdkafka1 from the Confluent clients repo on ALL
  bases (debian trixie via the bookworm suite, since Confluent has no trixie
  suite), so the image ships the LATEST librdkafka rather than the distro's
  older package. The binary still dynamic-links, so a manual host run uses
  whatever librdkafka is on that host.

## 2.9.1 (2026-06-26)

- Credential specs (`vault:path:key`, `env:VAR`, literal) now resolve
  through `scalo::secrets::{resolve, resolve_optional}`, folded in from
  the former `credential` module so every service shares one syntax.
- Generated Dockerfile licence/copyright labels come from the deployment
  contract's OCI labels, not a hardcoded value.
- Bumped git2 to 0.21 (clears RUSTSEC-2026-0183 / -0184) and dropped the
  libssh2-sys / openssl-sys chain it pulled in - fewer native deps.

## 2.9.0 (2026-06-24)

Initial open-source release. scalo is the Apache-2.0 continuation of a
Rust runtime for self-regulating data-plane services, originally developed
by HyperI. The codebase has been de-branded, relicensed under Apache-2.0,
and reseeded with clean history; the version line continues from the
predecessor's 2.8.x series.
