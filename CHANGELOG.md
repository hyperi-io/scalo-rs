# Changelog

Rendered by CI and committed back at the end of a release -- do not edit by
hand. Release notes also appear on the GitHub Releases page, one per tag.

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
