# Changelog

Rendered by CI and committed back at the end of a release -- do not edit by
hand. Release notes also appear on the GitHub Releases page, one per tag.

## [2.12.9](https://github.com/hyperi-io/scalo-rs/compare/v2.12.8...v2.12.9) (2026-09-24)

### Bug Fixes

* **deployment:** pin base digest, hadolint-clean ([#186](https://github.com/hyperi-io/scalo-rs/issues/186)) ([7fb8e39](https://github.com/hyperi-io/scalo-rs/commit/7fb8e3973af5b9f18bb9ea99ea78dbc78638824e))
* **kafka:** clear queued poll errors in one receive ([#192](https://github.com/hyperi-io/scalo-rs/issues/192)) ([13c5d96](https://github.com/hyperi-io/scalo-rs/commit/13c5d96a529e4273db39ecc99e71edeef3cb24cd)), closes [#184](https://github.com/hyperi-io/scalo-rs/issues/184)
* pipe filter tiers and false doc strings ([#182](https://github.com/hyperi-io/scalo-rs/issues/182)) ([f96b72b](https://github.com/hyperi-io/scalo-rs/commit/f96b72b3ef4f2ab69bf3a4557ab7a447dd969a0e))
* poll Kafka off the runtime so recv yields ([#183](https://github.com/hyperi-io/scalo-rs/issues/183)) ([4ee6573](https://github.com/hyperi-io/scalo-rs/commit/4ee6573b689340527c6ee7d27745398f9b29e6ef))
* **scripts:** pin loadgen images, run as uid 1000 ([#190](https://github.com/hyperi-io/scalo-rs/issues/190)) ([73e9610](https://github.com/hyperi-io/scalo-rs/commit/73e9610fd4a4d0051c90a24e39c66da22e5455ac))
* **transport:** cancel-safe pipe and file recv ([#189](https://github.com/hyperi-io/scalo-rs/issues/189)) ([88909b5](https://github.com/hyperi-io/scalo-rs/commit/88909b5fdb6af1ae761daa5ebee55b2984475511))
* **transport:** dead-letter an over-limit gRPC send instead of retrying it ([#188](https://github.com/hyperi-io/scalo-rs/issues/188)) ([72660b3](https://github.com/hyperi-io/scalo-rs/commit/72660b3863422c71e1610495715237bf307aa3ba))

## [2.12.8](https://github.com/hyperi-io/scalo-rs/compare/v2.12.7...v2.12.8) (2026-09-24)

### Bug Fixes

* remove the Redis transport and DLQ backend ([#179](https://github.com/hyperi-io/scalo-rs/issues/179)) ([846fa93](https://github.com/hyperi-io/scalo-rs/commit/846fa93e05cb9caa45768882a94d0458682c401b))

## [2.12.7](https://github.com/hyperi-io/scalo-rs/compare/v2.12.6...v2.12.7) (2026-09-24)

### Bug Fixes

* **transport:** a transient broker outage no longer ends the consumer ([#177](https://github.com/hyperi-io/scalo-rs/issues/177)) ([2aabeb3](https://github.com/hyperi-io/scalo-rs/commit/2aabeb3e415cfb7fc7084c08d01cbb598b5a8416)), closes [#176](https://github.com/hyperi-io/scalo-rs/issues/176) [#176](https://github.com/hyperi-io/scalo-rs/issues/176)
* **transport:** bind http test receivers on port 0 with no rebind race ([#175](https://github.com/hyperi-io/scalo-rs/issues/175)) ([07c693f](https://github.com/hyperi-io/scalo-rs/commit/07c693f8be99cf0eef114097f608ede04fba5e2d))

## [2.12.6](https://github.com/hyperi-io/scalo-rs/compare/v2.12.5...v2.12.6) (2026-09-24)

### Bug Fixes

* **kafka:** pipeline send_batch instead of awaiting each record ([#174](https://github.com/hyperi-io/scalo-rs/issues/174)) ([ad26203](https://github.com/hyperi-io/scalo-rs/commit/ad262037d10ec737f4d4778a11893514c83c2ece))

## [2.12.5](https://github.com/hyperi-io/scalo-rs/compare/v2.12.4...v2.12.5) (2026-09-24)

### Bug Fixes

* **ci:** move .hyperi-ci.yaml to the release vocabulary ([#173](https://github.com/hyperi-io/scalo-rs/issues/173)) ([abb5e97](https://github.com/hyperi-io/scalo-rs/commit/abb5e97578947b3d43b90e83fd96e3b5044c5457))
* **docs:** name scalo-py, not pylib or golib ([#169](https://github.com/hyperi-io/scalo-rs/issues/169)) ([25c6582](https://github.com/hyperi-io/scalo-rs/commit/25c658213a43d06e4d6b07465cfd3167e4420dde))
* **kafka:** derive internal group ids from config ([#171](https://github.com/hyperi-io/scalo-rs/issues/171)) ([387c6b1](https://github.com/hyperi-io/scalo-rs/commit/387c6b1faad66a191d0ef2dc013e7782c7c7e41f)), closes [#106](https://github.com/hyperi-io/scalo-rs/issues/106)

## [2.12.4](https://github.com/hyperi-io/scalo-rs/compare/v2.12.3...v2.12.4) (2026-09-23)

### Bug Fixes

* **auth:** assert the credential cache hit allocates nothing ([#128](https://github.com/hyperi-io/scalo-rs/issues/128)) ([eed1354](https://github.com/hyperi-io/scalo-rs/commit/eed13541cc7fe7caeb94212fd66fb8f2d98db4f6))
* **build:** let the .cargo negations apply ([#153](https://github.com/hyperi-io/scalo-rs/issues/153)) ([6e1f3d6](https://github.com/hyperi-io/scalo-rs/commit/6e1f3d65c880cff435f8bf3f695ac094c77ba01f))
* **build:** track the .cargo config files ([15feee4](https://github.com/hyperi-io/scalo-rs/commit/15feee48687fcda2104ba4d465163e8dd46bfe67)), closes [#178](https://github.com/hyperi-io/scalo-rs/issues/178)
* **deps:** empty the dead cargo-audit ignore list ([5fa546c](https://github.com/hyperi-io/scalo-rs/commit/5fa546c7c5e32fe824d197d9bd00d6acd1756289))
* **deps:** raise floors off three live advisories ([#163](https://github.com/hyperi-io/scalo-rs/issues/163)) ([d19f3af](https://github.com/hyperi-io/scalo-rs/commit/d19f3af35b005fd80e99d1ccdbd153b1864fb6d1))
* **deps:** raise the floors that cannot resolve ([#166](https://github.com/hyperi-io/scalo-rs/issues/166)) ([cb2526d](https://github.com/hyperi-io/scalo-rs/commit/cb2526d62b3e7b3780a7e363898f7481717dd723)), closes [#164](https://github.com/hyperi-io/scalo-rs/issues/164)
* **docs:** add the README `## Context` section, and stop duplicating native-deps ([#151](https://github.com/hyperi-io/scalo-rs/issues/151)) ([fa40885](https://github.com/hyperi-io/scalo-rs/commit/fa408858e9454e7ef641f31634b13551d193aef9))
* **kafka:** classify produce failures by error code, and record deliveries ([2f47b44](https://github.com/hyperi-io/scalo-rs/commit/2f47b449c29b02af6cd007d6a0065ad16421b7ce))
* **kafka:** KIP-848 by default, the 16 MiB record chain, and the oversize-record loss path ([#74](https://github.com/hyperi-io/scalo-rs/issues/74)) ([6d53e2f](https://github.com/hyperi-io/scalo-rs/commit/6d53e2f48353b38ac650a0001a76425cda285ae4))
* lead with the hero line, and cut the README to it ([#152](https://github.com/hyperi-io/scalo-rs/issues/152)) ([71c05e6](https://github.com/hyperi-io/scalo-rs/commit/71c05e6722890e331ad8c0334fc4fd0a80c52f1a))
* **tests:** make the smoke test boot something ([#167](https://github.com/hyperi-io/scalo-rs/issues/167)) ([77153ef](https://github.com/hyperi-io/scalo-rs/commit/77153ef00ea5c7082d7c0d70ab902d16d9ab90cd)), closes [#161](https://github.com/hyperi-io/scalo-rs/issues/161)
* thirteen consumer fixes, one release ([930c881](https://github.com/hyperi-io/scalo-rs/commit/930c88139c5f9fc596d30d06ebdd55dca88bfb4e)), closes [#124](https://github.com/hyperi-io/scalo-rs/issues/124) [#129](https://github.com/hyperi-io/scalo-rs/issues/129) [#135](https://github.com/hyperi-io/scalo-rs/issues/135) [#133](https://github.com/hyperi-io/scalo-rs/issues/133) [#102](https://github.com/hyperi-io/scalo-rs/issues/102) [#125](https://github.com/hyperi-io/scalo-rs/issues/125) [#139](https://github.com/hyperi-io/scalo-rs/issues/139) [#62](https://github.com/hyperi-io/scalo-rs/issues/62) [#134](https://github.com/hyperi-io/scalo-rs/issues/134) [#140](https://github.com/hyperi-io/scalo-rs/issues/140) [#130](https://github.com/hyperi-io/scalo-rs/issues/130) [#132](https://github.com/hyperi-io/scalo-rs/issues/132) [#127](https://github.com/hyperi-io/scalo-rs/issues/127)
* two silent failures -- a dropped config write and an inert setter ([#165](https://github.com/hyperi-io/scalo-rs/issues/165)) ([85d1a37](https://github.com/hyperi-io/scalo-rs/commit/85d1a3718ff74da7e16f0a06c0957ac9e4106a37)), closes [#158](https://github.com/hyperi-io/scalo-rs/issues/158) [#157](https://github.com/hyperi-io/scalo-rs/issues/157)

## [2.12.3](https://github.com/hyperi-io/scalo-rs/compare/v2.12.2...v2.12.3) (2026-09-16)

### Bug Fixes

* **auth:** shared credential sources and request signers under one retry loop ([183700f](https://github.com/hyperi-io/scalo-rs/commit/183700f59d5ef7a1823b96099d5f503dd894daab)), closes [#88](https://github.com/hyperi-io/scalo-rs/issues/88)
* **concurrency:** count periodic ticks on a paused clock, not real time ([3e79150](https://github.com/hyperi-io/scalo-rs/commit/3e7915023308332f628c4d96e47a687296dfa83b)), closes [#113](https://github.com/hyperi-io/scalo-rs/issues/113)
* **config:** accept the nested env spelling charts actually emit ([cb7d888](https://github.com/hyperi-io/scalo-rs/commit/cb7d8889aacfba25b9f48585e1169aae4b9bbfe8))
* **config:** let a file passed to --config reach the cascade ([6ce361a](https://github.com/hyperi-io/scalo-rs/commit/6ce361a6c2212b23ee85ffd38f5c316b15f760a6))
* **config:** say so when the cascade was never initialised ([a04441e](https://github.com/hyperi-io/scalo-rs/commit/a04441e6cc9296151b4a6979f4c8a4abcd480aef)), closes [#50](https://github.com/hyperi-io/scalo-rs/issues/50) [#49](https://github.com/hyperi-io/scalo-rs/issues/49)
* **deployment:** give KEDA the Kafka auth parameters it recognises ([4b893b9](https://github.com/hyperi-io/scalo-rs/commit/4b893b991c375d49b95a305185723256b2ca477f))
* **deps:** hold aws-smithy-types below 1.7 so a fresh resolve builds ([8949aee](https://github.com/hyperi-io/scalo-rs/commit/8949aee3b81a3bc1b2b5fdc3419045312ab35570))
* **secrets:** resolve file:, bao: and aws: specs and read the first vault segment as the mount ([a1fd909](https://github.com/hyperi-io/scalo-rs/commit/a1fd9097e76590810d55670683ab54698734bf59)), closes [#82](https://github.com/hyperi-io/scalo-rs/issues/82)

## [2.12.2](https://github.com/hyperi-io/scalo-rs/compare/v2.12.1...v2.12.2) (2026-09-12)

### Bug Fixes

* **memory:** read what the kernel charges, not the reservation counter ([f5164a5](https://github.com/hyperi-io/scalo-rs/commit/f5164a506514c6be7423cbabe00325d69c8eba12))

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
