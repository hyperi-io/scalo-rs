# Changelog

Rendered by CI and committed back at the end of a release -- do not edit by
hand. Release notes also appear on the GitHub Releases page, one per tag.

## [2.13.3](https://github.com/hyperi-io/scalo-rs/compare/v2.13.2...v2.13.3) (2026-09-29)

### Bug Fixes

* bound the fetch and admit on a hold release ([#251](https://github.com/hyperi-io/scalo-rs/issues/251)) ([35595f8](https://github.com/hyperi-io/scalo-rs/commit/35595f8e6d3b0284b1dc8b7805c08d27cb298b2e))
* bound the memory hold and stop an idle leak ([#247](https://github.com/hyperi-io/scalo-rs/issues/247)) ([551e8c3](https://github.com/hyperi-io/scalo-rs/commit/551e8c3c82b3d2301be2004c1372258c2240c8e5))
* neutral names in kafka/vector docs and tests ([#252](https://github.com/hyperi-io/scalo-rs/issues/252)) ([f47d353](https://github.com/hyperi-io/scalo-rs/commit/f47d353b3a38b7f8c802f73cdefc49ecaed88a58))
* neutral names in scalo's docs and tests ([#249](https://github.com/hyperi-io/scalo-rs/issues/249)) ([7728660](https://github.com/hyperi-io/scalo-rs/commit/772866060f893c25ae55051a497500ca9c02705d))
* shrink the byte budget only under memory ([#250](https://github.com/hyperi-io/scalo-rs/issues/250)) ([2222054](https://github.com/hyperi-io/scalo-rs/commit/2222054f562766866c4b2dcf9501880c23d5fc7a))

## [2.13.2](https://github.com/hyperi-io/scalo-rs/compare/v2.13.1...v2.13.2) (2026-09-29)

### Bug Fixes

* **chart:** instance id survives a pod restart ([#236](https://github.com/hyperi-io/scalo-rs/issues/236)) ([7933ecc](https://github.com/hyperi-io/scalo-rs/commit/7933eccb3e1691a05fc1c93ff47896282f355a2a))
* **kafka:** label rdkafka_ by client, lag to LSO ([#237](https://github.com/hyperi-io/scalo-rs/issues/237)) ([3859405](https://github.com/hyperi-io/scalo-rs/commit/3859405358afd1e18d0734652fb42f63bf4e9635))
* **kafka:** publish consumer lag and assignment from the first assignment ([#231](https://github.com/hyperi-io/scalo-rs/issues/231)) ([0ce5780](https://github.com/hyperi-io/scalo-rs/commit/0ce5780d0c267e23f5263995cd4f2bb6db325641))
* **kafka:** tell the caller when a revoke ends its claim on a partition ([#244](https://github.com/hyperi-io/scalo-rs/issues/244)) ([cb1f151](https://github.com/hyperi-io/scalo-rs/commit/cb1f151afbc8b50256a436b4f5e2255c22bf7058))
* **metrics:** fill the consumer series scalo registers, and drop a revoked partition's offsets ([#234](https://github.com/hyperi-io/scalo-rs/issues/234)) ([787c6eb](https://github.com/hyperi-io/scalo-rs/commit/787c6eb61344a265c561a093b487a0fbe2aa3399)), closes [#232](https://github.com/hyperi-io/scalo-rs/issues/232)
* **metrics:** label consumer series by group ([#239](https://github.com/hyperi-io/scalo-rs/issues/239)) ([f302165](https://github.com/hyperi-io/scalo-rs/commit/f302165cdea88222220d0d2f256123b88f0c238d)), closes [#238](https://github.com/hyperi-io/scalo-rs/issues/238)
* reduced-feature lint and doc drift ([#246](https://github.com/hyperi-io/scalo-rs/issues/246)) ([95ce828](https://github.com/hyperi-io/scalo-rs/commit/95ce828ee2315b56ecd6b7e82455bc13db1ce59a)), closes [#144](https://github.com/hyperi-io/scalo-rs/issues/144) [#225](https://github.com/hyperi-io/scalo-rs/issues/225) [hyperi-io/dfe-fetcher#206](https://github.com/hyperi-io/dfe-fetcher/issues/206)
* serve /scaling/pressure on start_server ([#245](https://github.com/hyperi-io/scalo-rs/issues/245)) ([b761c95](https://github.com/hyperi-io/scalo-rs/commit/b761c959c89b58e3b4526ce2396d639c5efc9e73)), closes [#168](https://github.com/hyperi-io/scalo-rs/issues/168)
* **test:** run the Kafka test brokers on the JVM image ([#243](https://github.com/hyperi-io/scalo-rs/issues/243)) ([0024d77](https://github.com/hyperi-io/scalo-rs/commit/0024d778e97b25a5b3a27af38cc7db8f8d58053e))
* **transport:** clippy clean with no backend on ([#241](https://github.com/hyperi-io/scalo-rs/issues/241)) ([1f73b6e](https://github.com/hyperi-io/scalo-rs/commit/1f73b6ef589479b7bbcb73a905495b81feb6a0c7)), closes [#240](https://github.com/hyperi-io/scalo-rs/issues/240)

## [2.13.1](https://github.com/hyperi-io/scalo-rs/compare/v2.13.0...v2.13.1) (2026-09-27)

### Bug Fixes

* **kafka:** rebuild a fatal consumer; classic is the default group protocol ([#228](https://github.com/hyperi-io/scalo-rs/issues/228)) ([cd441e1](https://github.com/hyperi-io/scalo-rs/commit/cd441e1c9aa87799f02443b0e247981456ade4ea))

## [2.13.0](https://github.com/hyperi-io/scalo-rs/compare/v2.12.11...v2.13.0) (2026-09-26)

### Features

* hold source acknowledgements until delivery ([#204](https://github.com/hyperi-io/scalo-rs/issues/204)) ([c4656b0](https://github.com/hyperi-io/scalo-rs/commit/c4656b08f847982c0ce71ac61409d15a15798c7b))

### Bug Fixes

* **build:** apply -D warnings alongside target rustflags ([#219](https://github.com/hyperi-io/scalo-rs/issues/219)) ([92bec98](https://github.com/hyperi-io/scalo-rs/commit/92bec986d0a558bb8f83a697f67268032b51bfed))
* **dlq:** cascade hands a failed Kafka delivery to the next backend ([#214](https://github.com/hyperi-io/scalo-rs/issues/214)) ([a30a3c8](https://github.com/hyperi-io/scalo-rs/commit/a30a3c818802ad3ae12f0876d2489518aca570f1)), closes [#213](https://github.com/hyperi-io/scalo-rs/issues/213)
* **dlq:** label overflow drops, clear CI warnings ([#221](https://github.com/hyperi-io/scalo-rs/issues/221)) ([ba22aea](https://github.com/hyperi-io/scalo-rs/commit/ba22aeaef883c78c299c3cf68e6b887d417980eb))
* **docs:** architecture.md states the feature edges Cargo.toml has ([#216](https://github.com/hyperi-io/scalo-rs/issues/216)) ([6ee6d33](https://github.com/hyperi-io/scalo-rs/commit/6ee6d335cb3cd372356fdffee7dff7efcf2b5615))
* **docs:** resolve feature-gated intra-doc links ([#217](https://github.com/hyperi-io/scalo-rs/issues/217)) ([427a159](https://github.com/hyperi-io/scalo-rs/commit/427a1595517fc9068cc7c9a154951a7faf58dd60))
* note why each flagged scanner line holds no secret ([#220](https://github.com/hyperi-io/scalo-rs/issues/220)) ([46660e4](https://github.com/hyperi-io/scalo-rs/commit/46660e4cffa70de518ed4565cb94004810366114))
* **tiered-sink:** export the corruption policy its config takes ([#218](https://github.com/hyperi-io/scalo-rs/issues/218)) ([67b48ea](https://github.com/hyperi-io/scalo-rs/commit/67b48ea0bace0aa2cb1f4a48d1b513471108f88f))
* zstd producer default, overrides last, one winner per librdkafka name ([#212](https://github.com/hyperi-io/scalo-rs/issues/212)) ([6b6319a](https://github.com/hyperi-io/scalo-rs/commit/6b6319a236ae73fb76a613b81ee32d9f3e07aab6))

### Performance Improvements

* **parse_guard:** block scan, same verdicts ([#215](https://github.com/hyperi-io/scalo-rs/issues/215)) ([6a2eb8a](https://github.com/hyperi-io/scalo-rs/commit/6a2eb8a5c9dcddb4bdb56314075973054d20a141))

## [2.12.11](https://github.com/hyperi-io/scalo-rs/compare/v2.12.10...v2.12.11) (2026-09-25)

### Bug Fixes

* **dlq:** settle Kafka at shutdown, wire send_timeout_ms, follow-ups ([#195](https://github.com/hyperi-io/scalo-rs/issues/195)) ([96aa959](https://github.com/hyperi-io/scalo-rs/commit/96aa9597ae9861e34a1b1e1b24cf3584201bfe4c))
* gRPC resilience and metric double counts ([#199](https://github.com/hyperi-io/scalo-rs/issues/199)) ([b25afc5](https://github.com/hyperi-io/scalo-rs/commit/b25afc539a007eb522d39e1d39969d36b2d2e66e))
* **grpc:** keep acked records at close ([#196](https://github.com/hyperi-io/scalo-rs/issues/196)) ([ddff0c8](https://github.com/hyperi-io/scalo-rs/commit/ddff0c8d05eb3b8e58d22ebb416907b394abb2c8))
* keep acked records at shutdown ([#198](https://github.com/hyperi-io/scalo-rs/issues/198)) ([159e411](https://github.com/hyperi-io/scalo-rs/commit/159e4117369d9fcba5f79808fbe1d140d85b84ae)), closes [#196](https://github.com/hyperi-io/scalo-rs/issues/196) [#196](https://github.com/hyperi-io/scalo-rs/issues/196)

## [2.12.10](https://github.com/hyperi-io/scalo-rs/compare/v2.12.9...v2.12.10) (2026-09-24)

### Bug Fixes

* **dlq:** durable Kafka flush, no rotation panic ([#194](https://github.com/hyperi-io/scalo-rs/issues/194)) ([ee29ebd](https://github.com/hyperi-io/scalo-rs/commit/ee29ebdba14f7c81df6be0e713d4f65883b93777))
* **dlq:** flush() fails when any batch since the last flush was refused ([#193](https://github.com/hyperi-io/scalo-rs/issues/193)) ([64e4616](https://github.com/hyperi-io/scalo-rs/commit/64e461688cbf2158b8326ebd560b68f23cc025f3)), closes [#187](https://github.com/hyperi-io/scalo-rs/issues/187)

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
