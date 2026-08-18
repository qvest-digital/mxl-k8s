# Changelog

## [1.1.0-rc.5](https://github.com/qvest-digital/mxl-k8s/compare/v1.1.0-rc.4...v1.1.0-rc.5) (2026-08-18)


### Bug Fixes

* **gateway:** check writer liveness on every failing source state ([#291](https://github.com/qvest-digital/mxl-k8s/issues/291)) ([b23ccb2](https://github.com/qvest-digital/mxl-k8s/commit/b23ccb2a601729c9076942b30af8479fbf038e50))
* **gateway:** record the probed head on the sample path ([#291](https://github.com/qvest-digital/mxl-k8s/issues/291)) ([b23ccb2](https://github.com/qvest-digital/mxl-k8s/commit/b23ccb2a601729c9076942b30af8479fbf038e50))


### Continuous Integration

* bump the go-mxl builder and keep both its images tracked ([#292](https://github.com/qvest-digital/mxl-k8s/issues/292)) ([b5d3f63](https://github.com/qvest-digital/mxl-k8s/commit/b5d3f63ced72e1a37cd7a29d8a9e12b936146853))

## [1.1.0-rc.4](https://github.com/qvest-digital/mxl-k8s/compare/v1.1.0-rc.3...v1.1.0-rc.4) (2026-08-17)


### Features

* **api:** record when a flow's origin moves and what confirmed it ([#285](https://github.com/qvest-digital/mxl-k8s/issues/285)) ([0de36fc](https://github.com/qvest-digital/mxl-k8s/commit/0de36fc670ef4600d469dbb1ad98670c8cd382d3))
* emit events across the whole mirror flow lifecycle ([#285](https://github.com/qvest-digital/mxl-k8s/issues/285)) ([0de36fc](https://github.com/qvest-digital/mxl-k8s/commit/0de36fc670ef4600d469dbb1ad98670c8cd382d3))


### Bug Fixes

* **chart:** put flow_id where the metadata table is read from ([#284](https://github.com/qvest-digital/mxl-k8s/issues/284)) ([eaab973](https://github.com/qvest-digital/mxl-k8s/commit/eaab973bf3fc32dd9082196ef9a6dfacdecadb4c))
* **chart:** separate flow ended from writer stopped ([#284](https://github.com/qvest-digital/mxl-k8s/issues/284)) ([eaab973](https://github.com/qvest-digital/mxl-k8s/commit/eaab973bf3fc32dd9082196ef9a6dfacdecadb4c))
* **exporter:** bound a departed flow to one domain GC sweep ([#284](https://github.com/qvest-digital/mxl-k8s/issues/284)) ([eaab973](https://github.com/qvest-digital/mxl-k8s/commit/eaab973bf3fc32dd9082196ef9a6dfacdecadb4c))
* **exporter:** log through the same logger as the other services ([#286](https://github.com/qvest-digital/mxl-k8s/issues/286)) ([fa4e07f](https://github.com/qvest-digital/mxl-k8s/commit/fa4e07fdc255435109d3d4831540874823b2c18e))
* **gateway:** leave grain pacing off unless configured ([#289](https://github.com/qvest-digital/mxl-k8s/issues/289)) ([a7bebee](https://github.com/qvest-digital/mxl-k8s/commit/a7bebee14f83ab0c0c4cbdf33211ffed12127cc6))
* **gateway:** release a source reader whose flow has no writer ([#285](https://github.com/qvest-digital/mxl-k8s/issues/285)) ([0de36fc](https://github.com/qvest-digital/mxl-k8s/commit/0de36fc670ef4600d469dbb1ad98670c8cd382d3))
* **gateway:** retry a full send queue instead of falling behind it ([#278](https://github.com/qvest-digital/mxl-k8s/issues/278)) ([524e6bd](https://github.com/qvest-digital/mxl-k8s/commit/524e6bd9bc8abf479679d5b3c62c911057f30405))
* report a flow that left the domain as gone, not stalled ([#284](https://github.com/qvest-digital/mxl-k8s/issues/284)) ([eaab973](https://github.com/qvest-digital/mxl-k8s/commit/eaab973bf3fc32dd9082196ef9a6dfacdecadb4c))


### Dependencies

* **gomod:** update module github.com/go-logr/logr to v1.4.4 ([#287](https://github.com/qvest-digital/mxl-k8s/issues/287)) ([b90ea76](https://github.com/qvest-digital/mxl-k8s/commit/b90ea7654eb21b39614b1b0a7de23da6731e8d97))
* **gomod:** update module github.com/stretchr/testify to v1.12.0 ([#282](https://github.com/qvest-digital/mxl-k8s/issues/282)) ([251c3ff](https://github.com/qvest-digital/mxl-k8s/commit/251c3ff8703a75546273142b7a2e3eb071cf003d))


### Continuous Integration

* **images:** gate the kind suite on what a diff can change ([#288](https://github.com/qvest-digital/mxl-k8s/issues/288)) ([b317d05](https://github.com/qvest-digital/mxl-k8s/commit/b317d05f158de9a1fd54eb2f40613a5403c3e82c))

## [1.1.0-rc.3](https://github.com/qvest-digital/mxl-k8s/compare/v1.1.0-rc.2...v1.1.0-rc.3) (2026-08-15)


### Features

* **gateway:** pace grain transmission and stop polling the fabric ([#275](https://github.com/qvest-digital/mxl-k8s/issues/275)) ([86655c3](https://github.com/qvest-digital/mxl-k8s/commit/86655c315f524285d7a152ae226a191ff6c07ddd))


### Dependencies

* **tools:** update module github.com/vektra/mockery/v3 to v3.7.3 ([#274](https://github.com/qvest-digital/mxl-k8s/issues/274)) ([9b8166f](https://github.com/qvest-digital/mxl-k8s/commit/9b8166ff6d07283559571368586deda67318d9ce))

## [1.1.0-rc.2](https://github.com/qvest-digital/mxl-k8s/compare/v1.1.0-rc.1...v1.1.0-rc.2) (2026-08-14)


### Features

* **agent:** deliver the LD_PRELOAD shim from the node ([#271](https://github.com/qvest-digital/mxl-k8s/issues/271)) ([838d937](https://github.com/qvest-digital/mxl-k8s/commit/838d9370c47bee81ffaf501bb98d7e72ce3e52ec))


### Dependencies

* **gomod:** update module github.com/prometheus/client_golang to v1.24.1 ([#270](https://github.com/qvest-digital/mxl-k8s/issues/270)) ([a08614b](https://github.com/qvest-digital/mxl-k8s/commit/a08614b45dce679505241c44dbfcb134d159ee76))

## [1.1.0-rc.1](https://github.com/qvest-digital/mxl-k8s/compare/v1.0.0-rc.24...v1.1.0-rc.1) (2026-08-13)


### Features

* build against libmxl 1.1.0-rc1 via go-mxl 1.1.0-rc.1 ([#269](https://github.com/qvest-digital/mxl-k8s/issues/269)) ([f1579fa](https://github.com/qvest-digital/mxl-k8s/commit/f1579fa2de8dbcad7ad3cfdcbc298ca81ae33e93))


### Bug Fixes

* **gateway:** classify target progress errors instead of assuming fatal ([#267](https://github.com/qvest-digital/mxl-k8s/issues/267)) ([7143cda](https://github.com/qvest-digital/mxl-k8s/commit/7143cdaaa5f7dade289f34161da13c69b1519999))

## [1.0.0-rc.24](https://github.com/qvest-digital/mxl-k8s/compare/v1.0.0-rc.23...v1.0.0-rc.24) (2026-08-11)


### Bug Fixes

* **gateway:** keep the mirror throughput label set unique ([#263](https://github.com/qvest-digital/mxl-k8s/issues/263)) ([a9678eb](https://github.com/qvest-digital/mxl-k8s/commit/a9678eb71765b6be90f7aafcc99f8081f8d84c19))

## [1.0.0-rc.23](https://github.com/qvest-digital/mxl-k8s/compare/v1.0.0-rc.22...v1.0.0-rc.23) (2026-08-11)


### Features

* **gateway:** measure mirror throughput and show it per node ([#260](https://github.com/qvest-digital/mxl-k8s/issues/260)) ([a9087cc](https://github.com/qvest-digital/mxl-k8s/commit/a9087ccf986161bc0f76ec4e7c2d48131e8993eb))


### Bug Fixes

* judge flow activity and mirror wedges on evidence that moves ([#258](https://github.com/qvest-digital/mxl-k8s/issues/258)) ([27996eb](https://github.com/qvest-digital/mxl-k8s/commit/27996eb6b84d82ee170b8337d64d1df2ad4cf200))

## [1.0.0-rc.22](https://github.com/qvest-digital/mxl-k8s/compare/v1.0.0-rc.21...v1.0.0-rc.22) (2026-08-11)


### Features

* **chart:** expose the gateway tuning flags as values ([#257](https://github.com/qvest-digital/mxl-k8s/issues/257)) ([314dec5](https://github.com/qvest-digital/mxl-k8s/commit/314dec5ea5b3a9e7e625030c13edef96bdbb6ff2))
* **exporter:** build against go-mxl v1.0.0-rc.15 ([b96318d](https://github.com/qvest-digital/mxl-k8s/commit/b96318d5d51e48496b7824d782b3b98ea646de98))


### Bug Fixes

* **exporter:** make flow activity a property of the flow ([#252](https://github.com/qvest-digital/mxl-k8s/issues/252)) ([5f10b1b](https://github.com/qvest-digital/mxl-k8s/commit/5f10b1bbe28c0a56065cc3e8c13dc9341d6d321d))
* **gateway:** make the target open backoff authoritative ([#255](https://github.com/qvest-digital/mxl-k8s/issues/255)) ([aa22733](https://github.com/qvest-digital/mxl-k8s/commit/aa22733ffc9bd0ce0c96eee4be5a7001707324fb))


### Dependencies

* **gomod:** update go modules ([#254](https://github.com/qvest-digital/mxl-k8s/issues/254)) ([40bcd47](https://github.com/qvest-digital/mxl-k8s/commit/40bcd47a69640c4b317e301e5532785f1e388b3b))
* **gomod:** update module github.com/go-logr/logr to v1.4.4 ([#256](https://github.com/qvest-digital/mxl-k8s/issues/256)) ([83427ad](https://github.com/qvest-digital/mxl-k8s/commit/83427ad1bc17cc5873dde072d1952fe3b0c1df7a))


### Performance

* **gateway:** stop rewriting the target descriptor on every tick ([#250](https://github.com/qvest-digital/mxl-k8s/issues/250)) ([b96318d](https://github.com/qvest-digital/mxl-k8s/commit/b96318d5d51e48496b7824d782b3b98ea646de98))


### Continuous Integration

* **renovate:** drop the "*" that invalidates the gomod group rule ([#251](https://github.com/qvest-digital/mxl-k8s/issues/251)) ([ac82911](https://github.com/qvest-digital/mxl-k8s/commit/ac8291151e8dceeb9b493e20da81d10dba0deaa8)), closes [#215](https://github.com/qvest-digital/mxl-k8s/issues/215)

## [1.0.0-rc.21](https://github.com/qvest-digital/mxl-k8s/compare/v1.0.0-rc.20...v1.0.0-rc.21) (2026-08-10)


### Features

* **chart:** ship a flow metrics dashboard behind a toggle ([63e3676](https://github.com/qvest-digital/mxl-k8s/commit/63e367668af0e1808f4e8232757979c3758bd3e2))
* **exporter:** export MXL flow metrics once per node ([#245](https://github.com/qvest-digital/mxl-k8s/issues/245)) ([63e3676](https://github.com/qvest-digital/mxl-k8s/commit/63e367668af0e1808f4e8232757979c3758bd3e2))
* **kind:** bring the demo up with a monitoring stack ([63e3676](https://github.com/qvest-digital/mxl-k8s/commit/63e367668af0e1808f4e8232757979c3758bd3e2))


### Bug Fixes

* **operator:** keep the orphaned-mirror delete from being abandoned ([#243](https://github.com/qvest-digital/mxl-k8s/issues/243)) ([ad0a960](https://github.com/qvest-digital/mxl-k8s/commit/ad0a960201676d13e8d83d267be4ae2612d01b3a))


### Build System

* **shim:** fail the build above the supported glibc floor ([#241](https://github.com/qvest-digital/mxl-k8s/issues/241)) ([9a85e6f](https://github.com/qvest-digital/mxl-k8s/commit/9a85e6f390fc30640b57864ea971460fdecbe684))

## [1.0.0-rc.20](https://github.com/qvest-digital/mxl-k8s/compare/v1.0.0-rc.19...v1.0.0-rc.20) (2026-08-09)


### Features

* **api:** add targetAttemptCount to MxlFlowMirror status ([61ca815](https://github.com/qvest-digital/mxl-k8s/commit/61ca81526f1f79802753a14ab4a3d7477f00c530))


### Bug Fixes

* **gateway:** escalate and recover a target that cannot open ([#238](https://github.com/qvest-digital/mxl-k8s/issues/238)) ([61ca815](https://github.com/qvest-digital/mxl-k8s/commit/61ca81526f1f79802753a14ab4a3d7477f00c530)), closes [#236](https://github.com/qvest-digital/mxl-k8s/issues/236) [#237](https://github.com/qvest-digital/mxl-k8s/issues/237)
* **shim:** hook __xstat so pre-2.33 glibc consumers raise intent ([#239](https://github.com/qvest-digital/mxl-k8s/issues/239)) ([8c3661a](https://github.com/qvest-digital/mxl-k8s/commit/8c3661a79c4dfc6b460c909c2c2968abe1af2661))

## [1.0.0-rc.19](https://github.com/qvest-digital/mxl-k8s/compare/v1.0.0-rc.18...v1.0.0-rc.19) (2026-08-09)


### Features

* **operator:** delete MxlNodeCapabilities of departed nodes ([#235](https://github.com/qvest-digital/mxl-k8s/issues/235)) ([b0090e1](https://github.com/qvest-digital/mxl-k8s/commit/b0090e1779d9b724c45e242d0c1179b334879661))


### Bug Fixes

* **gateway:** own MxlNodeCapabilities by node, rate-limit the probe ([#233](https://github.com/qvest-digital/mxl-k8s/issues/233)) ([7d6d098](https://github.com/qvest-digital/mxl-k8s/commit/7d6d09839a3e50e869cdf57752ca3f37b2024f97))


### Miscellaneous

* stop committing the graphify knowledge graph ([#230](https://github.com/qvest-digital/mxl-k8s/issues/230)) ([ec5f564](https://github.com/qvest-digital/mxl-k8s/commit/ec5f5648510b414e4d36282c9b908ea1be90ec10))

## [1.0.0-rc.18](https://github.com/qvest-digital/mxl-k8s/compare/v1.0.0-rc.17...v1.0.0-rc.18) (2026-08-06)


### Features

* **api:** gate provider selection on probed device counts ([8cc2965](https://github.com/qvest-digital/mxl-k8s/commit/8cc29658f1c9fe5f76c16582a8b0ae2754a1f214))
* **chart:** render one gateway DaemonSet per node class ([#228](https://github.com/qvest-digital/mxl-k8s/issues/228)) ([8cf5424](https://github.com/qvest-digital/mxl-k8s/commit/8cf54245db9e012b67b5bc0dc04169f813ea9df0))
* **gateway:** publish probed providers and scope the fabric ([#227](https://github.com/qvest-digital/mxl-k8s/issues/227)) ([8cc2965](https://github.com/qvest-digital/mxl-k8s/commit/8cc29658f1c9fe5f76c16582a8b0ae2754a1f214))


### Dependencies

* **gateway:** move onto go-mxl 1.0.0-rc.14 ([8cc2965](https://github.com/qvest-digital/mxl-k8s/commit/8cc29658f1c9fe5f76c16582a8b0ae2754a1f214))

## [1.0.0-rc.17](https://github.com/qvest-digital/mxl-k8s/compare/v1.0.0-rc.16...v1.0.0-rc.17) (2026-08-04)


### Bug Fixes

* **gateway:** select the fabric interface before setup ([#224](https://github.com/qvest-digital/mxl-k8s/issues/224)) ([f7d76e7](https://github.com/qvest-digital/mxl-k8s/commit/f7d76e74cbe7afa3ffc1df855962704766e42e21))

## [1.0.0-rc.16](https://github.com/qvest-digital/mxl-k8s/compare/v1.0.0-rc.15...v1.0.0-rc.16) (2026-08-04)


### Features

* **operator:** collect flows no node holds and no mirror needs ([6edad0b](https://github.com/qvest-digital/mxl-k8s/commit/6edad0be3a8a54d4793ce00fd425314e3be2fd34))


### Bug Fixes

* **gateway:** keep a flow a local producer has taken over ([#222](https://github.com/qvest-digital/mxl-k8s/issues/222)) ([1a6472f](https://github.com/qvest-digital/mxl-k8s/commit/1a6472ff12742c8049a04741503b13cc795dbe61)), closes [#219](https://github.com/qvest-digital/mxl-k8s/issues/219)
* **gateway:** reopen a source reader whose head stops advancing ([6edad0b](https://github.com/qvest-digital/mxl-k8s/commit/6edad0be3a8a54d4793ce00fd425314e3be2fd34))
* **shim:** report a producer attach only on the flow data file ([#220](https://github.com/qvest-digital/mxl-k8s/issues/220)) ([6edad0b](https://github.com/qvest-digital/mxl-k8s/commit/6edad0be3a8a54d4793ce00fd425314e3be2fd34))


### Dependencies

* **gateway:** move onto go-mxl 1.0.0-rc.12 ([#223](https://github.com/qvest-digital/mxl-k8s/issues/223)) ([a38481c](https://github.com/qvest-digital/mxl-k8s/commit/a38481cdae96c27ceb88ff547431d81aab9aae6f))

## [1.0.0-rc.15](https://github.com/qvest-digital/mxl-k8s/compare/v1.0.0-rc.14...v1.0.0-rc.15) (2026-08-01)


### Features

* **api:** add ReaderNotAdvancing condition reason ([37e5499](https://github.com/qvest-digital/mxl-k8s/commit/37e5499a130c77e52cee9f8b2958a0b422128d5f))


### Bug Fixes

* **gateway:** surface a source reader whose head stops advancing ([#217](https://github.com/qvest-digital/mxl-k8s/issues/217)) ([37e5499](https://github.com/qvest-digital/mxl-k8s/commit/37e5499a130c77e52cee9f8b2958a0b422128d5f))

## [1.0.0-rc.14](https://github.com/qvest-digital/mxl-k8s/compare/v1.0.0-rc.13...v1.0.0-rc.14) (2026-07-31)


### Continuous Integration

* collapse the release train onto two release-please packages ([#213](https://github.com/qvest-digital/mxl-k8s/issues/213)) ([521e8c8](https://github.com/qvest-digital/mxl-k8s/commit/521e8c8064a354492ceadb5bed70d65d6e48fe90))
