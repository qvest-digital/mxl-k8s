# Changelog

## [1.0.0-rc.23](https://github.com/qvest-digital/mxl-k8s/compare/v1.0.0-rc.22...v1.0.0-rc.23) (2026-08-11)


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
