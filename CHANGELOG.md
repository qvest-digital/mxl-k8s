# Changelog

## [1.0.0-rc.19](https://github.com/qvest-digital/mxl-k8s/compare/v1.0.0-rc.18...v1.0.0-rc.19) (2026-08-06)


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
