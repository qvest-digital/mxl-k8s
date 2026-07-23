# Changelog

## [1.0.0-rc.10](https://github.com/qvest-digital/mxl-k8s/compare/charts/mxl-k8s/v1.0.0-rc.9...charts/mxl-k8s/v1.0.0-rc.10) (2026-07-23)


### Dependencies

* **chart:** update mxl-k8s module images ([#169](https://github.com/qvest-digital/mxl-k8s/issues/169)) ([cbc21c3](https://github.com/qvest-digital/mxl-k8s/commit/cbc21c3eb45dcbccb3dafc8336a5580f4239eb2c))
* **chart:** update mxl-k8s module images ([#171](https://github.com/qvest-digital/mxl-k8s/issues/171)) ([fa77fe5](https://github.com/qvest-digital/mxl-k8s/commit/fa77fe59a0accecf110ef063ee99eda3468f3749))

## [1.0.0-rc.9](https://github.com/qvest-digital/mxl-k8s/compare/charts/mxl-k8s/v1.0.0-rc.8...charts/mxl-k8s/v1.0.0-rc.9) (2026-07-21)


### Features

* resolve mirror provider from node capabilities instead of stamping auto ([#154](https://github.com/qvest-digital/mxl-k8s/issues/154)) ([731b245](https://github.com/qvest-digital/mxl-k8s/commit/731b245d152960b1da8aba5c9ef89bbb1a3fd4a7))


### Dependencies

* **chart:** update mxl-k8s module images ([#160](https://github.com/qvest-digital/mxl-k8s/issues/160)) ([39b364d](https://github.com/qvest-digital/mxl-k8s/commit/39b364d27a397acfdf84731e69325fa8f374c5d4))
* **chart:** update mxl-k8s module images ([#161](https://github.com/qvest-digital/mxl-k8s/issues/161)) ([e57c419](https://github.com/qvest-digital/mxl-k8s/commit/e57c41946ffaa8ed61a77ee1e9ee14309b5e5da2))
* **chart:** update mxl-k8s module images ([#163](https://github.com/qvest-digital/mxl-k8s/issues/163)) ([ee1a4e0](https://github.com/qvest-digital/mxl-k8s/commit/ee1a4e03768bad51b463eef907f7b8af8f8b599f))

## [1.0.0-rc.8](https://github.com/qvest-digital/mxl-k8s/compare/charts/mxl-k8s/v1.0.0-rc.7...charts/mxl-k8s/v1.0.0-rc.8) (2026-07-01)


### Dependencies

* **chart:** update mxl-k8s module images ([#140](https://github.com/qvest-digital/mxl-k8s/issues/140)) ([f64fad4](https://github.com/qvest-digital/mxl-k8s/commit/f64fad4feba9ef91852fa274d8041a4059194d0d))

## [1.0.0-rc.7](https://github.com/qvest-digital/mxl-k8s/compare/charts/mxl-k8s/v1.0.0-rc.6...charts/mxl-k8s/v1.0.0-rc.7) (2026-07-01)


### Features

* **chart:** pin bundled module images, require tag or digest ([#115](https://github.com/qvest-digital/mxl-k8s/issues/115)) ([8181f07](https://github.com/qvest-digital/mxl-k8s/commit/8181f072fc5584acdc090483157cf6cefc0e90f5))


### Dependencies

* **tools:** update ci tool versions ([#120](https://github.com/qvest-digital/mxl-k8s/issues/120)) ([db6473e](https://github.com/qvest-digital/mxl-k8s/commit/db6473eda06b53c49d3429be4288bba593de76dc))


### Code Refactoring

* **chart:** package committed image pins ([#118](https://github.com/qvest-digital/mxl-k8s/issues/118)) ([b840fd0](https://github.com/qvest-digital/mxl-k8s/commit/b840fd06c7c602bc49c358d1f49b78e3449619a5))

## [1.0.0-rc.6](https://github.com/qvest-digital/mxl-k8s/compare/charts/mxl-k8s/v1.0.0-rc.5...charts/mxl-k8s/v1.0.0-rc.6) (2026-06-02)


### Miscellaneous

* **deps:** update busybox docker tag to v1.38 ([#100](https://github.com/qvest-digital/mxl-k8s/issues/100)) ([9790e64](https://github.com/qvest-digital/mxl-k8s/commit/9790e642c6932d9988a947e8e6e5d63996f2f770))

## [1.0.0-rc.5](https://github.com/qvest-digital/mxl-k8s/compare/charts/mxl-k8s/v1.0.0-rc.4...charts/mxl-k8s/v1.0.0-rc.5) (2026-05-27)


### Features

* **chart:** expose gateway pprof bind address ([f32db01](https://github.com/qvest-digital/mxl-k8s/commit/f32db0158adf99863d25c76ac9a78ed9f8709e31))
* **chart:** predelete hook to wipe domain dirs ([#99](https://github.com/qvest-digital/mxl-k8s/issues/99)) ([9b83bbc](https://github.com/qvest-digital/mxl-k8s/commit/9b83bbc991090ed3e1778cbd45ec62af7ec39e98))
* **chart:** support NAD-attached RDMA and chart-driven kind-up ([#77](https://github.com/qvest-digital/mxl-k8s/issues/77)) ([3cfb182](https://github.com/qvest-digital/mxl-k8s/commit/3cfb1828d33f50e621de8ef6dea2ed5bd5719286))
* **gateway:** pprof bind address flag ([#101](https://github.com/qvest-digital/mxl-k8s/issues/101)) ([f32db01](https://github.com/qvest-digital/mxl-k8s/commit/f32db0158adf99863d25c76ac9a78ed9f8709e31))
* **operator,gateway,agent:** harden MxlFlowMirror lifecycle ([#79](https://github.com/qvest-digital/mxl-k8s/issues/79)) ([a8aa3e3](https://github.com/qvest-digital/mxl-k8s/commit/a8aa3e306ea77e8856008d0dad57a0052331db3b))


### Bug Fixes

* **chart:** sanitize "+" in app.kubernetes.io/version label ([#96](https://github.com/qvest-digital/mxl-k8s/issues/96)) ([26388a8](https://github.com/qvest-digital/mxl-k8s/commit/26388a85178ffa971c9012ce902fd609f937a829))
* **gateway:** recover targets wedged after first grain ([#87](https://github.com/qvest-digital/mxl-k8s/issues/87)) ([fe49ca3](https://github.com/qvest-digital/mxl-k8s/commit/fe49ca302296afd517118b08f3309d33b2b7a526))
* **operator:** refcount shared mirrors via OwnerReferences ([#86](https://github.com/qvest-digital/mxl-k8s/issues/86)) ([48f27c2](https://github.com/qvest-digital/mxl-k8s/commit/48f27c29af6162fe071891305f45e79abd6e0513))

## [1.0.0-rc.4](https://github.com/qvest-digital/mxl-k8s/compare/charts/mxl-k8s/v1.0.0-rc.3...charts/mxl-k8s/v1.0.0-rc.4) (2026-05-19)


### Continuous Integration

* collapse dual release-please configs into one with per-package prerelease toggle ([#51](https://github.com/qvest-digital/mxl-k8s/issues/51)) ([e537056](https://github.com/qvest-digital/mxl-k8s/commit/e537056ec89e16659b6f0100fc5346ee36c58041))

## [1.0.0-rc.3](https://github.com/qvest-digital/mxl-k8s/compare/charts/mxl-k8s/v1.0.0-rc.2...charts/mxl-k8s/v1.0.0-rc.3) (2026-05-19)


### Bug Fixes

* **chart:** align pod template name label with selector ([#50](https://github.com/qvest-digital/mxl-k8s/issues/50)) ([5b65bd4](https://github.com/qvest-digital/mxl-k8s/commit/5b65bd47ece985ac123b7849daee9f2f692ccde9))

## [1.0.0-rc.2](https://github.com/qvest-digital/mxl-k8s/compare/charts/mxl-k8s/v1.0.0-rc.1...charts/mxl-k8s/v1.0.0-rc.2) (2026-05-19)


### Bug Fixes

* **agent:** reader pod can't open mirrored flow on KIND demo ([#40](https://github.com/qvest-digital/mxl-k8s/issues/40)) ([27ee1f9](https://github.com/qvest-digital/mxl-k8s/commit/27ee1f9d300f6dd223f03f2f0bf2eb3953e4829f))

## [1.0.0-rc.1](https://github.com/qvest-digital/mxl-k8s/compare/charts/mxl-k8s/v1.0.0-rc.0...charts/mxl-k8s/v1.0.0-rc.1) (2026-05-18)


### Features

* **chart:** add Helm chart for the mxl-k8s control plane ([b4f2b56](https://github.com/qvest-digital/mxl-k8s/commit/b4f2b5643e819541a07eb4fa716a043b7722e6cb))


### Miscellaneous

* **chart:** wire appVersion sentinel and pin doc snippets to 1.0.0-rc.1 ([#36](https://github.com/qvest-digital/mxl-k8s/issues/36)) ([36fc730](https://github.com/qvest-digital/mxl-k8s/commit/36fc73092be7fd7ebda7c368f38861d705b2061a))
