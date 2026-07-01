# Changelog

## [1.0.0-rc.4](https://github.com/qvest-digital/mxl-k8s/compare/operator/v1.0.0-rc.3...operator/v1.0.0-rc.4) (2026-07-01)


### Dependencies

* **gomod:** update go modules ([#123](https://github.com/qvest-digital/mxl-k8s/issues/123)) ([811033d](https://github.com/qvest-digital/mxl-k8s/commit/811033d8144c8c9bc5414322256338dac436dbce))

## [1.0.0-rc.3](https://github.com/qvest-digital/mxl-k8s/compare/operator/v1.0.0-rc.2...operator/v1.0.0-rc.3) (2026-06-02)


### Miscellaneous

* **deps:** update k8s.io/utils digest to ff6756f ([#110](https://github.com/qvest-digital/mxl-k8s/issues/110)) ([c60f82d](https://github.com/qvest-digital/mxl-k8s/commit/c60f82d51f842646a2bc9121d9d04cd663edad87))
* **deps:** update module github.com/qvest-digital/mxl-k8s/api to v1.0.0-rc.2 ([#105](https://github.com/qvest-digital/mxl-k8s/issues/105)) ([6652a9f](https://github.com/qvest-digital/mxl-k8s/commit/6652a9f297fb8eb84d0a2f149f0788b8362a79ef))

## [1.0.0-rc.2](https://github.com/qvest-digital/mxl-k8s/compare/operator/v1.0.0-rc.1...operator/v1.0.0-rc.2) (2026-05-27)


### Features

* **operator,gateway,agent:** harden MxlFlowMirror lifecycle ([#79](https://github.com/qvest-digital/mxl-k8s/issues/79)) ([a8aa3e3](https://github.com/qvest-digital/mxl-k8s/commit/a8aa3e306ea77e8856008d0dad57a0052331db3b))


### Bug Fixes

* **operator:** refcount shared mirrors via OwnerReferences ([#86](https://github.com/qvest-digital/mxl-k8s/issues/86)) ([48f27c2](https://github.com/qvest-digital/mxl-k8s/commit/48f27c29af6162fe071891305f45e79abd6e0513))
* **operator:** require LabelCreatedByReceiverNamespace on cross-ns adopt ([8fd6dc1](https://github.com/qvest-digital/mxl-k8s/commit/8fd6dc1634211c7e368ef703cbf9e6b0e03a1431))
* **operator:** scrub mirror owner refs to dead receivers ([8fd6dc1](https://github.com/qvest-digital/mxl-k8s/commit/8fd6dc1634211c7e368ef703cbf9e6b0e03a1431))
* **operator:** scrub mirror refs to dead receivers ([#90](https://github.com/qvest-digital/mxl-k8s/issues/90)) ([8fd6dc1](https://github.com/qvest-digital/mxl-k8s/commit/8fd6dc1634211c7e368ef703cbf9e6b0e03a1431))

## [1.0.0-rc.1](https://github.com/qvest-digital/mxl-k8s/compare/operator/v1.0.0-rc.0...operator/v1.0.0-rc.1) (2026-05-18)


### Features

* **operator:** implement MxlReceiver reconciler ([2f46c6f](https://github.com/qvest-digital/mxl-k8s/commit/2f46c6fc3992b0e01de14bc538d9fac1a7bcdf33))
* **operator:** scaffold controller-manager with observe-only reconcilers ([5cb2306](https://github.com/qvest-digital/mxl-k8s/commit/5cb23065f771bb34cf4e81a0ff83525f9666bba5))


### Bug Fixes

* **operator:** re-reconcile receivers on bound mirror changes ([2ad6ff1](https://github.com/qvest-digital/mxl-k8s/commit/2ad6ff122c461f9d26fa8c0e723e83c44b2ddf01))


### Build System

* **deps:** bump in-repo api dependency to 1.0.0-rc.1 ([#32](https://github.com/qvest-digital/mxl-k8s/issues/32)) ([2c4f802](https://github.com/qvest-digital/mxl-k8s/commit/2c4f80248cb05a0fe36aceb50db545096363b4b8))


### Miscellaneous

* scaffold multi-module go workspace and CI ([11f4159](https://github.com/qvest-digital/mxl-k8s/commit/11f41597db99c5de1b47dfa7a5060ecc3090cebf))
