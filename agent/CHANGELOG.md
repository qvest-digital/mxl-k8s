# Changelog

## [1.0.0-rc.4](https://github.com/qvest-digital/mxl-k8s/compare/agent/v1.0.0-rc.3...agent/v1.0.0-rc.4) (2026-06-01)


### Miscellaneous

* **deps:** update module github.com/qvest-digital/mxl-k8s/api to v1.0.0-rc.2 ([#105](https://github.com/qvest-digital/mxl-k8s/issues/105)) ([6652a9f](https://github.com/qvest-digital/mxl-k8s/commit/6652a9f297fb8eb84d0a2f149f0788b8362a79ef))

## [1.0.0-rc.3](https://github.com/qvest-digital/mxl-k8s/compare/agent/v1.0.0-rc.2...agent/v1.0.0-rc.3) (2026-05-27)


### Features

* **operator,gateway,agent:** harden MxlFlowMirror lifecycle ([#79](https://github.com/qvest-digital/mxl-k8s/issues/79)) ([a8aa3e3](https://github.com/qvest-digital/mxl-k8s/commit/a8aa3e306ea77e8856008d0dad57a0052331db3b))


### Miscellaneous

* contributor-review pass on docs, comments, and typography ([#46](https://github.com/qvest-digital/mxl-k8s/issues/46)) ([cddc9ba](https://github.com/qvest-digital/mxl-k8s/commit/cddc9bad1535087a19d04570b77438e6df27a1eb))
* **deps:** update module golang.org/x/sys to v0.45.0 ([#78](https://github.com/qvest-digital/mxl-k8s/issues/78)) ([19f3148](https://github.com/qvest-digital/mxl-k8s/commit/19f3148c8deea374a51b11d0fef14bbf7f590613))

## [1.0.0-rc.2](https://github.com/qvest-digital/mxl-k8s/compare/agent/v1.0.0-rc.1...agent/v1.0.0-rc.2) (2026-05-19)


### Bug Fixes

* **agent:** reader pod can't open mirrored flow on KIND demo ([#40](https://github.com/qvest-digital/mxl-k8s/issues/40)) ([27ee1f9](https://github.com/qvest-digital/mxl-k8s/commit/27ee1f9d300f6dd223f03f2f0bf2eb3953e4829f))
* **shim,agent,gateway:** close intent path and quiet reconciler noise ([#41](https://github.com/qvest-digital/mxl-k8s/issues/41)) ([36d6d88](https://github.com/qvest-digital/mxl-k8s/commit/36d6d883aab66565d90b7832c04c0cfe3cf0d116))

## [1.0.0-rc.1](https://github.com/qvest-digital/mxl-k8s/compare/agent/v1.0.0-rc.0...agent/v1.0.0-rc.1) (2026-05-18)


### Features

* **agent:** on-demand mirror dispatcher via UDS intent endpoint ([3556b17](https://github.com/qvest-digital/mxl-k8s/commit/3556b1752bb22ef8dd2c128940833018d846ecae))
* **agent:** v0 mxl-domain-agent with fanotify-driven flow publisher ([8060285](https://github.com/qvest-digital/mxl-k8s/commit/8060285854c5c18e7378e5d1648bef71333b6e5c))


### Build System

* **deps:** bump in-repo api dependency to 1.0.0-rc.1 ([#32](https://github.com/qvest-digital/mxl-k8s/issues/32)) ([2c4f802](https://github.com/qvest-digital/mxl-k8s/commit/2c4f80248cb05a0fe36aceb50db545096363b4b8))


### Miscellaneous

* **deps:** update go modules ([#23](https://github.com/qvest-digital/mxl-k8s/issues/23)) ([11d827c](https://github.com/qvest-digital/mxl-k8s/commit/11d827c3e219894079702d75f3b408c5709fd130))
* scaffold multi-module go workspace and CI ([11f4159](https://github.com/qvest-digital/mxl-k8s/commit/11f41597db99c5de1b47dfa7a5060ecc3090cebf))
