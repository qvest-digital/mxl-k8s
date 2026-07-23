# Changelog

## [1.0.0-rc.7](https://github.com/qvest-digital/mxl-k8s/compare/agent/v1.0.0-rc.6...agent/v1.0.0-rc.7) (2026-07-23)


### Bug Fixes

* **gateway:** detect origin rotation for a reader opened before Stale clears ([#166](https://github.com/qvest-digital/mxl-k8s/issues/166)) ([18addab](https://github.com/qvest-digital/mxl-k8s/commit/18addab361ae3c1f13b59dd26d3b4e1fa781b8fe))
* **gateway:** gate stuck-handshake recovery on source activity ([#165](https://github.com/qvest-digital/mxl-k8s/issues/165)) ([4c330dd](https://github.com/qvest-digital/mxl-k8s/commit/4c330ddd1a854f63526fe65378e6a413dab6fee9))

## [1.0.0-rc.6](https://github.com/qvest-digital/mxl-k8s/compare/agent/v1.0.0-rc.5...agent/v1.0.0-rc.6) (2026-07-21)


### Features

* resolve mirror provider from node capabilities instead of stamping auto ([#154](https://github.com/qvest-digital/mxl-k8s/issues/154)) ([731b245](https://github.com/qvest-digital/mxl-k8s/commit/731b245d152960b1da8aba5c9ef89bbb1a3fd4a7))


### Dependencies

* **api:** bump api module to v1.0.0-rc.4 ([#159](https://github.com/qvest-digital/mxl-k8s/issues/159)) ([2afcadb](https://github.com/qvest-digital/mxl-k8s/commit/2afcadb10ef8c11741ac43d3cf9ee297cd1ae71e))
* **api:** update module github.com/qvest-digital/mxl-k8s/api to v1.0.0-rc.3 ([#139](https://github.com/qvest-digital/mxl-k8s/issues/139)) ([5444825](https://github.com/qvest-digital/mxl-k8s/commit/54448254f61ada511bd3c75dd1019ce610cfb477))
* **gomod:** update module golang.org/x/sys to v0.47.0 ([#138](https://github.com/qvest-digital/mxl-k8s/issues/138)) ([c3b1ee9](https://github.com/qvest-digital/mxl-k8s/commit/c3b1ee90215abb373060e8493c3a82ab84171697))


### Build System

* **deps:** bump golang.org/x/net from 0.49.0 to 0.55.0 in /agent ([#145](https://github.com/qvest-digital/mxl-k8s/issues/145)) ([5da8e16](https://github.com/qvest-digital/mxl-k8s/commit/5da8e166ee9114686c98099cf908d8f354bb985f))

## [1.0.0-rc.5](https://github.com/qvest-digital/mxl-k8s/compare/agent/v1.0.0-rc.4...agent/v1.0.0-rc.5) (2026-07-01)


### Dependencies

* **gomod:** update go modules ([#123](https://github.com/qvest-digital/mxl-k8s/issues/123)) ([811033d](https://github.com/qvest-digital/mxl-k8s/commit/811033d8144c8c9bc5414322256338dac436dbce))

## [1.0.0-rc.4](https://github.com/qvest-digital/mxl-k8s/compare/agent/v1.0.0-rc.3...agent/v1.0.0-rc.4) (2026-06-02)


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
