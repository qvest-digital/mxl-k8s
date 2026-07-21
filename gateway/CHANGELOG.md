# Changelog

## [1.0.0-rc.7](https://github.com/qvest-digital/mxl-k8s/compare/gateway/v1.0.0-rc.6...gateway/v1.0.0-rc.7) (2026-07-21)


### Features

* resolve mirror provider from node capabilities instead of stamping auto ([#154](https://github.com/qvest-digital/mxl-k8s/issues/154)) ([731b245](https://github.com/qvest-digital/mxl-k8s/commit/731b245d152960b1da8aba5c9ef89bbb1a3fd4a7))


### Dependencies

* **api:** bump api module to v1.0.0-rc.4 ([#159](https://github.com/qvest-digital/mxl-k8s/issues/159)) ([2afcadb](https://github.com/qvest-digital/mxl-k8s/commit/2afcadb10ef8c11741ac43d3cf9ee297cd1ae71e))
* **api:** update module github.com/qvest-digital/mxl-k8s/api to v1.0.0-rc.3 ([#139](https://github.com/qvest-digital/mxl-k8s/issues/139)) ([5444825](https://github.com/qvest-digital/mxl-k8s/commit/54448254f61ada511bd3c75dd1019ce610cfb477))
* **gateway:** follow libmxl-fabrics v1.1.0-beta-1 interface API ([#146](https://github.com/qvest-digital/mxl-k8s/issues/146)) ([3e6b3ce](https://github.com/qvest-digital/mxl-k8s/commit/3e6b3ce1d6dce2a5ef7316720a5737cb20f2ff08))


### Build System

* **deps:** bump golang.org/x/net from 0.49.0 to 0.55.0 in /gateway ([#147](https://github.com/qvest-digital/mxl-k8s/issues/147)) ([53fb581](https://github.com/qvest-digital/mxl-k8s/commit/53fb581e60cdef7c3a64cc2fdcf2fb29ac180414))

## [1.0.0-rc.6](https://github.com/qvest-digital/mxl-k8s/compare/gateway/v1.0.0-rc.5...gateway/v1.0.0-rc.6) (2026-07-01)


### Bug Fixes

* **gateway:** resync mirror to live head when it falls out of the producer ring ([#132](https://github.com/qvest-digital/mxl-k8s/issues/132)) ([a331444](https://github.com/qvest-digital/mxl-k8s/commit/a33144452827b69763852206aacf401385f9dc84))
* **gateway:** surface target-open failures in MxlFlowMirror status ([#127](https://github.com/qvest-digital/mxl-k8s/issues/127)) ([b20a1ca](https://github.com/qvest-digital/mxl-k8s/commit/b20a1ca5f52c89d8b43e694d9aca9cb82635fff6))


### Dependencies

* **gomod:** update go modules ([#123](https://github.com/qvest-digital/mxl-k8s/issues/123)) ([811033d](https://github.com/qvest-digital/mxl-k8s/commit/811033d8144c8c9bc5414322256338dac436dbce))

## [1.0.0-rc.5](https://github.com/qvest-digital/mxl-k8s/compare/gateway/v1.0.0-rc.4...gateway/v1.0.0-rc.5) (2026-06-02)


### Features

* **examples:** add audio sample transfer to the TCP demo ([#112](https://github.com/qvest-digital/mxl-k8s/issues/112)) ([f9245a8](https://github.com/qvest-digital/mxl-k8s/commit/f9245a8d2f6fcca3ea34ad6c2b12179f93cbbd95))
* **gateway:** mirror continuous audio flows via sample transfer ([#111](https://github.com/qvest-digital/mxl-k8s/issues/111)) ([0807b88](https://github.com/qvest-digital/mxl-k8s/commit/0807b88d3e18e2efb660c7a2330b99ca7f71c529))


### Build System

* **gateway:** migrate to go-mxl v1.0.0-rc.9 ([#106](https://github.com/qvest-digital/mxl-k8s/issues/106)) ([d3a52b0](https://github.com/qvest-digital/mxl-k8s/commit/d3a52b01fdf035d8e408d6ed575345b60300161d))


### Miscellaneous

* **deps:** update module github.com/qvest-digital/mxl-k8s/api to v1.0.0-rc.2 ([#105](https://github.com/qvest-digital/mxl-k8s/issues/105)) ([6652a9f](https://github.com/qvest-digital/mxl-k8s/commit/6652a9f297fb8eb84d0a2f149f0788b8362a79ef))

## [1.0.0-rc.4](https://github.com/qvest-digital/mxl-k8s/compare/gateway/v1.0.0-rc.3...gateway/v1.0.0-rc.4) (2026-05-27)


### Features

* **chart:** expose gateway pprof bind address ([f32db01](https://github.com/qvest-digital/mxl-k8s/commit/f32db0158adf99863d25c76ac9a78ed9f8709e31))
* **gateway:** pprof bind address flag ([#101](https://github.com/qvest-digital/mxl-k8s/issues/101)) ([f32db01](https://github.com/qvest-digital/mxl-k8s/commit/f32db0158adf99863d25c76ac9a78ed9f8709e31))
* **operator,gateway,agent:** harden MxlFlowMirror lifecycle ([#79](https://github.com/qvest-digital/mxl-k8s/issues/79)) ([a8aa3e3](https://github.com/qvest-digital/mxl-k8s/commit/a8aa3e306ea77e8856008d0dad57a0052331db3b))


### Bug Fixes

* **gateway:** close entry on sourceNode mismatch ([#97](https://github.com/qvest-digital/mxl-k8s/issues/97)) ([3434e83](https://github.com/qvest-digital/mxl-k8s/commit/3434e83adae7eeaf464dff1330a84ed33977522e))
* **gateway:** recover targets wedged after first grain ([#87](https://github.com/qvest-digital/mxl-k8s/issues/87)) ([fe49ca3](https://github.com/qvest-digital/mxl-k8s/commit/fe49ca302296afd517118b08f3309d33b2b7a526))
* **gateway:** recover targets wedged in silent ErrNotReady ([#85](https://github.com/qvest-digital/mxl-k8s/issues/85)) ([943c266](https://github.com/qvest-digital/mxl-k8s/commit/943c266d997646febb74f10183389826f88c71e6))


### Build System

* **gateway:** bump go-mxl to 1.0.0-rc.6 ([#98](https://github.com/qvest-digital/mxl-k8s/issues/98)) ([4a3a3eb](https://github.com/qvest-digital/mxl-k8s/commit/4a3a3eb010f22aa5286ae401f4dcded6abe09496))
* **gateway:** bump go-mxl to 1.0.0-rc.7 ([#102](https://github.com/qvest-digital/mxl-k8s/issues/102)) ([aadb15d](https://github.com/qvest-digital/mxl-k8s/commit/aadb15dd02878058d102cc232ea84d6a1573f21a))
* **gateway:** bump go-mxl to 1.0.0-rc.8 ([#103](https://github.com/qvest-digital/mxl-k8s/issues/103)) ([97023a6](https://github.com/qvest-digital/mxl-k8s/commit/97023a69a284fff4a2f9d8364c642206487663db))

## [1.0.0-rc.3](https://github.com/qvest-digital/mxl-k8s/compare/gateway/v1.0.0-rc.2...gateway/v1.0.0-rc.3) (2026-05-19)


### Miscellaneous

* contributor-review pass on docs, comments, and typography ([#46](https://github.com/qvest-digital/mxl-k8s/issues/46)) ([cddc9ba](https://github.com/qvest-digital/mxl-k8s/commit/cddc9bad1535087a19d04570b77438e6df27a1eb))

## [1.0.0-rc.2](https://github.com/qvest-digital/mxl-k8s/compare/gateway/v1.0.0-rc.1...gateway/v1.0.0-rc.2) (2026-05-19)


### Bug Fixes

* **shim,agent,gateway:** close intent path and quiet reconciler noise ([#41](https://github.com/qvest-digital/mxl-k8s/issues/41)) ([36d6d88](https://github.com/qvest-digital/mxl-k8s/commit/36d6d883aab66565d90b7832c04c0cfe3cf0d116))

## [1.0.0-rc.1](https://github.com/qvest-digital/mxl-k8s/compare/gateway/v1.0.0-rc.0...gateway/v1.0.0-rc.1) (2026-05-18)


### Features

* **gateway:** MxlFlowMirror source-side reconciler + transfer loop ([e01437f](https://github.com/qvest-digital/mxl-k8s/commit/e01437f05914565233d95370fa4382d690eebf0f))
* **gateway:** MxlFlowMirror target-side reconciler via go-mxl/fabrics ([488f660](https://github.com/qvest-digital/mxl-k8s/commit/488f660193282b797710e7ff7641189e1470cc57))
* **gateway:** v0 capability publisher via go-mxl/fabrics provider enum ([91fe452](https://github.com/qvest-digital/mxl-k8s/commit/91fe45205aa459c4adb05c0c3ae02037920ff1a8))


### Bug Fixes

* **gateway:** drive target progress loop + commit received grains ([337e489](https://github.com/qvest-digital/mxl-k8s/commit/337e4893d768c92e1c99737c9b904401c6f7c592))
* **gateway:** drop blocking ReadGrain to avoid SIGURG tear-downs ([ab6bdd0](https://github.com/qvest-digital/mxl-k8s/commit/ab6bdd0883ef8b80cca5f9c6277dd508c6816028))
* **gateway:** keep writer alive when rebuilding fabric side ([f9e98a2](https://github.com/qvest-digital/mxl-k8s/commit/f9e98a21f73566bd4783ec675a0472cd0f4481f1))
* **gateway:** recover instead of segfaulting on fatal ReadGrain ([6f566e4](https://github.com/qvest-digital/mxl-k8s/commit/6f566e43762e01b083df9d267b8e4df8d0902d2d))
* **gateway:** reopen source initiator when target info rotates ([9573ac8](https://github.com/qvest-digital/mxl-k8s/commit/9573ac86e6d341f6a04a78078f594005aa26199c))
* **gateway:** require live writer in target-side fast-path ([c48a2e6](https://github.com/qvest-digital/mxl-k8s/commit/c48a2e6ac8c96a8788c27c7769902b99e8838304))
* **gateway:** wake source reconciler on MxlFlow changes ([b1c9b6a](https://github.com/qvest-digital/mxl-k8s/commit/b1c9b6a3694f546871ebcb9292a7166148866d9e))


### Build System

* bump go-mxl to 1.0.0-rc.5 and drop fork-libmxl overlay ([965b276](https://github.com/qvest-digital/mxl-k8s/commit/965b27690af04e1cc3d99ed12cadbd14463f481e))
* **deps:** bump in-repo api dependency to 1.0.0-rc.1 ([#32](https://github.com/qvest-digital/mxl-k8s/issues/32)) ([2c4f802](https://github.com/qvest-digital/mxl-k8s/commit/2c4f80248cb05a0fe36aceb50db545096363b4b8))


### Miscellaneous

* scaffold multi-module go workspace and CI ([11f4159](https://github.com/qvest-digital/mxl-k8s/commit/11f41597db99c5de1b47dfa7a5060ecc3090cebf))
