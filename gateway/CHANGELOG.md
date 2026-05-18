# Changelog

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
