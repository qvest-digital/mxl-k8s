# Changelog

## [1.0.0-rc.4](https://github.com/qvest-digital/mxl-k8s/compare/api/v1.0.0-rc.3...api/v1.0.0-rc.4) (2026-07-21)


### Features

* resolve mirror provider from node capabilities instead of stamping auto ([#154](https://github.com/qvest-digital/mxl-k8s/issues/154)) ([731b245](https://github.com/qvest-digital/mxl-k8s/commit/731b245d152960b1da8aba5c9ef89bbb1a3fd4a7))


### Build System

* **deps:** bump golang.org/x/net from 0.49.0 to 0.55.0 in /api and operator ([#148](https://github.com/qvest-digital/mxl-k8s/issues/148)) ([8dbbbbe](https://github.com/qvest-digital/mxl-k8s/commit/8dbbbbe1dbb4959b73555279b73093d1698ca077))

## [1.0.0-rc.3](https://github.com/qvest-digital/mxl-k8s/compare/api/v1.0.0-rc.2...api/v1.0.0-rc.3) (2026-07-01)


### Bug Fixes

* **gateway:** surface target-open failures in MxlFlowMirror status ([#127](https://github.com/qvest-digital/mxl-k8s/issues/127)) ([b20a1ca](https://github.com/qvest-digital/mxl-k8s/commit/b20a1ca5f52c89d8b43e694d9aca9cb82635fff6))


### Dependencies

* **gomod:** update go modules ([#123](https://github.com/qvest-digital/mxl-k8s/issues/123)) ([811033d](https://github.com/qvest-digital/mxl-k8s/commit/811033d8144c8c9bc5414322256338dac436dbce))

## [1.0.0-rc.2](https://github.com/qvest-digital/mxl-k8s/compare/api/v1.0.0-rc.1...api/v1.0.0-rc.2) (2026-05-27)


### Features

* **operator,gateway,agent:** harden MxlFlowMirror lifecycle ([#79](https://github.com/qvest-digital/mxl-k8s/issues/79)) ([a8aa3e3](https://github.com/qvest-digital/mxl-k8s/commit/a8aa3e306ea77e8856008d0dad57a0052331db3b))


### Bug Fixes

* **gateway:** recover targets wedged after first grain ([#87](https://github.com/qvest-digital/mxl-k8s/issues/87)) ([fe49ca3](https://github.com/qvest-digital/mxl-k8s/commit/fe49ca302296afd517118b08f3309d33b2b7a526))
* **operator:** refcount shared mirrors via OwnerReferences ([#86](https://github.com/qvest-digital/mxl-k8s/issues/86)) ([48f27c2](https://github.com/qvest-digital/mxl-k8s/commit/48f27c29af6162fe071891305f45e79abd6e0513))

## [1.0.0-rc.1](https://github.com/qvest-digital/mxl-k8s/compare/api/v1.0.0-rc.0...api/v1.0.0-rc.1) (2026-05-18)


### Features

* **api:** add v1alpha1 CRD types ([9729a59](https://github.com/qvest-digital/mxl-k8s/commit/9729a59427d470f7100b9eb4f724b5a4c2646590))


### Miscellaneous

* scaffold multi-module go workspace and CI ([11f4159](https://github.com/qvest-digital/mxl-k8s/commit/11f41597db99c5de1b47dfa7a5060ecc3090cebf))
