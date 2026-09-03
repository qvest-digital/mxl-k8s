# Changelog

## [1.0.0-rc.14](https://github.com/qvest-digital/mxl-k8s/compare/api/v1.0.0-rc.13...api/v1.0.0-rc.14) (2026-09-03)


### Features

* **gateway:** report RDMA devices missing from the provider probe ([#307](https://github.com/qvest-digital/mxl-k8s/issues/307)) ([1a7f82d](https://github.com/qvest-digital/mxl-k8s/commit/1a7f82dddf54336abe140e5975f5817fe27a21bb))


### Dependencies

* **gomod:** update go modules to v0.37.0 ([#306](https://github.com/qvest-digital/mxl-k8s/issues/306)) ([bc5ae4a](https://github.com/qvest-digital/mxl-k8s/commit/bc5ae4a104632bd54477e04d8d13d70759a1ec7b))

## [1.0.0-rc.13](https://github.com/qvest-digital/mxl-k8s/compare/api/v1.0.0-rc.12...api/v1.0.0-rc.13) (2026-08-27)


### Dependencies

* **gomod:** update go modules ([#295](https://github.com/qvest-digital/mxl-k8s/issues/295)) ([c4e0d3c](https://github.com/qvest-digital/mxl-k8s/commit/c4e0d3c1826ee5ed3f680aa8ef1ca4930bb8281e))

## [1.0.0-rc.12](https://github.com/qvest-digital/mxl-k8s/compare/api/v1.0.0-rc.11...api/v1.0.0-rc.12) (2026-08-17)


### Features

* **api:** record when a flow's origin moves and what confirmed it ([#285](https://github.com/qvest-digital/mxl-k8s/issues/285)) ([0de36fc](https://github.com/qvest-digital/mxl-k8s/commit/0de36fc670ef4600d469dbb1ad98670c8cd382d3))
* emit events across the whole mirror flow lifecycle ([#285](https://github.com/qvest-digital/mxl-k8s/issues/285)) ([0de36fc](https://github.com/qvest-digital/mxl-k8s/commit/0de36fc670ef4600d469dbb1ad98670c8cd382d3))


### Bug Fixes

* **gateway:** release a source reader whose flow has no writer ([#285](https://github.com/qvest-digital/mxl-k8s/issues/285)) ([0de36fc](https://github.com/qvest-digital/mxl-k8s/commit/0de36fc670ef4600d469dbb1ad98670c8cd382d3))


### Dependencies

* **gomod:** update module github.com/stretchr/testify to v1.12.0 ([#282](https://github.com/qvest-digital/mxl-k8s/issues/282)) ([251c3ff](https://github.com/qvest-digital/mxl-k8s/commit/251c3ff8703a75546273142b7a2e3eb071cf003d))

## [1.0.0-rc.11](https://github.com/qvest-digital/mxl-k8s/compare/api/v1.0.0-rc.10...api/v1.0.0-rc.11) (2026-08-15)


### Features

* **gateway:** pace grain transmission and stop polling the fabric ([#275](https://github.com/qvest-digital/mxl-k8s/issues/275)) ([86655c3](https://github.com/qvest-digital/mxl-k8s/commit/86655c315f524285d7a152ae226a191ff6c07ddd))

## [1.0.0-rc.10](https://github.com/qvest-digital/mxl-k8s/compare/api/v1.0.0-rc.9...api/v1.0.0-rc.10) (2026-08-11)


### Bug Fixes

* judge flow activity and mirror wedges on evidence that moves ([#258](https://github.com/qvest-digital/mxl-k8s/issues/258)) ([27996eb](https://github.com/qvest-digital/mxl-k8s/commit/27996eb6b84d82ee170b8337d64d1df2ad4cf200))

## [1.0.0-rc.9](https://github.com/qvest-digital/mxl-k8s/compare/api/v1.0.0-rc.8...api/v1.0.0-rc.9) (2026-08-09)


### Features

* **api:** add targetAttemptCount to MxlFlowMirror status ([61ca815](https://github.com/qvest-digital/mxl-k8s/commit/61ca81526f1f79802753a14ab4a3d7477f00c530))


### Bug Fixes

* **gateway:** escalate and recover a target that cannot open ([#238](https://github.com/qvest-digital/mxl-k8s/issues/238)) ([61ca815](https://github.com/qvest-digital/mxl-k8s/commit/61ca81526f1f79802753a14ab4a3d7477f00c530)), closes [#236](https://github.com/qvest-digital/mxl-k8s/issues/236) [#237](https://github.com/qvest-digital/mxl-k8s/issues/237)

## [1.0.0-rc.8](https://github.com/qvest-digital/mxl-k8s/compare/api/v1.0.0-rc.7...api/v1.0.0-rc.8) (2026-08-06)


### Features

* **api:** gate provider selection on probed device counts ([8cc2965](https://github.com/qvest-digital/mxl-k8s/commit/8cc29658f1c9fe5f76c16582a8b0ae2754a1f214))
* **gateway:** publish probed providers and scope the fabric ([#227](https://github.com/qvest-digital/mxl-k8s/issues/227)) ([8cc2965](https://github.com/qvest-digital/mxl-k8s/commit/8cc29658f1c9fe5f76c16582a8b0ae2754a1f214))


### Dependencies

* **gateway:** move onto go-mxl 1.0.0-rc.14 ([8cc2965](https://github.com/qvest-digital/mxl-k8s/commit/8cc29658f1c9fe5f76c16582a8b0ae2754a1f214))

## [1.0.0-rc.7](https://github.com/qvest-digital/mxl-k8s/compare/api/v1.0.0-rc.6...api/v1.0.0-rc.7) (2026-08-01)


### Features

* **api:** add ReaderNotAdvancing condition reason ([37e5499](https://github.com/qvest-digital/mxl-k8s/commit/37e5499a130c77e52cee9f8b2958a0b422128d5f))


### Bug Fixes

* **gateway:** surface a source reader whose head stops advancing ([#217](https://github.com/qvest-digital/mxl-k8s/issues/217)) ([37e5499](https://github.com/qvest-digital/mxl-k8s/commit/37e5499a130c77e52cee9f8b2958a0b422128d5f))

## [1.0.0-rc.6](https://github.com/qvest-digital/mxl-k8s/compare/api/v1.0.0-rc.5...api/v1.0.0-rc.6) (2026-07-28)


### Miscellaneous

* **main:** release gateway 1.0.0-rc.9 ([#184](https://github.com/qvest-digital/mxl-k8s/issues/184)) ([1afb362](https://github.com/qvest-digital/mxl-k8s/commit/1afb362f196511dd746d71a6d08858520537f42c))

## [1.0.0-rc.5](https://github.com/qvest-digital/mxl-k8s/compare/api/v1.0.0-rc.4...api/v1.0.0-rc.5) (2026-07-27)


### Features

* **api:** share MirrorName and DefaultLeaseDuration ([b3ba4df](https://github.com/qvest-digital/mxl-k8s/commit/b3ba4df1def925b50b3659359c68a0ee218fe5c7))


### Bug Fixes

* **agent:** repoint intent mirrors when the flow origin moves ([b3ba4df](https://github.com/qvest-digital/mxl-k8s/commit/b3ba4df1def925b50b3659359c68a0ee218fe5c7))
* **agent:** report a stale Origin distinctly from an unknown flow ([b3ba4df](https://github.com/qvest-digital/mxl-k8s/commit/b3ba4df1def925b50b3659359c68a0ee218fe5c7))
* **gateway:** reap mirror finalizers orphaned by node removal ([#176](https://github.com/qvest-digital/mxl-k8s/issues/176)) ([b3ba4df](https://github.com/qvest-digital/mxl-k8s/commit/b3ba4df1def925b50b3659359c68a0ee218fe5c7))


### Dependencies

* **gomod:** update go modules to v0.36.3 ([#174](https://github.com/qvest-digital/mxl-k8s/issues/174)) ([594fb72](https://github.com/qvest-digital/mxl-k8s/commit/594fb72a4d43fa06db11ce08cc9c49b91fea678c))

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
