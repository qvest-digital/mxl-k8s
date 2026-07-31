# Changelog

Frozen at 1.0.0-rc.13. operator, agent, gateway, shim and the
chart now release together under one repository version; entries
from 1.0.0-rc.14 on are in the repository CHANGELOG.md.

## [1.0.0-rc.6](https://github.com/qvest-digital/mxl-k8s/compare/shim/v1.0.0-rc.5...shim/v1.0.0-rc.6) (2026-07-30)


### Bug Fixes

* **agent:** follow the flow origin when it moves between nodes ([#206](https://github.com/qvest-digital/mxl-k8s/issues/206)) ([4c3cc00](https://github.com/qvest-digital/mxl-k8s/commit/4c3cc008197e74576799cd63204f4dace87afa20))
* **shim:** report a producer attaching to a pre-existing flow ([4c3cc00](https://github.com/qvest-digital/mxl-k8s/commit/4c3cc008197e74576799cd63204f4dace87afa20))

## [1.0.0-rc.5](https://github.com/qvest-digital/mxl-k8s/compare/shim/v1.0.0-rc.4...shim/v1.0.0-rc.5) (2026-07-27)


### Bug Fixes

* **shim:** reach libc through direct syscalls, not dlsym ([#190](https://github.com/qvest-digital/mxl-k8s/issues/190)) ([1823d56](https://github.com/qvest-digital/mxl-k8s/commit/1823d56e42c7619cde719693cbb5cf90575cd4b4))


### Miscellaneous

* **main:** release gateway 1.0.0-rc.9 ([#184](https://github.com/qvest-digital/mxl-k8s/issues/184)) ([1afb362](https://github.com/qvest-digital/mxl-k8s/commit/1afb362f196511dd746d71a6d08858520537f42c))

## [1.0.0-rc.4](https://github.com/qvest-digital/mxl-k8s/compare/shim/v1.0.0-rc.3...shim/v1.0.0-rc.4) (2026-07-27)


### Bug Fixes

* **examples,shim:** mount /run/mxl directory for intent socket access ([#172](https://github.com/qvest-digital/mxl-k8s/issues/172)) ([ed27b97](https://github.com/qvest-digital/mxl-k8s/commit/ed27b97c18259e160b533869dd31e4ae825c7b54))

## [1.0.0-rc.3](https://github.com/qvest-digital/mxl-k8s/compare/shim/v1.0.0-rc.2...shim/v1.0.0-rc.3) (2026-05-27)


### Miscellaneous

* contributor-review pass on docs, comments, and typography ([#46](https://github.com/qvest-digital/mxl-k8s/issues/46)) ([cddc9ba](https://github.com/qvest-digital/mxl-k8s/commit/cddc9bad1535087a19d04570b77438e6df27a1eb))

## [1.0.0-rc.2](https://github.com/qvest-digital/mxl-k8s/compare/shim/v1.0.0-rc.1...shim/v1.0.0-rc.2) (2026-05-19)


### Bug Fixes

* **shim,agent,gateway:** close intent path and quiet reconciler noise ([#41](https://github.com/qvest-digital/mxl-k8s/issues/41)) ([36d6d88](https://github.com/qvest-digital/mxl-k8s/commit/36d6d883aab66565d90b7832c04c0cfe3cf0d116))

## [1.0.0-rc.1](https://github.com/qvest-digital/mxl-k8s/compare/shim/v1.0.0-rc.0...shim/v1.0.0-rc.1) (2026-05-18)


### Features

* **shim:** libmxl-intent.so LD_PRELOAD shim ([c9a9ae7](https://github.com/qvest-digital/mxl-k8s/commit/c9a9ae70e4e08caffd47eba8ac8b77e1ddedd959))
