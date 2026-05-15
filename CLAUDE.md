# Contributor notes (Claude and humans)

Rules for working in this repository. Read them before opening a PR or
running an automated assistant against this tree.

## Documentation

- Keep `README.md` and code comments tight. State facts; don't speculate.
- Do not write about how a component "should be used" or "is ideal for"
  any particular system. The control plane wraps libmxl + libmxl-fabrics;
  downstream choice is not ours to characterize.
- Don't invent API behavior. If you can't verify it by reading libmxl's
  headers, go-mxl's source, or testing against a real install, leave it out.
- Don't add SPDX headers or copyright lines to new files unless you're
  preserving existing ones from an external source.

## Multi-module workspace

This repo is a Go workspace with five modules:

| Path | Module | CGo |
| --- | --- | --- |
| `api/` | `github.com/qvest-digital/mxl-k8s/api` | no |
| `ipc/` | `github.com/qvest-digital/mxl-k8s/ipc` | no |
| `operator/` | `github.com/qvest-digital/mxl-k8s/operator` | no |
| `agent/` | `github.com/qvest-digital/mxl-k8s/agent` | libmxl (via `go-mxl`) |
| `gateway/` | `github.com/qvest-digital/mxl-k8s/gateway` | libmxl + libmxl-fabrics (via `go-mxl/fabrics`) |

`go.work` at the repo root enumerates all five `use` paths. Don't add
a `replace` directive to it. `api`, `ipc`, and `operator` must not gain
any CGo dependency — they have to build on a host without libmxl
installed.

## Commits

- Use Conventional Commits with a scope matching the module being
  changed: `feat(api): …`, `fix(agent): …`, `docs(operator): …`. Cross-
  cutting changes use the broader `chore:`/`ci:`/`build:` types without
  a scope.
- Breaking changes get `!` (`feat(api)!: …`) or a `BREAKING CHANGE:`
  footer.
- Subject line ≤ 72 chars. Body wraps at 72.
- Prefer small, focused commits. The release tooling derives version
  bumps and the changelog from commit subjects.

## Versioning and tags

- Each module is released independently. `release-please` is configured
  for five packages; tags take the form `<module>/vMAJOR.MINOR.PATCH`
  (for example `api/v0.1.0`, `agent/v0.1.0`).
- The Go module proxy resolves `github.com/qvest-digital/mxl-k8s/api@v0.1.0`
  against the `api/v0.1.0` tag — don't move tag prefixes by hand.
- Don't hand-tag or hand-edit `CHANGELOG.md` — let the workflow do it.

## Build

- `api`, `ipc`, and `operator` are pure Go.
- `agent` and `gateway` are cgo. `libmxl` (and for the gateway,
  `libmxl-fabrics`) must be installed with headers and a pkg-config file
  before `go build` works in those modules. See `docs/BUILD.md`.
- Integration tests that require a running libmxl writer go under the
  build tag `mxl_integration`. The CI lint/vet/build jobs don't run
  them.

## When in doubt

Ask the maintainer before changing the public Go API of `api` or `ipc`,
the module paths, or the release/tagging strategy.
