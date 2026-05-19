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

## Branches and PRs

- Direct commits to `main` are off by default. Every change opens a
  feature branch and a PR against `main`. Commit directly to `main`
  only when the maintainer has explicitly approved it for that
  specific change.
- Force-pushes are off by default. Force-pushing to `main` is
  prohibited. Force-pushing to a feature branch is only permitted
  with explicit approval, because another editor may be reviewing
  the branch or checked out against it.
- Use a separate `git worktree` per branch when working alongside
  other editors on the repository. Worktrees keep the main checkout
  clean while sharing the object database, so parallel sessions do
  not collide over staged changes or the working tree.
- Merge PRs with **Squash and merge**. release-please derives version
  bumps and changelog entries from the resulting single commit on
  `main`, and a noisy merge of dozens of intermediate commits would
  bury the release-relevant ones.
- Delete the feature branch on the remote as soon as the PR is
  merged (GitHub's "Delete branch" button on the merged PR, or the
  repo-level "Automatically delete head branches" setting). Stale
  remote branches confuse the next contributor's `git fetch` and
  inflate `git branch -r` output.

### Squash commit format for release-please

GitHub's squash-merge uses the PR title as the resulting commit
subject (with the PR number appended) and the PR body as the commit
body. release-please parses that commit on `main` to decide what
gets a changelog entry and how the version bumps. Two consequences:

1. **PR title is Conventional Commits.** Write the PR title in
   `<type>(<scope>): <subject>` form just as if it were a single
   commit subject. Subject `<= 72` chars, imperative mood.
2. **Multiple release-relevant changes go in the PR body, at the
   bottom, one per line.** release-please reads additional
   conventional-commit footer lines and emits one changelog entry
   per line. Add them after the prose explanation, separated by a
   blank line. Example for a PR that touches two modules:

   ```
   feat(operator): adopt server-side apply for MxlFlowMirror status

   Status updates collided with the controller-runtime cache when
   the gateway raced the operator. Switch to SSA so only the
   reconciler's field manager owns status.targetInfo.

   fix(gateway): close FlowReader on shutdown
   BREAKING CHANGE: operator now requires Kubernetes >= 1.30 for
   server-side apply on subresources
   ```

   That single squash commit produces three release-relevant
   entries: the `feat(operator)` (driving the minor bump), the
   `fix(gateway)` (driving a patch bump on gateway), and a
   `BREAKING CHANGE` footer (driving a major bump on operator).
   Use `BREAKING CHANGE:` or `BREAKING-CHANGE:` (release-please
   accepts both) and `Release-As: X.Y.Z` for explicit overrides.

### Working in a worktree

From the main checkout, create a worktree pinned to a feature
branch tracking `origin/main`:

```sh
git fetch origin
git worktree add ../mxl-k8s.<topic> -b <topic> origin/main
cd ../mxl-k8s.<topic>
```

When the PR has merged, drop the worktree, the local branch, and
the now-stale remote tracking ref:

```sh
git worktree remove ../mxl-k8s.<topic>
git branch -D <topic>
git fetch --prune origin
```

## Commits

- Use Conventional Commits with a scope matching the module being
  changed: `feat(api): …`, `fix(agent): …`, `docs(operator): …`. Cross-
  cutting changes use the broader `chore:`/`ci:`/`build:` types without
  a scope.
- Breaking changes get `!` (`feat(api)!: …`) or a `BREAKING CHANGE:`
  footer.
- Subject line ≤ 72 chars, imperative mood ("add", "fix", not "added",
  "fixes"). Body wraps at 72.
- Prefer small, focused commits. The release tooling derives version
  bumps and the changelog from commit subjects.

### Message content

A commit message documents why a change exists, in terms that stay
useful when read alone, years later, by someone with no memory of
the work that produced it. The same rules apply to PR descriptions.

- Explain *why*. The diff shows *what*; don't restate it.
- Stay scoped to this repository and this change. No speculation
  about upstream, downstream, future work, or follow-ups. If
  something was deliberately left out of the diff, name it and the
  reason — only when that omission matters for understanding the
  present change.
- Reference another repository or project only when its state is
  the direct reason for the change (a dependency bump, a vendored
  fix, an API contract pinned to a published version). Context for
  reviewers, gratitude, or cross-linking belongs in the PR thread
  or an issue, not the commit.
- Write declarative facts. No personal pronouns ("I", "we", "you").
  Don't address a reader: no "note that…", "as you can see…", "we
  decided to…", "this should help…".
- Don't narrate. No history of what was tried first, what failed,
  or what alternatives were considered.
- No filler verbs without specifics. "Clean up", "improve",
  "refactor" alone tell nothing; either name the actual change or
  drop the line.
- No checklists, "Summary" / "Test plan" sections, marketing
  phrasing, or emojis. Those belong in the PR description if
  anywhere.
- No tool-authored `Co-Authored-By:` trailers — the message
  describes the change, not the process that produced it.
- Cross-reference an issue or PR only when its content is itself
  the reason for the change (`closes #N` where the issue is the
  why). Vague "see #N for context" pointers do not belong here.

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

## Shell scripts

- All bash scripts under `hack/` (and anything else invoked from the
  Makefile) must run on bash 3.2. macOS still ships
  `/bin/bash` 3.2.57, and `make` recipes resolve bare `bash` via
  `PATH`, which on a default macOS install hits `/bin/bash` first.
- No `declare -A` / associative arrays, no `${var,,}` / `${var^^}`
  case conversion, no `mapfile` / `readarray`, no `[[ ... =~ ]]` with
  capture groups via `BASH_REMATCH` assumptions that differ in 3.2,
  no `${!prefix*}` indirect expansion tricks beyond what 3.2 supports.
  Use parallel indexed arrays in place of associative arrays.
- Verify a script parses under the system bash before committing:
  `/bin/bash -n hack/<script>.sh`.

## Test

- Assertions use `github.com/stretchr/testify/require` (plus `assert`
  when a single failure should not stop the test). Diffs use
  `github.com/google/go-cmp/cmp`. Mocks are generated by `mockery` v3
  from `.mockery.yaml` into `<pkg>/mocks/`. Reconciler tests use
  `sigs.k8s.io/controller-runtime/pkg/envtest` for branches with real
  apiserver behaviour; pure handlers and observer stubs use
  `fake.NewClientBuilder()`. Goroutine leaks are checked with
  `go.uber.org/goleak` in packages that spawn long-running loops.
- `make test` runs the pure-Go modules through `gotestsum`. JUnit XML
  and coverage profiles land in `bin/` keyed by module name. The
  operator suite needs the kube-apiserver + etcd binaries that
  `make envtest-assets` provisions; their path is exported via
  `KUBEBUILDER_ASSETS`.
- `make test-gateway` runs the gateway suite. It requires libmxl +
  libmxl-fabrics on the host. CI runs it inside the `go-mxl-builder`
  container.
- `make mocks` regenerates every mock listed in `.mockery.yaml`.
  `make mocks-check` fails the build on drift.
- CI publishes a per-module JUnit check via `dorny/test-reporter` and
  a coverage summary via `octocov`. Both run in the `report` job that
  fans in artifacts from the per-module test jobs.

## CI path filters

`ci.yml` and `images.yml` each open with a `changes` job
(`dorny/paths-filter@v3`) that scopes every downstream job to the
diff. Two consequences for contributors:

- The filter list is part of each module's "what depends on me"
  contract. If a Go module starts importing a sibling that it
  didn't before, that sibling's path glob must be added to the
  importing module's filter in `ci.yml`. Same for `images.yml`:
  if a Dockerfile starts COPYing a sibling module, that module
  goes on the image's filter. Without the update the dependent
  job stops re-running when its dependency moves.
- Branch protection requires only the `ci-summary` / `images-
  summary` jobs (which run unconditionally and fail iff any
  required upstream is in `failure` / `cancelled`). Do not add
  individual conditional jobs to the required-checks list -- a
  skipped check on an unrelated diff would block the PR.

## When in doubt

Ask the maintainer before changing the public Go API of `api` or `ipc`,
the module paths, or the release/tagging strategy.
