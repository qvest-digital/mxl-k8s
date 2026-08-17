# Contributor notes (Claude and humans)

Rules for working in this repository. Read them before opening a PR or
running an automated assistant against this tree.

## STOP. Use a git worktree.

Before ANY mutation of this repository -- edit, write, commit,
branch create, push, rebase, `gh pr create` -- the very first
action of the session is to set up a dedicated `git worktree`.
This rule has no exceptions. There is no change small enough to
skip it: typos, single-line fixes, doc-only PRs, even edits to
this file itself all require the same worktree dance.

The repository is worked on by multiple parallel sessions and
editors at once. Two writers in the same tree corrupt staging
state, step on each other's branches, and lose work without
warning -- the rule exists to make that physically impossible,
not to be polite about it.

Worktrees ALWAYS live under `<repo>/.claude/worktrees/`. Not
next to the repo, not in `/tmp`, not anywhere else -- that path
is the project's established convention and the location the
harness manages.

The preferred path is to delegate the mutation to a sub-agent
with `isolation: "worktree"` on the Agent call. The harness
then creates `<repo>/.claude/worktrees/agent-<id>/` automatically
with a unique id, so two concurrent sub-agents never share a
path. Do not assume a worktree from earlier in the session is
still mounted.

For a manual worktree (no sub-agent), pick a short random tag
so two sessions on the same topic never collide, and place the
worktree under `.claude/worktrees/`:

```sh
git fetch origin
id=$(openssl rand -hex 4)
git worktree add .claude/worktrees/<topic>-$id -b <topic> origin/main
cd .claude/worktrees/<topic>-$id
```

All subsequent edits, commits, and pushes happen from the
worktree.

The only thing allowed in the main checkout is read-only
inspection: `git log`, `git diff`, `git status`, `gh pr view`,
reading files, grep / find. Anything that touches the index, the
working tree, the branch list, or the remote is out.

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

This repo is a Go workspace with four modules:

| Path | Module | CGo |
| --- | --- | --- |
| `api/` | `github.com/qvest-digital/mxl-k8s/api` | no |
| `operator/` | `github.com/qvest-digital/mxl-k8s/operator` | no |
| `agent/` | `github.com/qvest-digital/mxl-k8s/agent` | no |
| `gateway/` | `github.com/qvest-digital/mxl-k8s/gateway` | libmxl + libmxl-fabrics (via `go-mxl`) |

`go.work` at the repo root enumerates all four `use` paths. Don't add
a `replace` directive to it. `api` and `operator` must not gain any
CGo dependency -- they have to build on a host without libmxl
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

The repository is set to `squash_merge_commit_title=PR_TITLE` and
`squash_merge_commit_message=COMMIT_MESSAGES`. The PR title becomes
the commit subject with the PR number appended; the body is GitHub's
bulleted list of the branch's commit messages. The PR description
does not reach the commit at all.

release-please reads that commit's subject, and footers -- the
trailing block of `token: value` lines. The bulleted subjects in the
body sit at column 0 with prose between them, so they are neither
subject nor footer and produce no changelog entry. One squash is one
entry, taken from the PR title. That is release-please's intended
model rather than a defect: intra-PR churn ("fix a bug introduced two
commits ago") never belonged in release notes.

Three rules follow:

1. **PR title is Conventional Commits.** Write the PR title in
   `<type>(<scope>): <subject>` form just as if it were a single
   commit subject. Subject `<= 72` chars, imperative mood.
2. **A PR carrying more than one release-relevant change is split,
   or carries a `BEGIN_COMMIT_OVERRIDE` block in its PR body.**
   release-please reads that block from the pull request over the
   API, so it works even though `COMMIT_MESSAGES` discards the body.
   The block replaces the whole commit message, so it repeats the
   entry the title would have produced and carries every footer the
   release needs:

   ```
   BEGIN_COMMIT_OVERRIDE
   feat(operator): adopt server-side apply for MxlFlowMirror status (#123)

   fix(gateway): close FlowReader on shutdown (#123)
   BREAKING CHANGE: operator now requires Kubernetes >= 1.30 for
   server-side apply on subresources
   END_COMMIT_OVERRIDE
   ```

   Adding the block to an already merged PR works: the next
   release-please run rebuilds the pending release PR from it.
3. **`!`, `BREAKING CHANGE:` (or `BREAKING-CHANGE:`, both are
   accepted) and `Release-As: X.Y.Z` go on the PR title or in the
   override block.** All three are parsed as footers, so they take
   effect only at the bottom of the squashed message. A branch with
   more than one commit ends in prose, and a footer written into an
   intermediate commit is silently inert.

Which package a line lands in follows the paths the commit touched,
not the scope on the line, and the strongest bump across a package's
lines wins. A commit touching only `api/` therefore bumps both
packages -- api compiles into all three binaries -- and an override
on such a commit puts every one of its lines in both changelogs.
That is a reason to split a mixed PR rather than override it.

### Working in a worktree

From the main checkout, create a worktree pinned to a feature
branch tracking `origin/main`. The worktree lives under
`.claude/worktrees/`, the project's canonical worktree location,
and gets a short random tag so two sessions on the same topic
never share a path:

```sh
git fetch origin
id=$(openssl rand -hex 4)
git worktree add .claude/worktrees/<topic>-$id -b <topic> origin/main
cd .claude/worktrees/<topic>-$id
```

When the PR has merged, drop the worktree, the local branch, and
the now-stale remote tracking ref:

```sh
git worktree remove .claude/worktrees/<topic>-<id>
git branch -D <topic>
git fetch --prune origin
```

`git worktree remove` is rarely needed when sub-agents manage the
worktree: the harness cleans up its own `agent-<id>` directories.
The teardown block above is for the manual `git worktree add`
case.

## Commits

- Use Conventional Commits with a scope matching the module being
  changed: `feat(api): …`, `fix(agent): …`, `docs(operator): …`. Cross-
  cutting changes use the broader `chore:`/`ci:`/`build:` types without
  a scope.
- Breaking changes get `!` (`feat(api)!: …`) or a `BREAKING CHANGE:`
  footer.
- Subject line ≤ 72 chars, imperative mood ("add", "fix", not "added",
  "fixes"). Body wraps at 72.
- Prefer small, focused commits. They survive in the squashed body as
  the record of how the change was built; the release tooling reads
  the PR title, not them.

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

`release-please` is configured for two packages, and they share one
release pull request (`separate-pull-requests: false`):

| Package | Covers | Tag | Changelog |
| --- | --- | --- | --- |
| `api` | `api/` | `api/vX.Y.Z` | `api/CHANGELOG.md` |
| `.` | operator, agent, gateway, shim, chart | `vX.Y.Z` | `CHANGELOG.md` |

- The bundle version is the chart version. `charts/mxl-k8s/Chart.yaml`
  carries it twice: `version` (the chart) and `appVersion` (the module
  images the chart bundles). Both are written by release-please.
- The four images publish under that one version, so
  `ghcr.io/qvest-digital/mxl-k8s/agent:vX.Y.Z` and the chart at `X.Y.Z`
  always belong together. `<component>.image.tag` in `values.yaml` is
  empty and resolves to `v<appVersion>`; there is no pin to bump.
- `api` keeps its own version because it is the only module meant to be
  imported from outside this repository. The `require` lines in
  `agent/go.mod`, `operator/go.mod` and `gateway/go.mod` carry a
  `// x-release-please-version` annotation and are rewritten inside the
  api release PR, so the pin and the tag land in the same commit. No
  `go.sum` change follows: api is resolved through a local `replace`,
  so it has no sum entry. `hack/check-api-pins.sh` gates the invariant.
- The Go module proxy resolves `github.com/qvest-digital/mxl-k8s/api@v0.1.0`
  against the `api/v0.1.0` tag -- don't move tag prefixes by hand.
- Don't hand-tag or hand-edit `CHANGELOG.md` -- let the workflow do it.
  The frozen `agent/`, `operator/`, `gateway/`, `shim/` and
  `charts/mxl-k8s/` changelogs are history up to `1.0.0-rc.13`; entries
  from `1.0.0-rc.14` on land in the repository `CHANGELOG.md`.
- The root package excludes `.github`, `docs`, `examples`, `hack`,
  `kind-diagnostics` and `test`: nothing under them ships in an image
  or the chart. A commit confined to those paths cuts no release. Every
  other path does, including `api/` -- an api change is compiled into
  all three binaries, so it has to reach a new image.
- Nothing opens a pull request after a release. The pins that used to
  need one (api requires, chart image tags) are written by
  release-please or derived at render time, which is what makes the
  train work on a repository without auto-merge.

## Build

- `api`, `operator`, and `agent` are pure Go.
- `gateway` is the only cgo module. It links `libmxl` + `libmxl-fabrics`
  through the `go-mxl` binding, so both must be installed with headers
  and pkg-config files before `go build` works there. See `docs/BUILD.md`.
- Integration testing runs against a local KIND cluster (`make kind-up`,
  `make kind-test`); see `docs/KIND.md`. Plain `go test ./...` jobs need
  no cluster.

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
- `images.yml` filters on path and then again on whether the diff
  can reach a binary, because the per-image filters glob whole
  module directories and so match a changelog or a test-only
  requirement the same as a source file. That second pass is the
  `inert` block in `meta`, and each arm asserts the binaries come
  out identical to the ones main already published: markdown only,
  release-please's own branch (whose api pin is inert under the
  `replace` directives), and a requirement bump that `go list
  -deps` does not find in any `cmd/` import graph. Adding an arm
  means making that same claim for it.
- The `kind` filter selects the suite independently of any image
  build, for changes that alter what the cluster runs without
  producing a binary: the chart, the CRDs, the RBAC, the demo
  manifests, the harness. `kind-up` is the only place in CI that
  installs the chart rather than rendering it, so a path that
  changes the installed result belongs on that filter.

## When in doubt

Ask the maintainer before changing the public Go API of `api`,
the module paths, or the release/tagging strategy.
