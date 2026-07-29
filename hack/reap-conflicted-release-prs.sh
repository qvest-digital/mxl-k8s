#!/usr/bin/env bash
# reap-conflicted-release-prs.sh -- close release PRs that can no
# longer merge, so release-please recreates them from current main.
#
# release-please rewrites a component's release branch when that
# component's pending release changes, not when main moves under it.
# A branch therefore keeps the release-please-manifest.json snapshot
# it was cut from, and every component shares that one file: once a
# sibling release lands, the stale neighbouring keys collide with
# main. Whether a given PR survives comes down to whether its key is
# adjacent to one that moved, which is an accident of alphabetical
# order in a JSON file.
#
# Neither recovery the tooling offers helps. A workflow_dispatch of
# release-please regenerates a release PR whose contents are stale,
# not one whose base has moved, and GitHub's "Update branch" refuses
# on conflict. Closing is what works: release-please sees no open PR
# for the component and cuts a fresh one against current main.
#
# Runs before the release-please step so the same run recreates what
# it closes. Only PRs release-please itself labels are considered,
# and only on an unambiguous CONFLICTING -- never on UNKNOWN, which
# is what GitHub reports while it recomputes mergeability.
#
# That recompute is the reason for the polling below. On a push
# trigger this runs seconds after the merge that invalidated the
# siblings, and GitHub answers UNKNOWN for every one of them: without
# waiting, the run reaps nothing and the conflict survives until
# someone dispatches the workflow by hand.
#
# bash 3.2 compatible: no associative arrays, no mapfile.

set -uo pipefail

LABEL="${LABEL:-autorelease: pending}"
DRY_RUN="${DRY_RUN:-false}"
# Per-PR budget for GitHub to settle on a mergeable state.
SETTLE_TRIES="${SETTLE_TRIES:-10}"
SETTLE_SLEEP="${SETTLE_SLEEP:-6}"

command -v gh >/dev/null 2>&1 || { echo "gh not on PATH" >&2; exit 1; }

numbers="$(gh pr list --state open --label "$LABEL" \
  --json number --jq '.[].number' 2>/dev/null)"

if [ -z "$numbers" ]; then
  echo "no open PRs labelled '${LABEL}'"
  exit 0
fi

reaped=0
for n in $numbers; do
  # Queried per PR rather than in the list call: mergeable is
  # computed lazily, and asking for it one at a time gives GitHub the
  # chance to have settled on a real answer. Retried because "lazily"
  # can mean tens of seconds after a push.
  title="$(gh pr view "$n" --json title --jq .title 2>/dev/null)"
  state=""
  try=0
  while [ "$try" -lt "$SETTLE_TRIES" ]; do
    state="$(gh pr view "$n" --json mergeable --jq .mergeable 2>/dev/null)"
    case "$state" in
      CONFLICTING|MERGEABLE) break ;;
    esac
    try=$((try + 1))
    [ "$try" -lt "$SETTLE_TRIES" ] && sleep "$SETTLE_SLEEP"
  done

  case "$state" in
    CONFLICTING)
      echo "reaping #${n} (${state}): ${title}"
      if [ "$DRY_RUN" = "true" ]; then
        echo "  DRY_RUN set, leaving it open"
        continue
      fi
      gh pr comment "$n" --body \
        "Closed automatically: this release PR conflicts with main and release-please does not rebase a branch whose base has moved. A replacement is cut from current main in the same workflow run; the release it carries is unchanged." \
        >/dev/null 2>&1 || true
      if gh pr close "$n" --delete-branch >/dev/null 2>&1; then
        reaped=$((reaped + 1))
      else
        echo "  could not close #${n}; leaving it for a human" >&2
      fi
      ;;
    MERGEABLE)
      echo "ok      #${n}: ${title}"
      ;;
    *)
      # Still UNKNOWN after the budget, or an empty response. Doing
      # nothing is correct: a PR closed on an unknown would churn the
      # release for no reason. The next run gets another go.
      echo "skip    #${n} (mergeable=${state:-?} after ${SETTLE_TRIES} tries): ${title}"
      ;;
  esac
done

echo "reaped ${reaped} conflicted release PR(s)"
