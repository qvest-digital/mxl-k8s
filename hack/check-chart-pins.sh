#!/usr/bin/env bash
# check-chart-pins.sh -- fail when the chart pins a module image
# older than that module's newest release tag.
#
# The chart carries no pins of its own on a release PR: it bumps its
# own version and inherits whatever values.yaml holds on main at
# merge time. A chart release cut before a component's pin bump has
# landed therefore packages a stale image, and the PR looks entirely
# healthy while doing it -- mergeable, green, and wrong.
#
# Reads the tag pinned per component out of values.yaml and compares
# it with the newest <component>/v* tag in the repository.
#
# bash 3.2 compatible: no associative arrays, no mapfile.

set -uo pipefail

VALUES="${VALUES:-charts/mxl-k8s/values.yaml}"
COMPONENTS="${COMPONENTS:-operator agent gateway}"

[ -f "$VALUES" ] || { echo "no such file: $VALUES" >&2; exit 2; }

# pinned_tag <component> -- the image tag values.yaml pins for the
# component, read from the renovate-annotated line so a stray tag:
# elsewhere in the file cannot be mistaken for it.
pinned_tag() {
  grep -E "tag: \"v[0-9][^\"]*\".*depName=ghcr\.io/qvest-digital/mxl-k8s/$1( |$)" "$VALUES" \
    | head -1 | sed -E 's/.*tag: "(v[^"]+)".*/\1/'
}

# released_tag <component> -- newest <component>/vX tag, by version.
released_tag() {
  git tag --list "$1/v*" | sed "s#^$1/##" | sort -V | tail -1
}

rc=0
for c in $COMPONENTS; do
  pinned="$(pinned_tag "$c")"
  released="$(released_tag "$c")"

  if [ -z "$pinned" ]; then
    echo "FAIL ${c}: no pinned tag found in ${VALUES}" >&2
    rc=1
    continue
  fi
  if [ -z "$released" ]; then
    echo "skip ${c}: no release tag yet"
    continue
  fi

  if [ "$pinned" = "$released" ]; then
    echo "ok   ${c}: ${pinned}"
    continue
  fi

  # Newer-than-released is not a failure: a pin bump merges before
  # the chart release that carries it, so main legitimately sits
  # ahead of the chart's own last release.
  newest="$(printf '%s\n%s\n' "$pinned" "$released" | sort -V | tail -1)"
  if [ "$newest" = "$pinned" ]; then
    echo "ok   ${c}: ${pinned} (ahead of released ${released})"
  else
    echo "FAIL ${c}: chart pins ${pinned} but ${released} is released" >&2
    rc=1
  fi
done

if [ "$rc" -ne 0 ]; then
  echo >&2
  echo "The chart would package an image older than the newest release." >&2
  echo "Let the post-release/chart-pins bump merge first, then re-run." >&2
fi
exit "$rc"
