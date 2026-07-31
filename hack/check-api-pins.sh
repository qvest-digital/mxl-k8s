#!/usr/bin/env bash
# check-api-pins.sh -- fail when a consuming module's api require
# does not name the api version release-please last released.
#
# agent, operator and gateway resolve api through a local replace
# directive, so the require line never affects a build inside this
# repository and a stale pin compiles and tests green. It is what
# `go get github.com/qvest-digital/mxl-k8s/agent@vX` resolves for a
# consumer outside the repository, and what the module's own release
# publishes as its dependency set.
#
# release-please rewrites the three require lines inside the api
# release pull request (extra-files on the api package, driven by the
# `// x-release-please-version` annotation on each line), so the pin
# and the tag land in the same commit. This gate is the backstop for
# the ways that can silently stop working: the annotation being
# dropped by an editor, a module joining the workspace without one,
# or a hand-edited require.
#
# bash 3.2 compatible: no associative arrays, no mapfile.

set -eu

MANIFEST="${MANIFEST:-.github/release-please-manifest.json}"
MODULES="${MODULES:-agent operator gateway}"
API_MODULE="github.com/qvest-digital/mxl-k8s/api"
ANNOTATION="x-release-please-version"

[ -f "$MANIFEST" ] || { echo "no such file: $MANIFEST" >&2; exit 2; }

released="$(jq -re '.api' "$MANIFEST")"

rc=0
for m in $MODULES; do
  gomod="${m}/go.mod"
  if [ ! -f "$gomod" ]; then
    echo "FAIL ${m}: no such file: ${gomod}" >&2
    rc=1
    continue
  fi

  line="$(grep -F "${API_MODULE} v" "$gomod" | grep -v '^replace' | head -1)"
  if [ -z "$line" ]; then
    echo "FAIL ${m}: no ${API_MODULE} require in ${gomod}" >&2
    rc=1
    continue
  fi

  case "$line" in
    *"${ANNOTATION}"*) ;;
    *)
      echo "FAIL ${m}: require line carries no '// ${ANNOTATION}'; release-please cannot bump it" >&2
      rc=1
      continue
      ;;
  esac

  pinned="$(echo "$line" | sed -E "s#.*${API_MODULE} v([^ ]+).*#\1#")"
  if [ "$pinned" = "$released" ]; then
    echo "ok   ${m}: v${pinned}"
  else
    echo "FAIL ${m}: pins api v${pinned} but v${released} is released" >&2
    rc=1
  fi
done

if [ "$rc" -ne 0 ]; then
  echo >&2
  echo "Run: for m in ${MODULES}; do (cd \$m && go mod edit -require=${API_MODULE}@v${released}); done" >&2
  echo "go mod edit preserves the annotation comment. No go.sum change follows:" >&2
  echo "api is replaced by a filesystem path, so it has no sum entry." >&2
fi
exit "$rc"
