#!/usr/bin/env bash
# Set <component>.image.tag in charts/mxl-k8s/values.yaml for the
# floating dev chart, and emit the bundled-component table for any
# build.
#
# Usage:
#   hack/chart-resolve-tags.sh <mode-or-version>
#
# The single argument is either an explicit mode (dev | release) or a
# chart version string, in which case the mode is auto-detected:
#   - "0.0.0-dev*"  -> dev      (track main HEAD; rewrite tags to "dev")
#   - anything else -> release  (leave the tags empty)
#
# Release builds ship no per-component pin at all. operator, agent and
# gateway are released under the chart's own version, so an empty tag
# resolves to "v<appVersion>" in mxlk8s.image and there is nothing to
# keep in step. Only the dev channel rewrites the tags, to "dev", so
# the 0.0.0-dev chart tracks the :dev images built on every merge to
# main rather than a version that was never released.
#
# In dev mode the script writes in place; after running locally,
# `git checkout -- charts/mxl-k8s/values.yaml` reverts.
#
# On stdout: a Markdown table of the effective tags so a caller (the
# chart workflow) can paste it into the GitHub release notes or the
# workflow summary.

set -euo pipefail

arg="${1:?usage: $0 <dev|release|<chart-version>>}"
values=charts/mxl-k8s/values.yaml
chart=charts/mxl-k8s/Chart.yaml
components=(operator agent gateway)

case "$arg" in
  dev|release)  mode="$arg" ;;
  0.0.0-dev*)   mode=dev ;;
  *)            mode=release ;;
esac

if [ "$mode" = dev ]; then
  for c in "${components[@]}"; do
    yq -i ".${c}.image.tag = \"dev\"" "$values"
  done
fi

# The same fallback mxlk8s.image applies: an empty tag resolves to the
# chart appVersion prefixed with "v".
app_version=$(yq '.appVersion' "$chart")

echo "## Bundled component versions"
echo ""
echo "| Component | Image tag |"
echo "| --- | --- |"
for c in "${components[@]}"; do
  t=$(yq ".${c}.image.tag" "$values")
  if [ -z "$t" ] || [ "$t" = "null" ]; then
    t="v${app_version}"
  fi
  echo "| ${c} | \`${t}\` |"
done
