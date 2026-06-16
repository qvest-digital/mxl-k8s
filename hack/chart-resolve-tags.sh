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
#   - anything else -> release  (keep the committed per-component pins)
#
# Release builds ship the pins committed in values.yaml as-is. Renovate
# keeps those current: a bump opens a deps(chart) PR that release-please
# turns into a chart release, so the committed pins are already correct
# at package time and nothing is resolved. Only the dev channel rewrites
# the tags, to "dev", so the 0.0.0-dev chart tracks the :dev images
# built on every merge to main.
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

echo "## Bundled component versions"
echo ""
echo "| Component | Image tag |"
echo "| --- | --- |"
for c in "${components[@]}"; do
  t=$(yq ".${c}.image.tag" "$values")
  echo "| ${c} | \`${t}\` |"
done
