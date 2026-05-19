#!/usr/bin/env bash
# Resolve <component>.image.tag in charts/mxl-k8s/values.yaml from
# .github/release-please-manifest.json so the chart pins per
# component instead of falling back to a single chart-wide
# Chart.AppVersion.
#
# Usage:
#   hack/chart-resolve-tags.sh <mode-or-version>
#
# The single argument is either an explicit mode (dev | release)
# or a chart version string, in which case the mode is auto-
# detected:
#   - "0.0.0-dev*"  -> dev      (track main HEAD; pin "dev")
#   - anything else -> release  (read .github/release-please-manifest.json)
#
# The script writes in place. After running locally,
# `git checkout -- charts/mxl-k8s/values.yaml` reverts.
#
# On stdout: a Markdown table of the resolved tags so a caller (the
# chart workflow) can paste it into the GitHub release notes or the
# workflow summary.

set -euo pipefail

arg="${1:?usage: $0 <dev|release|<chart-version>>}"
values=charts/mxl-k8s/values.yaml
manifest=.github/release-please-manifest.json
components=(operator agent gateway)

case "$arg" in
  dev|release)  mode="$arg" ;;
  0.0.0-dev*)   mode=dev ;;
  *)            mode=release ;;
esac

case "$mode" in
  dev)
    for c in "${components[@]}"; do
      yq -i ".${c}.image.tag = \"dev\"" "$values"
    done
    ;;
  release)
    for c in "${components[@]}"; do
      v=$(jq -r ".\"${c}\"" "$manifest")
      yq -i ".${c}.image.tag = \"v${v}\"" "$values"
    done
    ;;
esac

echo "## Bundled component versions"
echo ""
echo "| Component | Image tag |"
echo "| --- | --- |"
for c in "${components[@]}"; do
  t=$(yq ".${c}.image.tag" "$values")
  echo "| ${c} | \`${t}\` |"
done
