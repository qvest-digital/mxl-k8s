#!/usr/bin/env bash
# Resolve <component>.image.tag in charts/mxl-k8s/values.yaml from
# .github/{prerelease,release}-manifest.json so the chart pins per
# component instead of falling back to a single chart-wide
# Chart.AppVersion.
#
# Usage:
#   hack/chart-resolve-tags.sh <mode-or-version>
#
# The single argument is either an explicit mode (dev | rc | stable)
# or a chart version string, in which case the mode is auto-detected:
#   - "0.0.0-dev*"       -> dev      (track main HEAD; pin "dev")
#   - any "<X.Y.Z>-..."  -> rc       (read .github/prerelease-manifest.json)
#   - any other          -> stable   (read .github/release-manifest.json)
#
# The script writes in place. After running locally,
# `git checkout -- charts/mxl-k8s/values.yaml` reverts.
#
# On stdout: a Markdown table of the resolved tags so a caller (the
# chart workflow) can paste it into the GitHub release notes or the
# workflow summary.

set -euo pipefail

arg="${1:?usage: $0 <dev|rc|stable|<chart-version>>}"
values=charts/mxl-k8s/values.yaml
components=(operator agent gateway)

case "$arg" in
  dev|rc|stable)  mode="$arg" ;;
  0.0.0-dev*)     mode=dev ;;
  *-*)            mode=rc ;;
  *)              mode=stable ;;
esac

case "$mode" in
  dev)
    for c in "${components[@]}"; do
      yq -i ".${c}.image.tag = \"dev\"" "$values"
    done
    ;;
  rc)
    for c in "${components[@]}"; do
      v=$(jq -r ".\"${c}\"" .github/prerelease-manifest.json)
      yq -i ".${c}.image.tag = \"v${v}\"" "$values"
    done
    ;;
  stable)
    for c in "${components[@]}"; do
      v=$(jq -r ".\"${c}\"" .github/release-manifest.json)
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
