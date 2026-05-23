#!/usr/bin/env bash
# Assert the five mxl.qvest-digital.com CRDs reach the Established
# condition. The operator's reconcilers cannot bind to the API
# objects they manage until each CRD's schema is registered.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

CRDS=(
  mxldomains.mxl.qvest-digital.com
  mxlflows.mxl.qvest-digital.com
  mxlflowmirrors.mxl.qvest-digital.com
  mxlreceivers.mxl.qvest-digital.com
  mxlnodecapabilities.mxl.qvest-digital.com
)

for crd in "${CRDS[@]}"; do
  "${KUBECTL[@]}" wait --for=condition=Established --timeout=60s "crd/${crd}" \
    || fail "CRD ${crd} not Established"
  echo "  ${crd}: Established"
done
