#!/usr/bin/env bash
# Assert the bundled dashboard has everything it needs to draw: the
# exporter's samples reached Prometheus, Grafana loaded the board, and
# the datasource uid the panels name resolves.
#
# Case 47 proves the exporter serves the metrics. This one proves the
# path from there to a panel, which is the part a chart consumer gets
# wrong: a ServiceMonitor Prometheus does not select, a ConfigMap the
# sidecar does not watch, or a datasource uid that names nothing.
#
# Skipped when the cluster was brought up with MONITORING=0.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

MONITORING_NAMESPACE="${MONITORING_NAMESPACE:-monitoring}"
DASHBOARD_UID="${DASHBOARD_UID:-mxl-flow-metrics}"
DATASOURCE_UID="${DATASOURCE_UID:-prometheus}"
GRAFANA_AUTH="${GRAFANA_AUTH:-admin:admin}"
SCRAPE_TIMEOUT_SECS="${SCRAPE_TIMEOUT_SECS:-90}"

if ! "${KUBECTL[@]}" get namespace "$MONITORING_NAMESPACE" >/dev/null 2>&1; then
  echo "  no ${MONITORING_NAMESPACE} namespace; cluster is up without the monitoring stack"
  exit 0
fi

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

prom_pod=$(PF_NAMESPACE="$MONITORING_NAMESPACE" resolve_pod prometheus) \
  || fail "no Running Prometheus pod in ${MONITORING_NAMESPACE}"
graf_pod=$(PF_NAMESPACE="$MONITORING_NAMESPACE" resolve_pod grafana) \
  || fail "no Running Grafana pod in ${MONITORING_NAMESPACE}"
echo "-> prometheus=${prom_pod} grafana=${graf_pod}"

# Prometheus has to have selected the exporter's ServiceMonitor and
# scraped it. Querying for a real sample covers both, and covers the
# node label surviving the scrape.
deadline=$(( $(date +%s) + SCRAPE_TIMEOUT_SECS ))
samples=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  code=$(PF_NAMESPACE="$MONITORING_NAMESPACE" port_forward_fetch \
           "$prom_pod" 9090 'api/v1/query?query=mxl_flow_active' \
           "$WORK_DIR/query.json" || true)
  if [ "$code" = "200" ]; then
    samples=$(tr ',' '\n' < "$WORK_DIR/query.json" | grep -c '"__name__":"mxl_flow_active"' || true)
    [ "$samples" -gt 0 ] && break
  fi
  sleep 5
done
[ "$samples" -gt 0 ] \
  || fail "prometheus returned no mxl_flow_active samples within ${SCRAPE_TIMEOUT_SECS}s; the exporter's ServiceMonitor is not being scraped"
echo "   prometheus has ${samples} mxl_flow_active series"

# A sample carrying no node label would mean the scrape dropped the
# label the whole board filters on.
grep -q '"node":"' "$WORK_DIR/query.json" \
  || fail "mxl_flow_active samples in prometheus carry no node label"
echo "   samples carry the node label"

# Grafana's sidecar has to have picked the dashboard ConfigMap up.
code=$(PF_NAMESPACE="$MONITORING_NAMESPACE" port_forward_fetch \
         "$graf_pod" 3000 "api/dashboards/uid/${DASHBOARD_UID}" \
         "$WORK_DIR/dashboard.json" -u "$GRAFANA_AUTH" || true)
[ "$code" = "200" ] \
  || fail "grafana has no dashboard with uid ${DASHBOARD_UID} (HTTP ${code}); the sidecar did not load the ConfigMap"
grep -q '"title":"MXL Flow Metrics"' "$WORK_DIR/dashboard.json" \
  || fail "dashboard ${DASHBOARD_UID} in grafana is not the MXL Flow Metrics board"
echo "   grafana serves dashboard ${DASHBOARD_UID}"

# The panels name a datasource by uid. If it resolves to nothing every
# panel renders an error rather than no data, which is the failure a
# chart consumer with a differently provisioned Grafana hits.
code=$(PF_NAMESPACE="$MONITORING_NAMESPACE" port_forward_fetch \
         "$graf_pod" 3000 "api/datasources/uid/${DATASOURCE_UID}" \
         "$WORK_DIR/datasource.json" -u "$GRAFANA_AUTH" || true)
[ "$code" = "200" ] \
  || fail "grafana has no datasource with uid ${DATASOURCE_UID} (HTTP ${code}); the board's panels would all error"
grep -q '"type":"prometheus"' "$WORK_DIR/datasource.json" \
  || fail "datasource ${DATASOURCE_UID} in grafana is not a prometheus datasource"
echo "   datasource ${DATASOURCE_UID} resolves to a prometheus datasource"
