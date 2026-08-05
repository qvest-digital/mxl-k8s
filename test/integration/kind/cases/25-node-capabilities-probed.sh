#!/usr/bin/env bash
# Assert every node's MxlNodeCapabilities comes from a real
# libmxl-fabrics enumeration, not from the gateway's --providers flag.
#
# KIND has no RDMA hardware, so tcp and shm are the only providers a
# node can report here. What it does exercise is the part that has
# nothing to do with hardware: libfabric enumerates loopback for the
# tcp provider on every host, and a peer handed 127.0.0.1 in a
# TargetInfo dials itself instead of the mirror source.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

# Scoped to the nodes running a gateway, not to every node: the
# publisher is the gateway, and the DaemonSet carries no toleration for
# the control-plane taint, so a control-plane node has no
# MxlNodeCapabilities to assert anything about.
nodes=$("${KUBECTL[@]}" -n "$NAMESPACE" get pods \
  -l app.kubernetes.io/name=mxl-k8s-gateway \
  --field-selector=status.phase=Running \
  -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u)
[ -n "$nodes" ] || fail "no Running gateway pod found; nothing publishes MxlNodeCapabilities"

failed=0
for node in $nodes; do
  echo "-> ${node}"

  if ! wait_phase "mxlnodecapabilities/${node}" \
        '{.status.conditions[?(@.type=="Probed")].status}' '^True$' 60 >/dev/null; then
    echo "   Probed condition never went True" >&2
    "${KUBECTL[@]}" get "mxlnodecapabilities/${node}" -o yaml >&2 || true
    failed=1
    continue
  fi
  echo "   Probed=True"

  # tcp needs no hardware, so a node reporting zero devices for it
  # means the enumeration or the fabric selection dropped everything.
  tcp_count=$("${KUBECTL[@]}" get "mxlnodecapabilities/${node}" \
    -o jsonpath='{.status.providers[?(@.name=="tcp")].deviceCount}' 2>/dev/null || true)
  if [ -z "$tcp_count" ] || [ "$tcp_count" -lt 1 ]; then
    echo "   tcp deviceCount='${tcp_count:-unset}', expected at least 1" >&2
    "${KUBECTL[@]}" get "mxlnodecapabilities/${node}" -o yaml >&2 || true
    failed=1
    continue
  fi
  echo "   tcp deviceCount=${tcp_count}"

  addresses=$("${KUBECTL[@]}" get "mxlnodecapabilities/${node}" \
    -o jsonpath='{range .status.providers[*].interfaces[*]}{.address}{"\n"}{end}' 2>/dev/null || true)
  [ -n "$addresses" ] || { echo "   no interfaces advertised" >&2; failed=1; continue; }

  for address in $addresses; do
    case "$address" in
      127.*|::1)
        echo "   advertised loopback address ${address}" >&2
        failed=1
        ;;
    esac
  done
  echo "   interfaces: $(echo "$addresses" | tr '\n' ' ')"
done

[ "$failed" -eq 0 ] || fail "node capabilities are not a usable probe on one or more nodes"
