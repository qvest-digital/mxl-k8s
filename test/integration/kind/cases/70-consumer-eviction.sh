#!/usr/bin/env bash
# Assert the operator garbage-collects the demo MxlFlowMirror when
# the consumer pod is removed. The demo reader is a bare Pod;
# deleting it leaves the MxlReceiver pointing at a podRef that no
# longer resolves, which the operator's pod watch must translate
# into a mirror GC. The reader is re-applied at the end so the
# rest of the suite (notably 80-origin-lease-expiry) still has a
# materialized mirror to inspect.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

READER_POD="${READER_POD:-mxl-tcp-demo-reader}"
READER_MANIFEST="${READER_MANIFEST:-${PWD}/examples/tcp-demo/21-reader.yaml}"
GC_TIMEOUT_SECS="${GC_TIMEOUT_SECS:-30}"

# The deleted reader consumes the video flow, so only that flow's
# mirror must be GC'd. The audio reader is untouched, so the audio
# flow's mirror must persist -- GC is per-consumer, not global.
VIDEO_FLOW_ID="${VIDEO_FLOW_ID:-5fbec3b1-1b0f-417d-9059-8b94a47197ed}"
AUDIO_FLOW_ID="${AUDIO_FLOW_ID:-b3bb5be7-9fe9-4324-a5bb-4c70e1084449}"

mirror_names() {
  "${KUBECTL[@]}" -n "$NAMESPACE" get mxlfm \
      -o jsonpath='{range .items[*]}{.metadata.name} {end}' 2>/dev/null || true
}

# Establish baseline: the video consumer's mirror exists so the
# assertion is not trivially satisfied.
pre=$(mirror_names)
echo "  pre-deletion mirrors: ${pre}"
case "$pre" in
  *"${VIDEO_FLOW_ID}--"*) : ;;
  *) fail "no video MxlFlowMirror (${VIDEO_FLOW_ID}) in ${NAMESPACE} before deletion" ;;
esac

"${KUBECTL[@]}" -n "$NAMESPACE" delete "pod/${READER_POD}" \
    --grace-period=0 --force --ignore-not-found \
  || fail "delete pod/${READER_POD} failed"

# Wait for the pod to actually be gone; the operator's resolver
# returns nil targets only on a NotFound, not on Terminating.
"${KUBECTL[@]}" -n "$NAMESPACE" wait --for=delete \
    "pod/${READER_POD}" --timeout=30s >/dev/null 2>&1 || true

deadline=$(( $(date +%s) + GC_TIMEOUT_SECS ))
video_gone=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  case "$(mirror_names)" in
    *"${VIDEO_FLOW_ID}--"*) ;;        # video mirror still present
    *) video_gone=1; break ;;
  esac
  sleep 2
done
[ "$video_gone" -eq 1 ] \
  || fail "video MxlFlowMirror (${VIDEO_FLOW_ID}) not GC'd within ${GC_TIMEOUT_SECS}s after reader deletion"
echo "  video mirror GC'd within budget"

# The audio consumer was not touched, so its mirror must survive the
# video reader's deletion: GC keys on the evicted consumer, not on the
# presence of any consumer.
case "$(mirror_names)" in
  *"${AUDIO_FLOW_ID}--"*) echo "  audio mirror retained (per-consumer GC)" ;;
  *) fail "audio MxlFlowMirror (${AUDIO_FLOW_ID}) GC'd by deleting the video reader; GC must be per-consumer" ;;
esac

# Restore the reader so subsequent cases see a converged demo. A
# per-pod apply (not the all-in-one tcp-demo kustomization) leaves
# the rest of the cluster untouched and avoids spawning legacy
# DaemonSets that the kustomize-only manifest set would.
"${KUBECTL[@]}" -n "$NAMESPACE" apply -f "$READER_MANIFEST" >/dev/null \
  || fail "re-apply ${READER_MANIFEST} failed"
"${KUBECTL[@]}" -n "$NAMESPACE" wait --for=condition=Ready \
    "pod/${READER_POD}" --timeout="${ROLLOUT_TIMEOUT_SECS}s" \
  || fail "${READER_POD} did not return to Ready after restore"

# Wait until the operator re-creates the mirror so cleanup is
# observable, not just declarative.
JSONPATH='{range .items[*]}{.metadata.name}={.status.phase};{end}'
phase=$(wait_phase mxlfm "$JSONPATH" \
        '^([a-z0-9-]+=Ready;)+$' \
        "$MIRROR_TIMEOUT_SECS") \
  || fail "mirror did not return to Ready after reader restore"
echo "  post-restore phases: ${phase}"
