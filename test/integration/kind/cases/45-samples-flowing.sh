#!/usr/bin/env bash
# Assert audio samples are actually flowing across the fabric to the
# reader, not just that the MxlFlowMirror status says Ready. The reader
# pod runs go-mxl's read-samples example which prints one line per
# sample batch observed (e.g. "idx=480 ch=2 frags=[1920,1920]").
# Sample the batch index, wait, sample again, and require it to advance.
# This is the audio (sample-transfer) analogue of 40-frames-flowing.sh.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

READER_POD="${READER_POD:-mxl-tcp-demo-audio-reader}"
SAMPLE_WINDOW_SECS="${SAMPLES_WINDOW_SECS:-5}"

# The reader must be Running before the index can advance. read-samples
# runs in an until-loop, so the container stays up while the mirrored
# flow warms up; Ready means it is at least retrying.
"${KUBECTL[@]}" -n "$NAMESPACE" wait --for=condition=Ready \
    "pod/${READER_POD}" --timeout="${MIRROR_TIMEOUT_SECS}s" \
  || fail "${READER_POD} did not become Ready"

# Tail the reader's logs and extract the most recent batch index. Only
# the per-batch output lines carry "frags="; anchoring on it avoids the
# "fell behind (idx=... head=...)" resync log lines.
sample_idx() {
  "${KUBECTL[@]}" -n "$NAMESPACE" logs "pod/${READER_POD}" --tail=20 2>/dev/null \
    | awk '
        /frags=/ && match($0, /idx=[0-9]+/) {
          last = substr($0, RSTART + 4, RLENGTH - 4)
        }
        END { if (last != "") print last }
      '
}

# Allow a warm-up for the first batch to land: the gateway target must
# commit at least one arrived sample run before read-samples gets a head.
deadline=$(( $(date +%s) + 60 ))
first=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  first=$(sample_idx || true)
  [ -n "$first" ] && break
  sleep 1
done
[ -n "$first" ] || fail "${READER_POD} produced no idx= batch lines within 60s"

sleep "$SAMPLE_WINDOW_SECS"

last=$(sample_idx || true)
[ -n "$last" ] || fail "${READER_POD} stopped producing idx= batch lines mid-window"

if [ "$last" -le "$first" ]; then
  fail "reader sample index did not advance: first=${first} last=${last} window=${SAMPLE_WINDOW_SECS}s"
fi

echo "  idx advanced ${first} -> ${last} over ${SAMPLE_WINDOW_SECS}s ($(( last - first )) samples)"
