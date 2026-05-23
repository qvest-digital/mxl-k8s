#!/usr/bin/env bash
# Assert grains are actually flowing across the fabric to the reader,
# not just that the MxlFlowMirror status says Ready. The reader pod
# runs go-mxl's read-grain example which prints one line per grain
# observed (e.g. "idx=53318102095 size=5529600 ..."). Sample the
# grain index, wait, sample again, and require the index to advance.

set -euo pipefail
# shellcheck source=../lib.sh
. "$KIND_TEST_LIB"

READER_POD="${READER_POD:-mxl-tcp-demo-reader}"
SAMPLE_WINDOW_SECS="${FRAMES_WINDOW_SECS:-5}"

# The reader must be Running before the index can advance. Wait up
# to MIRROR_TIMEOUT_SECS, since the reader is the consumer of the
# mirror and starts producing log lines only after the LD_PRELOAD
# shim's intent wait completes.
"${KUBECTL[@]}" -n "$NAMESPACE" wait --for=condition=Ready \
    "pod/${READER_POD}" --timeout="${MIRROR_TIMEOUT_SECS}s" \
  || fail "${READER_POD} did not become Ready"

# Tail the reader's logs and extract the most recent grain index.
# read-grain prints lines like "idx=<digits> size=... ...".
sample_idx() {
  "${KUBECTL[@]}" -n "$NAMESPACE" logs "pod/${READER_POD}" --tail=20 2>/dev/null \
    | awk '
        match($0, /idx=[0-9]+/) {
          v = substr($0, RSTART + 4, RLENGTH - 4)
          last = v
        }
        END { if (last != "") print last }
      '
}

# Allow a brief warm-up for the first grain to land if we got here
# the instant the mirror flipped Ready.
deadline=$(( $(date +%s) + 30 ))
first=""
while [ "$(date +%s)" -lt "$deadline" ]; do
  first=$(sample_idx || true)
  [ -n "$first" ] && break
  sleep 1
done
[ -n "$first" ] || fail "${READER_POD} produced no idx= lines within 30s"

sleep "$SAMPLE_WINDOW_SECS"

last=$(sample_idx || true)
[ -n "$last" ] || fail "${READER_POD} stopped producing idx= lines mid-window"

if [ "$last" -le "$first" ]; then
  fail "reader grain index did not advance: first=${first} last=${last} window=${SAMPLE_WINDOW_SECS}s"
fi

echo "  idx advanced ${first} -> ${last} over ${SAMPLE_WINDOW_SECS}s ($(( last - first )) grains)"
