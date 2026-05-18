//go:build linux

package fanotify

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// These tests drive parseOne / extractName / dispatch with hand-built
// byte buffers. The fanotify wire format does not change across
// kernel releases, but the parser easily off-by-ones the cursor when
// edited; the tests pin both happy-path decoding and the rejection
// paths a malformed event must take.

// buildEvent constructs one fanotify event ready for parseOne:
//
//	metadata (24 bytes) + info_dfid_name (header 4 + fsid 8 + fh 8 + 0
//	handle bytes + zero-terminated name).
//
// handleBytes is the length of the (variable-size) file_handle handle
// portion; the test passes 0 because the parser only needs to skip it.
func buildEvent(t *testing.T, mask uint64, pid int32, name string, handleBytes int) []byte {
	t.Helper()

	payload := make([]byte, 0)
	// fsid (8 bytes, opaque to the parser).
	payload = append(payload, make([]byte, fanFSIDSize)...)
	// file_handle header: handle_bytes (uint32) + handle_type (int32).
	hdr := make([]byte, fileHandleHdrSize)
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(handleBytes))
	binary.LittleEndian.PutUint32(hdr[4:8], 0) // type
	payload = append(payload, hdr...)
	// Handle content (skipped by the parser).
	payload = append(payload, make([]byte, handleBytes)...)
	// Null-terminated name.
	payload = append(payload, []byte(name)...)
	payload = append(payload, 0x00)

	// info record header: 1 byte type, 1 byte pad, 2 bytes total_len.
	info := make([]byte, infoHeaderSize)
	info[0] = byte(infoTypeDFIDName)
	infoLen := infoHeaderSize + len(payload)
	binary.LittleEndian.PutUint16(info[2:4], uint16(infoLen))
	info = append(info, payload...)

	// metadata: 24 bytes.
	//  0..3:   event_len (uint32 LE)
	//  4..7:   vers + reserved (don't care)
	//  8..15:  mask (uint64 LE)
	// 16..19:  fd (int32, not consumed)
	// 20..23:  pid (uint32 LE)
	meta := make([]byte, metaSize)
	eventLen := uint32(metaSize + len(info))
	binary.LittleEndian.PutUint32(meta[0:4], eventLen)
	binary.LittleEndian.PutUint64(meta[8:16], mask)
	binary.LittleEndian.PutUint32(meta[20:24], uint32(pid))

	return append(meta, info...)
}

func TestParseOne_HappyPath(t *testing.T) {
	const name = "11111111-2222-3333-4444-555555555555.mxl-flow"
	buf := buildEvent(t, unix.FAN_CREATE|unix.FAN_ONDIR, 12345, name, 0)

	ev, consumed, err := parseOne(buf)
	require.NoError(t, err)
	require.NotNil(t, ev)
	assert.Equal(t, len(buf), consumed)
	assert.Equal(t, uint64(unix.FAN_CREATE|unix.FAN_ONDIR), ev.Mask)
	assert.Equal(t, int32(12345), ev.PID)
	assert.Equal(t, name, ev.Name)
	assert.True(t, ev.IsCreate())
	assert.False(t, ev.IsRemove())
}

func TestParseOne_RemoveMaskIsClassified(t *testing.T) {
	buf := buildEvent(t, unix.FAN_DELETE|unix.FAN_ONDIR, 7, "x.mxl-flow", 0)
	ev, _, err := parseOne(buf)
	require.NoError(t, err)
	require.NotNil(t, ev)
	assert.True(t, ev.IsRemove())
	assert.False(t, ev.IsCreate(),
		"a delete event must not classify as a create; the agent uses "+
			"this discriminator to choose between PublishAppeared and "+
			"PublishVanished, and a misclassification would flip the "+
			"MxlFlow phase the wrong way")
}

func TestParseOne_HandleBytesSkipped(t *testing.T) {
	// Vary handle_bytes; the parser must skip over it without
	// reading the contents or mistaking them for the name.
	for _, hb := range []int{0, 4, 16, 64} {
		buf := buildEvent(t, unix.FAN_CREATE, 1, "a.mxl-flow", hb)
		ev, _, err := parseOne(buf)
		require.NoError(t, err)
		require.NotNil(t, ev)
		assert.Equalf(t, "a.mxl-flow", ev.Name,
			"handle_bytes=%d must not bleed into the name", hb)
	}
}

func TestParseOne_TruncatedMetadata_ReturnsError(t *testing.T) {
	buf := []byte{0, 1, 2} // way under metaSize
	_, _, err := parseOne(buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "truncated")
}

func TestParseOne_InvalidEventLen_ReturnsError(t *testing.T) {
	buf := buildEvent(t, unix.FAN_CREATE, 1, "a.mxl-flow", 0)
	// Overwrite the event_len with a value larger than the buffer.
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(buf))+100)
	_, _, err := parseOne(buf)
	require.Error(t, err)
}

func TestParseOne_EventWithoutName_IsSkippedQuietly(t *testing.T) {
	// Construct a metadata-only event (no info record). The parser
	// must return (nil, eventLen, nil) so dispatch skips it.
	meta := make([]byte, metaSize)
	binary.LittleEndian.PutUint32(meta[0:4], metaSize)
	binary.LittleEndian.PutUint64(meta[8:16], unix.FAN_CREATE)

	ev, consumed, err := parseOne(meta)
	require.NoError(t, err)
	assert.Nil(t, ev,
		"events without a DFID_NAME record are routine on older kernels "+
			"and on inode-mark events for non-name-bearing changes; the "+
			"parser must skip them, not fail the whole read")
	assert.Equal(t, metaSize, consumed)
}

func TestExtractName_TruncatedPayload(t *testing.T) {
	_, err := extractName([]byte{0, 1, 2})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too short")
}

func TestExtractName_HandleOverrunsPayload(t *testing.T) {
	payload := make([]byte, fanFSIDSize+fileHandleHdrSize)
	binary.LittleEndian.PutUint32(payload[fanFSIDSize:fanFSIDSize+4], 9999)
	_, err := extractName(payload)
	require.Error(t, err)
}

func TestDispatch_ForwardsMultipleEvents(t *testing.T) {
	// Concatenate two events; dispatch must emit both on the channel
	// and consume the entire buffer.
	a := buildEvent(t, unix.FAN_CREATE, 1, "a.mxl-flow", 0)
	b := buildEvent(t, unix.FAN_DELETE, 2, "b.mxl-flow", 0)
	buf := append(append([]byte{}, a...), b...)

	out := make(chan Event, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := dispatch(ctx, buf, out)
	require.NoError(t, err)
	close(out)

	got := make([]Event, 0, 2)
	for e := range out {
		got = append(got, e)
	}
	require.Len(t, got, 2)
	assert.Equal(t, "a.mxl-flow", got[0].Name)
	assert.Equal(t, "b.mxl-flow", got[1].Name)
	assert.True(t, got[1].IsRemove())
}

func TestDispatch_RespectsContextCancel(t *testing.T) {
	// One event waiting; channel buffered=0; ctx already cancelled.
	a := buildEvent(t, unix.FAN_CREATE, 1, "a.mxl-flow", 0)
	out := make(chan Event)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := dispatch(ctx, a, out)
	assert.ErrorIs(t, err, context.Canceled,
		"a fanotify dispatch must abandon a blocked channel send when "+
			"the agent is shutting down; otherwise the read loop never "+
			"unblocks on Close")
}

func TestEventMaskHelpers(t *testing.T) {
	assert.True(t, Event{Mask: MaskCreate}.IsCreate())
	assert.True(t, Event{Mask: MaskMovedTo}.IsCreate())
	assert.True(t, Event{Mask: MaskDelete}.IsRemove())
	assert.True(t, Event{Mask: MaskMovedFrom}.IsRemove())
	assert.False(t, Event{Mask: MaskOnDir}.IsCreate(),
		"FAN_ONDIR alone is not a creation; it is the directory-flavour bit")
	assert.False(t, Event{Mask: MaskOnDir}.IsRemove())
}
