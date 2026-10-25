package merge

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/go-logr/logr"
	"github.com/qvest-digital/go-mxl/mxl"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/qvest-digital/mxl-k8s/gateway/internal/mxlfs"
)

// These run against a real mxlfs (MXLFS_TEST_DIR), where views exist;
// elsewhere they are skipped. Two members are written with staggered
// outages that never overlap, so every grain, and every sample, exists
// in one member, and a reader of the output must get all of it.

const (
	memberA = "11111111-1111-4111-8111-111111111111"
	memberB = "22222222-2222-4222-8222-222222222222"
	output  = "33333333-3333-4333-8333-333333333333"
)

func mxlfsDomain(t *testing.T) string {
	t.Helper()
	dir := os.Getenv("MXLFS_TEST_DIR")
	if dir == "" {
		t.Skip("MXLFS_TEST_DIR not set")
	}
	d, err := os.MkdirTemp(dir, "merge-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	require.NoError(t, os.WriteFile(filepath.Join(d, "options.json"),
		[]byte(`{"urn:x-mxl:option:history_duration/v1.0": 400000000}`), 0o644))
	return d
}

// lazily makes this process allocate lazily from here on, as the
// gateway does on mxlfs: what the engine creates must still be backed.
func lazily(t *testing.T, domain string) {
	t.Helper()
	require.NoError(t, mxlfs.SetLazy(domain, true))
	t.Cleanup(func() { _ = mxlfs.SetLazy(domain, false) })
}

func videoDef(id string) string {
	return `{"description":"merge test","id":"` + id + `","tags":{"urn:x-nmos:tag:grouphint/v1.0":["merge-test:Video"]},
"format":"urn:x-nmos:format:video","label":"merge test","parents":[],"media_type":"video/v210",
"grain_rate":{"numerator":50,"denominator":1},"frame_width":320,"frame_height":180,"interlace_mode":"progressive",
"colorspace":"BT709","components":[{"name":"Y","width":320,"height":180,"bit_depth":10},
{"name":"Cb","width":160,"height":180,"bit_depth":10},{"name":"Cr","width":160,"height":180,"bit_depth":10}]}`
}

func dataDef(id string) string {
	return `{"description":"merge test","id":"` + id + `","tags":{"urn:x-nmos:tag:grouphint/v1.0":["merge-test:Data"]},
"format":"urn:x-nmos:format:data","label":"merge test","parents":[],"media_type":"video/smpte291",
"grain_rate":{"numerator":50,"denominator":1}}`
}

func audioDef(id string) string {
	return `{"description":"merge test","id":"` + id + `","tags":{"urn:x-nmos:tag:grouphint/v1.0":["merge-test:Audio"]},
"format":"urn:x-nmos:format:audio","label":"merge test","parents":[],"media_type":"audio/float32",
"sample_rate":{"numerator":48000},"channel_count":2,"bit_depth":32}`
}

// outage reports whether member m skips grain or batch i: member A is out
// for 40 of every 100, member B for the other half of the period.
func outage(m int, i uint64) bool { return (i+uint64(m)*50)%100 < 40 }

func writeGrains(t *testing.T, inst *mxl.Instance, def string, member int, data bool, stop <-chan struct{}) {
	w, _, err := inst.NewWriter(def)
	require.NoError(t, err)
	defer w.Close()
	rate := w.Config().Common.GrainRate
	for i := mxl.CurrentIndex(rate); ; i++ {
		select {
		case <-stop:
			return
		case <-time.After(time.Duration(mxl.NsUntilIndex(i, rate))):
		}
		if outage(member, i) {
			continue
		}
		g, err := w.OpenGrain(i)
		require.NoError(t, err)
		binary.LittleEndian.PutUint64(g.Payload, uint64(member)<<56|i)
		valid := g.TotalSlices
		if data {
			valid = 64 // a data grain's valid slices are its length
		}
		require.NoError(t, g.Commit(valid, 0))
	}
}

func runDiscrete(t *testing.T, def func(string) string, data bool) {
	domain := mxlfsDomain(t)
	inst, err := mxl.NewInstance(domain, "")
	require.NoError(t, err)
	defer inst.Close()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for m, id := range []string{memberA, memberB} {
		wg.Add(1)
		go func() { defer wg.Done(); writeGrains(t, inst, def(id), m, data, stop) }()
	}
	defer func() { close(stop); wg.Wait() }()
	time.Sleep(300 * time.Millisecond)

	lazily(t, domain)
	e, err := Start(context.Background(), Spec{DomainDir: domain, OutputID: output, Members: []string{memberA, memberB},
		Skew: 40 * time.Millisecond, Ordered: true}, logr.Discard())
	require.NoError(t, err)
	defer e.Close()

	r, err := inst.NewReader(output)
	require.NoError(t, err)
	defer r.Close()
	rate := mxl.Rational{Num: 50, Den: 1}
	start := mxl.CurrentIndex(rate) + 2
	for i := start; i < start+250; i++ {
		var g mxl.Grain
		if data {
			g, err = r.GetGrainSlice(i, 0, time.Second)
		} else {
			g, err = r.GetGrain(i, time.Second)
		}
		require.NoError(t, err, "grain %d", i)
		require.False(t, g.Invalid(), "grain %d is INVALID though a member had it", i)
		v := binary.LittleEndian.Uint64(g.Payload)
		require.Equal(t, i, v&(1<<56-1), "grain %d shows grain %d", i, v&(1<<56-1))
		require.False(t, outage(int(v>>56), i), "grain %d shown from member %d, which skipped it", i, v>>56)
	}
}

func TestEngineMergesVideo(t *testing.T) { runDiscrete(t, videoDef, false) }

func TestEngineMergesANCData(t *testing.T) { runDiscrete(t, dataDef, true) }

func TestEngineMergesAudio(t *testing.T) {
	domain := mxlfsDomain(t)
	inst, err := mxl.NewInstance(domain, "")
	require.NoError(t, err)
	defer inst.Close()
	const batch = 480
	rate := mxl.Rational{Num: 48000, Den: 1}
	var e *Engine
	var mu sync.Mutex
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for m, id := range []string{memberA, memberB} {
		w, _, err := inst.NewWriter(audioDef(id))
		require.NoError(t, err)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer w.Close()
			for end := (mxl.CurrentIndex(rate)/batch + 1) * batch; ; end += batch {
				select {
				case <-stop:
					return
				case <-time.After(time.Duration(mxl.NsUntilIndex(end, rate))):
				}
				if outage(m, end/batch/10) { // outages of 10-batch blocks
					continue
				}
				s, err := w.OpenSamples(end, batch)
				require.NoError(t, err)
				for ch := uint64(0); ch < 2; ch++ {
					f1, f2, err := s.ChannelFragments(ch)
					require.NoError(t, err)
					idx := end - batch
					for _, f := range [][]byte{f1, f2} {
						for o := 0; o+4 <= len(f); o += 4 {
							binary.LittleEndian.PutUint32(f[o:], 0x80000000|uint32(m)<<28|uint32(idx&0x0fffffff))
							idx++
						}
					}
				}
				require.NoError(t, s.Commit())
				mu.Lock()
				if e != nil {
					e.Committed(m, end-batch, end)
				}
				mu.Unlock()
			}
		}()
	}
	defer func() { close(stop); wg.Wait() }()
	time.Sleep(300 * time.Millisecond)

	lazily(t, domain)
	mu.Lock()
	e, err = Start(context.Background(), Spec{DomainDir: domain, OutputID: output, Members: []string{memberA, memberB},
		Skew: 40 * time.Millisecond, Ordered: true}, logr.Discard())
	mu.Unlock()
	require.NoError(t, err)
	defer e.Close()

	r, err := inst.NewReader(output)
	require.NoError(t, err)
	defer r.Close()
	first := (mxl.CurrentIndex(rate)/batch + 4) * batch
	for end := first; end < first+400*batch; end += batch {
		v, err := r.GetSamples(end, batch, time.Second)
		require.NoError(t, err, "samples up to %d", end)
		for ch := uint64(0); ch < 2; ch++ {
			buf, err := v.CopyChannel(ch)
			require.NoError(t, err)
			for o := 0; o < len(buf); o += 4 {
				s := binary.LittleEndian.Uint32(buf[o:])
				idx := end - batch + uint64(o/4)
				require.NotZero(t, s, "sample %d of channel %d is silence", idx, ch)
				require.Equal(t, uint32(idx&0x0fffffff), s&0x0fffffff, fmt.Sprintf("sample %d", idx))
			}
		}
	}
}

// Every slot and page of an output starts on the INVALID grain or on
// silence, and a reader maps all of it. The gateway allocates lazily,
// so whatever backs those must be allocated outright, or touching an
// unwritten page faults SIGBUS in the reader.
func TestEngineBacksWhatNoMemberHolds(t *testing.T) {
	for _, tc := range []struct {
		def  func(string) string
		file string
	}{{videoDef, "grains/data.0"}, {audioDef, "channels"}} {
		domain := mxlfsDomain(t)
		inst, err := mxl.NewInstance(domain, "")
		require.NoError(t, err)
		defer inst.Close()
		for _, id := range []string{memberA, memberB} {
			w, _, err := inst.NewWriter(tc.def(id))
			require.NoError(t, err)
			defer w.Close()
		}
		lazily(t, domain)
		e, err := Start(context.Background(), Spec{DomainDir: domain, OutputID: output, Members: []string{memberA, memberB},
			Skew: 40 * time.Millisecond, Ordered: true}, logr.Discard())
		require.NoError(t, err)
		defer e.Close()

		f, err := os.Open(filepath.Join(flowDir(domain, output), tc.file))
		require.NoError(t, err)
		defer f.Close()
		st, err := f.Stat()
		require.NoError(t, err)
		mem, err := unix.Mmap(int(f.Fd()), 0, int(st.Size()), unix.PROT_READ, unix.MAP_SHARED)
		require.NoError(t, err)
		var sum int
		for o := 0; o < len(mem); o += 4096 {
			sum += int(mem[o])
		}
		_ = unix.Munmap(mem)
		t.Logf("%s: %d bytes mapped and read, first bytes of each page sum to %d", tc.file, st.Size(), sum)
	}
}

// The gateway reports an audio arrival by flow id, and the directory at
// that id may no longer hold the ring the engine mapped: a placeholder
// is retired for a ring of another geometry and created again, and a
// mirror fills the new one. The mapped ring then never got the samples
// and, held lazily, has no pages, so a page of the output selected to it
// faults SIGBUS in every reader. It runs without mxlfs: the decision is
// the engine's, from the mapped ring's own head.
func TestEngineCountsOnlyWhatTheMappedRingHolds(t *testing.T) {
	const pages = 4
	e := &Engine{audio: NewAudio(2, pages, 2)}
	for range 2 {
		path := filepath.Join(t.TempDir(), "data")
		require.NoError(t, os.WriteFile(path, make([]byte, 4096), 0o644))
		h, f, err := mapHeader(path, true)
		require.NoError(t, err)
		e.members, e.files = append(e.members, h), append(e.files, f)
	}
	t.Cleanup(e.release)
	setHead := func(m int, head uint64) {
		atomic.StoreUint64((*uint64)(unsafe.Pointer(&e.members[m].info.runtime.headIndex)), head)
	}

	// Member 0's mapped ring was replaced before anything was written to
	// it; member 1's mapped ring holds what it reports.
	const start, end = PageSamples, 3 * PageSamples
	setHead(1, end)
	e.Committed(0, start, end)
	e.Committed(1, start, end)
	sel, head, _ := e.audio.Step(time.Now())
	sel = append(sel, e.audio.Retire(end)...)
	for _, s := range sel {
		require.NotEqual(t, 0, s.Source, "page %d selected to a ring that holds none of it", s.Page)
	}
	require.Contains(t, sel, PageSelect{Page: 1, Source: 1})
	require.Equal(t, uint64(end), head)
}

// The same on mxlfs, through a reader: member A's lazily held
// placeholder is replaced by an allocated copy a mirror writes, while
// the engine still maps the placeholder. Reading what the output then
// publishes must not touch a page of the placeholder, which has none.
func TestEngineReaderNeverFaultsOnAReplacedMember(t *testing.T) {
	domain := mxlfsDomain(t)
	inst, err := mxl.NewInstance(domain, "")
	require.NoError(t, err)
	defer inst.Close()
	lazily(t, domain) // the gateway holds placeholders lazily
	var held []*mxl.Writer
	for _, id := range []string{memberA, memberB} {
		w, _, err := inst.NewWriter(audioDef(id))
		require.NoError(t, err)
		held = append(held, w)
	}
	defer func() {
		for _, w := range held {
			_ = w.Close()
		}
	}()
	e, err := Start(context.Background(), Spec{DomainDir: domain, OutputID: output, Members: []string{memberA, memberB},
		Skew: 40 * time.Millisecond, Ordered: true}, logr.Discard())
	require.NoError(t, err)
	defer e.Close()

	require.NoError(t, held[0].Close())
	require.NoError(t, os.RemoveAll(flowDir(domain, memberA)))
	w, _, err := inst.NewWriter(audioDef(memberA))
	require.NoError(t, err)
	held[0] = w
	require.NoError(t, mxlfs.AllocateFlow(flowDir(domain, memberA)))
	require.True(t, e.Stale(), "member A is still the mapped placeholder")

	const batch = 480
	rate := mxl.Rational{Num: 48000, Den: 1}
	first := (mxl.CurrentIndex(rate)/batch + 1) * batch
	for end := first; end < first+20*batch; end += batch {
		s, err := w.OpenSamples(end, batch)
		require.NoError(t, err)
		require.NoError(t, s.Commit())
		e.Committed(0, end-batch, end)
	}

	r, err := inst.NewReader(output)
	require.NoError(t, err)
	defer r.Close()
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	read := func(end uint64) (err error) {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("fault: %v", p)
			}
		}()
		v, err := r.GetSamples(end, batch, 50*time.Millisecond)
		if err != nil {
			return nil // nothing published there
		}
		for ch := uint64(0); ch < 2; ch++ {
			if _, err := v.CopyChannel(ch); err != nil {
				return err
			}
		}
		return nil
	}
	for end := first + 4*batch; end < first+20*batch; end += batch {
		require.NoError(t, read(end), "samples up to %d", end)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for p, src := range e.audio.Current {
		require.NotEqual(t, 0, src, "page %d selected to the replaced placeholder", p)
	}
}
