package merge

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"github.com/qvest-digital/go-mxl/mxl"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
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
	now := time.Now()
	sel, head, wait := e.audio.Step(now)
	if wait > 0 { // member 0, the first, is waited for
		var more []PageSelect
		more, head, _ = e.audio.Step(now.Add(wait))
		sel = append(sel, more...)
	}
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

	refs := []mxlv1alpha1.RedundancyFlowRef{{FlowID: memberA}}
	mapped := inodes(domain, refs)[0]
	require.NoError(t, held[0].Close())
	require.NoError(t, os.RemoveAll(flowDir(domain, memberA)))
	w, _, err := inst.NewWriter(audioDef(memberA))
	require.NoError(t, err)
	held[0] = w
	require.NoError(t, mxlfs.AllocateFlow(flowDir(domain, memberA)))
	require.NotEqual(t, mapped, inodes(domain, refs)[0], "member A is still the mapped placeholder")

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

// A member that goes away for good, its producer gone and its directory
// with it, leaves the engine serving from the others: the reconciler
// keeps the engine rather than rebuild the output under every reader.
// Losing the primary must neither stall the output nor blank it.
func TestEngineServesOnWhenAMemberIsRemoved(t *testing.T) {
	domain := mxlfsDomain(t)
	inst, err := mxl.NewInstance(domain, "")
	require.NoError(t, err)
	defer inst.Close()
	rate := mxl.Rational{Num: 50, Den: 1}
	stop := []chan struct{}{make(chan struct{}), make(chan struct{})}
	done := []chan struct{}{make(chan struct{}), make(chan struct{})}
	for m, id := range []string{memberA, memberB} {
		w, _, err := inst.NewWriter(videoDef(id))
		require.NoError(t, err)
		go func() {
			defer close(done[m])
			defer w.Close()
			for i := mxl.CurrentIndex(rate); ; i++ {
				select {
				case <-stop[m]:
					return
				case <-time.After(time.Duration(mxl.NsUntilIndex(i, rate))):
				}
				g, err := w.OpenGrain(i)
				require.NoError(t, err)
				binary.LittleEndian.PutUint64(g.Payload, uint64(m)<<56|i)
				require.NoError(t, g.Commit(g.TotalSlices, 0))
			}
		}()
	}
	defer func() { close(stop[1]); <-done[1] }()
	time.Sleep(300 * time.Millisecond)

	lazily(t, domain)
	e, err := Start(context.Background(), Spec{DomainDir: domain, OutputID: output, Members: []string{memberA, memberB},
		Skew: 40 * time.Millisecond, Ordered: true}, logr.Discard())
	require.NoError(t, err)
	defer e.Close()
	r, err := inst.NewReader(output)
	require.NoError(t, err)
	defer r.Close()
	before := mxl.CurrentIndex(rate) + 2
	g, err := r.GetGrain(before, time.Second)
	require.NoError(t, err)
	require.Zero(t, binary.LittleEndian.Uint64(g.Payload)>>56, "the output is not served from the primary")

	close(stop[0])
	<-done[0]
	require.NoError(t, os.RemoveAll(flowDir(domain, memberA)))

	start := mxl.CurrentIndex(rate) + 2
	for i := start; i < start+150; i++ {
		g, err := r.GetGrain(i, time.Second)
		require.NoError(t, err, "grain %d", i)
		require.False(t, g.Invalid(), "grain %d is INVALID though member B had it", i)
		v := binary.LittleEndian.Uint64(g.Payload)
		require.Equal(t, i, v&(1<<56-1), "grain %d shows grain %d", i, v&(1<<56-1))
		require.Equal(t, uint64(1), v>>56, "grain %d not from member B", i)
	}
}

// A pass that moves the head on by more than one, after a tolerance or
// skew wait, selects the slot of index i while the published head is
// below i - 1. Readers still trust that slot as i - Grains, so a reader
// of the oldest grain got the next ring period's grain under the old
// index. The head must reach i - 1 before the slot moves. It runs
// without mxlfs: the order is the engine's.
func TestEnginePublishesTheHeadBeforeMovingASlotReadersTrust(t *testing.T) {
	const grains = 20
	path := filepath.Join(t.TempDir(), "data")
	require.NoError(t, os.WriteFile(path, make([]byte, 4096), 0o644))
	out, f, err := mapHeader(path, true)
	require.NoError(t, err)
	e := &Engine{out: out, outFile: f, views: make([]*os.File, grains)}
	t.Cleanup(e.release)
	var trusted []uint64 // per select, the oldest index readers then trusted
	var indexes []uint64
	orig := selectAllCall
	t.Cleanup(func() { selectAllCall = orig })

	a, b := newRing(grains), newRing(grains)
	for i := uint64(90); i <= 100; i++ {
		a.commit(i)
		b.commit(i)
	}
	d := NewDiscrete(grains, 2)
	d.Ordered, d.Skew = true, 8
	now := time.Unix(0, 0)
	_, head, _ := d.Step([]Member{a.member(), b.member()}, now)
	e.publish(head)

	// The primary is late with 101 to 104, which b has: the head waits
	// out the tolerance, then moves four on, every slot to b.
	for i := uint64(101); i <= 104; i++ {
		b.commit(i)
	}
	ms := []Member{a.member(), b.member()}
	sel, _, wait := d.Step(ms, now)
	require.Empty(t, sel)
	require.Positive(t, wait)
	sel, head, _ = d.Step(ms, now.Add(wait))
	require.Equal(t, uint64(104), head)
	require.Len(t, sel, 4)

	selectAllCall = func(_ *os.File, _ int) error {
		trusted = append(trusted, e.out.head()+2-grains)
		return nil
	}
	require.NoError(t, e.apply(sel))
	for k, s := range sel {
		indexes = append(indexes, s.Index)
		require.Greater(t, trusted[k], s.Index-grains,
			"the slot of %d moved while readers trusted it as %d", s.Index, s.Index-grains)
	}
	require.Equal(t, []uint64{101, 102, 103, 104}, indexes)
}

// A member writing at the clock writes index k into the slot of
// k - Grains at the boundary of k, whatever its head says after a loss.
// While the output lags, readers still trust that slot, so the engine
// must pass, and retire it, before that boundary: a pass at the boundary
// races the member, which may write first. It runs without mxlfs: the
// timing is the engine's.
func TestEngineRetiresASlotBeforeTheBoundaryAMemberWritesItAt(t *testing.T) {
	const grains = 20
	rate := mxl.Rational{Num: 50, Den: 1}
	guard := (&Engine{period: 20 * time.Millisecond}).retireGuard()
	a, b := newRing(grains), newRing(grains)
	d := NewDiscrete(grains, 2)
	d.Ordered, d.Skew = true, 8
	// Both write at the clock up to 119, then lose 120: the output stays
	// at 119, and readers trust 101 on.
	for i := uint64(100); i <= 119; i++ {
		a.commit(i)
		b.commit(i)
		ms := []Member{a.member(), b.member()}
		d.Step(ms, time.Unix(0, 0))
		due, _ := retireDue(mxl.IndexToTimestamp(rate, i)+uint64(time.Millisecond), rate, guard)
		require.Equal(t, i, due, "a member committing at the clock reads as late")
		d.Retire(ms, due) // the pass the commit wakes
	}
	require.Equal(t, uint64(119), d.Head)
	ms := []Member{a.member(), b.member()}

	// A pass late in grain 120, then the one it schedules.
	now := mxl.IndexToTimestamp(rate, 120) + uint64(12*time.Millisecond)
	due, next := retireDue(now, rate, guard)
	require.Equal(t, uint64(120), due)
	require.Empty(t, d.Retire(ms, due))
	now += uint64(next)
	require.Less(t, now, mxl.IndexToTimestamp(rate, 121), "the next pass is not before the boundary of 121")
	due, _ = retireDue(now, rate, guard)
	require.Equal(t, uint64(121), due)
	require.Equal(t, []Select{{Slot: 101 % grains, Source: d.Placeholder, Index: 101}}, d.Retire(ms, due),
		"the slot of 101, which both members overwrite with 121 at its boundary, did not move before it")
}

// futex_waitv returns at once when it fails, ENOSYS on a kernel without
// it; the engine loop would then spin a core. A wait that times out, is
// woken or finds a word already changed is the normal case and must not
// add a sleep.
func TestWaitSleepsWhenFutexWaitvFails(t *testing.T) {
	var slept []time.Duration
	var errno unix.Errno
	origCall, origSleep := futexWaitvCall, sleep
	t.Cleanup(func() { futexWaitvCall, sleep = origCall, origSleep })
	futexWaitvCall = func([]futexWaitv, *unix.Timespec) unix.Errno { return errno }
	sleep = func(d time.Duration) { slept = append(slept, d) }
	var logged int
	e := &Engine{log: funcr.New(func(string, string) { logged++ }, funcr.Options{})}
	waits := make([]futexWaitv, 1)

	for _, errno = range []unix.Errno{0, unix.ETIMEDOUT, unix.EAGAIN, unix.EINTR} {
		e.wait(waits, maxSleep)
	}
	require.Empty(t, slept)

	for _, errno = range []unix.Errno{unix.ENOSYS, unix.EINVAL, unix.EFAULT} {
		e.wait(waits, maxSleep)
	}
	require.Equal(t, []time.Duration{maxSleep, maxSleep, maxSleep}, slept)
	require.Equal(t, 1, logged, "the failure is logged once")
}

// The output's definition is its first member's with the output's id.
// The member's id may be written in upper case, and the same uuid may
// name a parent: a byte replace missed the one and rewrote the other.
func TestWithIDSetsOnlyTheFlowsID(t *testing.T) {
	const member = "0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d"
	def := []byte(`{"id":"` + strings.ToUpper(member) + `","label":"cam","parents":["` + member + `"],` +
		`"grain_rate":{"numerator":50,"denominator":1}}`)

	got, err := withID(def, output)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(got, &m))
	require.Equal(t, output, m["id"])
	require.Equal(t, []any{member}, m["parents"])
	require.Equal(t, map[string]any{"numerator": 50.0, "denominator": 1.0}, m["grain_rate"])

	_, err = withID([]byte("not json"), output)
	require.Error(t, err)
}

// A module that predates MXLFS_IOC_VIEW_REPLACE_SOURCE does not know the
// command, as no other file does: the engine then reports it cannot
// replace a member, and the reconciler builds the output again.
func TestEngineCannotReplaceWithoutTheIoctl(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "view"))
	require.NoError(t, err)
	defer f.Close()
	require.False(t, canReplace(f))
	_, err = (&Engine{}).Replace(0)
	require.ErrorIs(t, err, ErrNoReplace)
}

// producer writes a flow from a goroutine until stopped.
type producer struct {
	stop, done chan struct{}
	once       sync.Once
}

// Stop stops the producer and closes its writer, which removes the flow
// directory as a producer's exit does.
func (p *producer) Stop() {
	p.once.Do(func() { close(p.stop) })
	<-p.done
}

// produce writes flow def at the clock. Video grains carry tag and their
// index in the first word; audio samples carry a mark, tag and their
// index.
func produce(t *testing.T, inst *mxl.Instance, def string, tag uint64) *producer {
	t.Helper()
	w, _, err := inst.NewWriter(def)
	require.NoError(t, err)
	p := &producer{stop: make(chan struct{}), done: make(chan struct{})}
	stop := p.stop
	go func() {
		defer close(p.done)
		defer w.Close()
		rate := w.Config().Common.GrainRate
		if w.Config().Common.Format == mxl.FormatAudio {
			const batch = 480
			for end := (mxl.CurrentIndex(rate)/batch + 1) * batch; ; end += batch {
				select {
				case <-stop:
					return
				case <-time.After(time.Duration(mxl.NsUntilIndex(end, rate))):
				}
				s, err := w.OpenSamples(end, batch)
				require.NoError(t, err)
				for ch := uint64(0); ch < 2; ch++ {
					f1, f2, err := s.ChannelFragments(ch)
					require.NoError(t, err)
					idx := end - batch
					for _, f := range [][]byte{f1, f2} {
						for o := 0; o+4 <= len(f); o += 4 {
							binary.LittleEndian.PutUint32(f[o:], 0x80000000|uint32(tag)<<28|uint32(idx&0x0fffffff))
							idx++
						}
					}
				}
				require.NoError(t, s.Commit())
			}
		}
		for i := mxl.CurrentIndex(rate); ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(mxl.NsUntilIndex(i, rate))):
			}
			g, err := w.OpenGrain(i)
			require.NoError(t, err)
			binary.LittleEndian.PutUint64(g.Payload, tag<<56|i)
			require.NoError(t, g.Commit(g.TotalSlices, 0))
		}
	}()
	return p
}

// trustedWindow reads what the output offers behind its head, as a
// reader of the oldest grain or sample does. Member B wrote all of it, so
// every grain and sample must be there with its own index: an INVALID
// grain or silence is a slot or page left on the replaced member, where
// the new ring holds nothing yet. A grain or sample of producer tag 0,
// the old ring, cannot be there any more.
func trustedWindow(t *testing.T, e *Engine, r *mxl.Reader) {
	t.Helper()
	rt, err := r.Runtime()
	require.NoError(t, err)
	head := rt.HeadIndex
	if e.audio == nil {
		grains := uint64(e.disc.Grains)
		for i := head + 2 - grains; i <= head; i++ {
			g, err := r.GetGrain(i, 0)
			if errors.Is(err, mxl.ErrOutOfRangeLate) {
				continue // overwritten since the head was read
			}
			require.NoError(t, err, "grain %d", i)
			require.False(t, g.Invalid(), "grain %d behind head %d is INVALID though member B holds it", i, head)
			v := binary.LittleEndian.Uint64(g.Payload)
			require.Equal(t, i, v&(1<<56-1), "grain %d behind head %d shows grain %d of producer %d", i, head, v&(1<<56-1), v>>56)
			require.NotZero(t, v>>56, "grain %d behind head %d is the old ring's", i, head)
		}
		return
	}
	const batch = 480
	length := e.audio.Pages * PageSamples
	for end := (head-length/2)/batch*batch + 2*batch; end <= head; end += batch {
		v, err := r.GetSamples(end, batch, 0)
		if errors.Is(err, mxl.ErrOutOfRangeLate) {
			continue
		}
		require.NoError(t, err, "samples up to %d", end)
		for ch := uint64(0); ch < 2; ch++ {
			buf, err := v.CopyChannel(ch)
			require.NoError(t, err)
			for o := 0; o < len(buf); o += 4 {
				s := binary.LittleEndian.Uint32(buf[o:])
				idx := end - batch + uint64(o/4)
				require.NotZero(t, s, "sample %d of channel %d behind head %d is silence though member B holds it", idx, ch, head)
				require.Equal(t, uint32(idx&0x0fffffff), s&0x0fffffff,
					"sample %d of channel %d behind head %d carries %#x", idx, ch, head, s)
				require.NotZero(t, s>>28&7, "sample %d of channel %d behind head %d is the old ring's", idx, ch, head)
			}
		}
	}
}

func dataInode(t *testing.T, domain, id string) uint64 {
	t.Helper()
	var st unix.Stat_t
	require.NoError(t, unix.Stat(filepath.Join(flowDir(domain, id), "data"), &st))
	return st.Ino
}

// A producer that restarts recreates its flow directory, every file in
// it new. The engine puts the new ring in place of the old one in every
// view: readers of the output keep their flow, never told it went away,
// and once the other member stops too, everything they read comes from
// the restarted producer's new ring.
func TestEngineTakesARestartedMembersRingInPlace(t *testing.T) {
	for name, def := range map[string]func(string) string{"video": videoDef, "audio": audioDef} {
		t.Run(name, func(t *testing.T) {
			domain := mxlfsDomain(t)
			inst, err := mxl.NewInstance(domain, "")
			require.NoError(t, err)
			defer inst.Close()
			a, b := produce(t, inst, def(memberA), 0), produce(t, inst, def(memberB), 1)
			defer func() { a.Stop(); b.Stop() }()
			time.Sleep(300 * time.Millisecond)

			lazily(t, domain)
			e, err := Start(context.Background(), Spec{DomainDir: domain, OutputID: output, Members: []string{memberA, memberB},
				Skew: 40 * time.Millisecond, Ordered: true}, logr.Discard())
			require.NoError(t, err)
			defer e.Close()
			if !e.replaceable {
				if os.Getenv("MXLFS_REQUIRE_REPLACE") == "1" {
					t.Fatal("this mxlfs has no MXLFS_IOC_VIEW_REPLACE_SOURCE")
				}
				t.Skip("this mxlfs has no MXLFS_IOC_VIEW_REPLACE_SOURCE")
			}
			r, err := inst.NewReader(output)
			require.NoError(t, err)
			defer r.Close()
			out := dataInode(t, domain, output)
			// Long enough that all the output offers behind its head was
			// published from the members, none from before it started.
			time.Sleep(time.Second)

			a.Stop()
			_, err = os.Stat(flowDir(domain, memberA))
			require.True(t, os.IsNotExist(err), "the producer's exit left its flow directory")
			a = produce(t, inst, def(memberA), 2)
			ino, err := e.Replace(0)
			require.NoError(t, err)
			require.Equal(t, dataInode(t, domain, memberA), ino)
			// What readers trust behind the head was served by the old ring
			// until now; none of it may show what the new ring holds there.
			trustedWindow(t, e, r)

			b.Stop()

			if name == "audio" {
				const batch = 480
				rate := mxl.Rational{Num: 48000, Den: 1}
				first := (mxl.CurrentIndex(rate)/batch + 4) * batch
				for end := first; end < first+100*batch; end += batch {
					v, err := r.GetSamples(end, batch, time.Second)
					require.NoError(t, err, "samples up to %d", end)
					for ch := uint64(0); ch < 2; ch++ {
						buf, err := v.CopyChannel(ch)
						require.NoError(t, err)
						for o := 0; o < len(buf); o += 4 {
							s := binary.LittleEndian.Uint32(buf[o:])
							idx := end - batch + uint64(o/4)
							require.Equal(t, 0x80000000|uint32(2)<<28|uint32(idx&0x0fffffff), s,
								"sample %d of channel %d is not the restarted producer's", idx, ch)
						}
					}
				}
			} else {
				rate := mxl.Rational{Num: 50, Den: 1}
				start := mxl.CurrentIndex(rate) + 2
				for i := start; i < start+100; i++ {
					g, err := r.GetGrain(i, time.Second)
					require.NoError(t, err, "grain %d", i)
					require.False(t, g.Invalid(), "grain %d is INVALID though the restarted producer had it", i)
					require.Equal(t, uint64(2)<<56|i, binary.LittleEndian.Uint64(g.Payload),
						"grain %d is not the restarted producer's", i)
				}
			}
			require.Equal(t, out, dataInode(t, domain, output), "the output was built again")
		})
	}
}

// A ring's depth comes from the domain's history duration, not from the
// flow's definition, so a member restarted after the domain's history
// duration changed passes every check of its definition. Replace must refuse it as a ring of another
// layout, which the reconciler builds the output again for, not as one
// it could not open, which it tries again. It runs without mxlfs: the
// ring is refused before any view is touched.
func TestEngineRefusesARingOfAnotherDepth(t *testing.T) {
	ring := func(history int) string {
		domain := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(domain, "options.json"),
			[]byte(fmt.Sprintf(`{"urn:x-mxl:option:history_duration/v1.0": %d}`, history)), 0o644))
		inst, err := mxl.NewInstance(domain, "")
		require.NoError(t, err)
		t.Cleanup(func() { inst.Close() })
		w, _, err := inst.NewWriter(videoDef(memberA))
		require.NoError(t, err)
		t.Cleanup(func() { w.Close() })
		return domain
	}
	out, f, err := mapHeader(filepath.Join(flowDir(ring(400000000), memberA), "data"), false)
	require.NoError(t, err)
	e := &Engine{spec: Spec{DomainDir: ring(200000000), Members: []string{memberA}}, out: out, outFile: f,
		disc: &Discrete{}, replaceable: true}
	t.Cleanup(e.release)

	_, err = e.Replace(0)
	require.ErrorIs(t, err, ErrOtherLayout)
	require.NotErrorIs(t, err, ErrNotOpened)
	require.Contains(t, err.Error(), "grains")
}
