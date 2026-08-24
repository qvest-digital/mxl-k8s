package domaingc

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeScaffold builds a directory shaped like the one libmxl stages a flow
// in: a descriptor, an access file, a data file and some grain payload.
func makeScaffold(t *testing.T, domain, name string, payload int) string {
	t.Helper()
	dir := filepath.Join(domain, name)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "grains"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "flow_def.json"), []byte("{}"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "access"), nil, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, flowDataFileName), make([]byte, 16), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "grains", "0"), make([]byte, payload), 0o644))
	return dir
}

// age backdates everything in the scaffold so it is past a grace.
func age(t *testing.T, dir string, d time.Duration) {
	t.Helper()
	when := time.Now().Add(-d)
	require.NoError(t, filepath.Walk(dir, func(p string, _ os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		return os.Chtimes(p, when, when)
	}))
	require.NoError(t, os.Chtimes(dir, when, when))
}

// attach takes the shared lock libmxl holds for as long as it is attached to
// the flow, and returns a function that lets go of it.
func attach(t *testing.T, dir string) func() {
	t.Helper()
	fd, err := syscall.Open(filepath.Join(dir, flowDataFileName), syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
	require.NoError(t, err)
	require.NoError(t, syscall.Flock(fd, syscall.LOCK_SH|syscall.LOCK_NB))
	return func() { _ = syscall.Close(fd) }
}

func newScaffoldSweeper(domain string) *Sweeper {
	return &Sweeper{DomainPath: domain, Log: logr.Discard()}
}

func TestSweepScaffoldsReclaimsAnAbandonedOne(t *testing.T) {
	// A SIGKILLed producer does not unwind, so libmxl's own cleanup never
	// runs and the partially allocated ring stays in the shared domain.
	// Nothing else in the platform reclaims it: libmxl's collector walks
	// published flow directories and a scaffold was never renamed into one.
	domain := t.TempDir()
	dir := makeScaffold(t, domain, ".mxl-tmp-abandoned", 4096)
	age(t, dir, time.Hour)

	removed, reclaimed := newScaffoldSweeper(domain).sweepScaffolds(time.Minute)

	assert.Equal(t, 1, removed)
	assert.Greater(t, reclaimed, int64(4096),
		"the reported figure has to be what the node got back, grains included")
	assert.NoDirExists(t, dir)
}

func TestSweepScaffoldsLeavesOneAWriterIsStillBuilding(t *testing.T) {
	// This is the case that makes the sweep safe to run at all. libmxl
	// takes a shared lock on the data file before it allocates the ring
	// and holds it for as long as it is attached, so a scaffold under
	// construction is indistinguishable from an abandoned one by age
	// alone - allocating a 1.5 GiB ring stops the mtimes advancing while
	// the writer is very much alive.
	domain := t.TempDir()
	dir := makeScaffold(t, domain, ".mxl-tmp-building", 4096)
	age(t, dir, time.Hour)

	release := attach(t, dir)
	defer release()

	removed, _ := newScaffoldSweeper(domain).sweepScaffolds(time.Minute)

	assert.Zero(t, removed)
	assert.DirExists(t, dir, "a flow being built must survive the sweep")

	// Once the writer is gone the same scaffold is collectable, which is
	// what shows the lock and not the age decided both outcomes.
	release()
	removed, _ = newScaffoldSweeper(domain).sweepScaffolds(time.Minute)
	assert.Equal(t, 1, removed)
	assert.NoDirExists(t, dir)
}

func TestSweepScaffoldsHoldsBackAYoungOne(t *testing.T) {
	// The lock decides whether a scaffold is abandoned; the grace closes
	// the window between libmxl creating the data file and locking it, in
	// which the file exists and is unlocked and the writer is fine.
	domain := t.TempDir()
	dir := makeScaffold(t, domain, ".mxl-tmp-fresh", 128)

	removed, _ := newScaffoldSweeper(domain).sweepScaffolds(time.Hour)

	assert.Zero(t, removed)
	assert.DirExists(t, dir)
}

func TestSweepScaffoldsIgnoresEverythingElseInTheDomain(t *testing.T) {
	// The domain is shared. Removing a published flow, or the options the
	// node agent owns, costs every producer on the node.
	domain := t.TempDir()

	flow := filepath.Join(domain, "abc"+flowDirSuffix)
	require.NoError(t, os.MkdirAll(flow, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(flow, flowDataFileName), nil, 0o644))
	options := filepath.Join(domain, "options.json")
	require.NoError(t, os.WriteFile(options, []byte("{}"), 0o644))
	notOurs := filepath.Join(domain, "mxl-tmp-no-leading-dot")
	require.NoError(t, os.MkdirAll(notOurs, 0o755))

	age(t, flow, time.Hour)
	age(t, notOurs, time.Hour)
	when := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(options, when, when))

	removed, _ := newScaffoldSweeper(domain).sweepScaffolds(time.Minute)

	assert.Zero(t, removed)
	assert.DirExists(t, flow)
	assert.FileExists(t, options)
	assert.DirExists(t, notOurs)
}

func TestSweepScaffoldsReclaimsOneThatNeverGotADataFile(t *testing.T) {
	// A producer killed between mkdtemp and the first write leaves a
	// directory with nothing to test a lock on. It is still abandoned, and
	// past the grace there is nobody it could belong to.
	domain := t.TempDir()
	dir := filepath.Join(domain, ".mxl-tmp-empty")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	age(t, dir, time.Hour)

	removed, _ := newScaffoldSweeper(domain).sweepScaffolds(time.Minute)

	assert.Equal(t, 1, removed)
	assert.NoDirExists(t, dir)
}

func TestSweepScaffoldsSurvivesAnUnreadableDomain(t *testing.T) {
	assert.NotPanics(t, func() {
		removed, reclaimed := newScaffoldSweeper(filepath.Join(t.TempDir(), "absent")).sweepScaffolds(time.Minute)
		assert.Zero(t, removed)
		assert.Zero(t, reclaimed)
	})

	s := &Sweeper{Log: logr.Discard()}
	removed, _ := s.sweepScaffolds(time.Minute)
	assert.Zero(t, removed, "an unset domain path cannot be swept")
}

func TestScaffoldGraceResolution(t *testing.T) {
	// Zero is "unset", not "no grace": a Sweeper built without the field
	// has to get the default rather than the raciest possible setting.
	assert.Equal(t, DefaultScaffoldGrace, (&Sweeper{}).scaffoldGrace())
	assert.Equal(t, time.Minute, (&Sweeper{ScaffoldGrace: time.Minute}).scaffoldGrace())
	assert.Negative(t, (&Sweeper{ScaffoldGrace: -1}).scaffoldGrace(),
		"a negative grace turns scaffold reclamation off")
}

func TestSweepRunsScaffoldsEvenWhenTheCollectorFails(t *testing.T) {
	// The two reclaim different things for different reasons. A domain
	// libmxl cannot collect is exactly the situation in which a node is
	// short of tmpfs, so that is the worst moment to skip the scaffolds.
	domain := t.TempDir()
	dir := makeScaffold(t, domain, ".mxl-tmp-abandoned", 512)
	age(t, dir, time.Hour)

	s := &Sweeper{
		Collector:     &fakeCollector{err: assert.AnError},
		DomainPath:    domain,
		ScaffoldGrace: time.Minute,
		Log:           logr.Discard(),
	}
	s.sweep(t.Context())

	assert.NoDirExists(t, dir)
}
