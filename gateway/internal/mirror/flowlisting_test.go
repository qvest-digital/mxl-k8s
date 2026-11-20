package mirror

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/qvest-digital/go-mxl/mxl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
	"github.com/qvest-digital/mxl-k8s/gateway/internal/instance"
)

// On mxlfs a lookup by name of a flow directory that is not there waits
// for it to appear. These cover the lookups that come before the
// gateway knows the flow is there: each is made only once the domain's
// listing has returned the flow.

// writerLookups records the data files the writer counts look up.
func writerLookups(t *testing.T) *[]string {
	t.Helper()
	var looked []string
	foreign, all := foreignWriters, flowWriters
	foreignWriters = func(path string) (int, error) {
		looked = append(looked, path)
		return foreign(path)
	}
	flowWriters = func(path string) (int, error) {
		looked = append(looked, path)
		return all(path)
	}
	t.Cleanup(func() { foreignWriters, flowWriters = foreign, all })
	return &looked
}

// flowDirIn lays down flow geometryFlowID's directory in domain, built
// from def.
func flowDirIn(t *testing.T, domain, def string) string {
	t.Helper()
	dir := filepath.Join(domain, geometryFlowID+".mxl-flow")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "flow_def.json"), []byte(def), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data"), nil, 0o644))
	return dir
}

// Every target open asks about a flow that is usually not there yet.
func TestPrepareFlowDirLooksUpOnlyAListedFlow(t *testing.T) {
	domain := t.TempDir()
	looked := writerLookups(t)

	require.NoError(t, prepareFlowDir(domain, geometryFlowID, videoDefAt("320"), nil))
	require.Empty(t, *looked, "a flow the listing did not return was looked up by name")

	dir := flowDirIn(t, domain, videoDefAt("640"))
	require.NoError(t, prepareFlowDir(domain, geometryFlowID, videoDefAt("320"), nil))
	data := filepath.Join(dir, "data")
	assert.Equal(t, []string{data, data}, *looked)
	assert.NoDirExists(t, dir, "a directory of another geometry nothing holds was kept")
}

// Retiring a placeholder closes its writer, which deletes a flow nothing
// else holds, as closing the target's own writer before a clear does.
func TestClearFlowDirLooksUpOnlyAListedFlow(t *testing.T) {
	domain := t.TempDir()
	looked := writerLookups(t)
	dir := flowDirIn(t, domain, videoDefAt("320"))

	require.NoError(t, clearFlowDir(dir, func(domain, id string) {
		require.NoError(t, os.RemoveAll(filepath.Join(domain, id+".mxl-flow")))
	}, errOtherGeometry))
	require.Empty(t, *looked, "a flow the listing did not return was looked up by name")

	flowDirIn(t, domain, videoDefAt("320"))
	require.NoError(t, clearFlowDir(dir, nil, errOtherGeometry))
	assert.Equal(t, []string{filepath.Join(dir, "data")}, *looked)
	assert.NoDirExists(t, dir)
}

// shmHandles opens a domain on /dev/shm.
func shmHandles(t *testing.T) *instance.Handles {
	t.Helper()
	if _, err := os.Stat("/dev/shm"); err != nil {
		t.Skip("/dev/shm not present")
	}
	dir, err := os.MkdirTemp("/dev/shm", "flowlisting-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	h, err := instance.Open(dir, false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// libmxlLookups records the flows libmxl is asked about by name.
func libmxlLookups(t *testing.T) *[]string {
	t.Helper()
	var asked []string
	active, reader := isFlowActive, newReader
	isFlowActive = func(inst *mxl.Instance, id string) (bool, error) {
		asked = append(asked, id)
		return active(inst, id)
	}
	newReader = func(inst *mxl.Instance, id string) (*mxl.Reader, error) {
		asked = append(asked, id)
		return reader(inst, id)
	}
	t.Cleanup(func() { isFlowActive, newReader = active, reader })
	return &asked
}

const listingFlowID = "5d1a7c2e-3f4b-4e8a-9c6d-2b7e1f0a9d34" // sharedFlowVideoDef's

// The liveness check runs on every reconcile of a mirror whose source
// failed and on the flusher's ticks, and the reader opens on every
// source set-up; all of them on the source side's one worker, and with
// the libmxl instance's lock held while libmxl looks the flow up.
func TestSourceAsksLibmxlOnlyAboutAListedFlow(t *testing.T) {
	h := shmHandles(t)
	asked := libmxlLookups(t)
	r := &SourceReconciler{Domains: &instance.Set{Primary: h}}
	o := &libmxlOpener{Domains: r.Domains}
	key := sourceKey{flowID: listingFlowID}

	live, err := r.flowActive(mxlv1alpha1.FlowRef{ID: listingFlowID})
	require.ErrorIs(t, err, mxl.ErrFlowNotFound, "a flow that is not there reads as gone")
	require.False(t, live)
	_, err = o.open(key)
	require.ErrorIs(t, err, mxl.ErrFlowNotFound)
	_, err = o.attach(&sharedSource{key: key, handles: h}, "", &sourceEntry{})
	require.ErrorIs(t, err, mxl.ErrFlowNotFound)
	require.Empty(t, *asked, "a flow the listing did not return was looked up by name")

	w, _, err := h.MXL().NewWriter(sharedFlowVideoDef)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	live, err = r.flowActive(mxlv1alpha1.FlowRef{ID: listingFlowID})
	require.NoError(t, err)
	require.True(t, live)
	_, err = o.open(key)
	require.NoError(t, err)
	require.Equal(t, []string{listingFlowID, listingFlowID}, *asked)
}
