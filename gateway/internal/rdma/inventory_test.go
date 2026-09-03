package rdma

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeDevice lays out one RDMA device in a sysfs tree, with one port
// per state given. The port state file carries the numeric state and
// its name, as the kernel writes it.
func writeDevice(t *testing.T, root, name string, states ...string) {
	t.Helper()
	for i, state := range states {
		dir := filepath.Join(root, name, "ports", string(rune('1'+i)))
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "state"), []byte(state), 0o644))
	}
}

// A host with no RDMA hardware has no sysfs directory at all. That is
// an answer of none rather than a failure to read, because reporting
// it as an error would leave every such node permanently unable to
// say anything about its own hardware.
func TestAbsentDirectoryReportsNoDevices(t *testing.T) {
	devices, err := Inventory{Path: filepath.Join(t.TempDir(), "nothing-here")}.ActiveDevices()

	require.NoError(t, err)
	assert.Empty(t, devices)
}

func TestActiveDevicesAreReportedSorted(t *testing.T) {
	root := t.TempDir()
	writeDevice(t, root, "dev1", "4: ACTIVE")
	writeDevice(t, root, "dev0", "4: ACTIVE")

	devices, err := Inventory{Path: root}.ActiveDevices()

	require.NoError(t, err)
	assert.Equal(t, []string{"dev0", "dev1"}, devices)
}

// A device whose ports are all down carries no traffic, so a provider
// that reports nothing for it has measured correctly. Counting it
// would turn every idle port into a false discrepancy.
func TestDeviceWithoutAnActivePortIsNotReported(t *testing.T) {
	root := t.TempDir()
	writeDevice(t, root, "dev0", "1: DOWN")
	writeDevice(t, root, "dev1", "2: INIT", "3: ARMED")

	devices, err := Inventory{Path: root}.ActiveDevices()

	require.NoError(t, err)
	assert.Empty(t, devices)
}

// One active port is enough: a multi-port device is usable through
// whichever of its ports is up.
func TestDeviceWithOneActivePortIsReported(t *testing.T) {
	root := t.TempDir()
	writeDevice(t, root, "dev0", "1: DOWN", "4: ACTIVE")

	devices, err := Inventory{Path: root}.ActiveDevices()

	require.NoError(t, err)
	assert.Equal(t, []string{"dev0"}, devices)
}

// The numeric prefix belongs to the kernel's enum and carries no
// promise across versions, so the name after the colon is what is
// read.
func TestPortStateIsReadByNameNotNumber(t *testing.T) {
	root := t.TempDir()
	writeDevice(t, root, "dev0", "9: ACTIVE")

	devices, err := Inventory{Path: root}.ActiveDevices()

	require.NoError(t, err)
	assert.Equal(t, []string{"dev0"}, devices)
}

// An entry with no ports directory is not a device this can speak
// for, and sysfs carries entries that are not devices at all.
func TestEntryWithoutPortsIsSkipped(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "not-a-device"), 0o755))
	writeDevice(t, root, "dev0", "4: ACTIVE")

	devices, err := Inventory{Path: root}.ActiveDevices()

	require.NoError(t, err)
	assert.Equal(t, []string{"dev0"}, devices)
}

func TestMalformedStateIsNotActive(t *testing.T) {
	root := t.TempDir()
	writeDevice(t, root, "dev0", "ACTIVE")
	writeDevice(t, root, "dev1", "")

	devices, err := Inventory{Path: root}.ActiveDevices()

	require.NoError(t, err)
	assert.Empty(t, devices)
}

func TestDefaultPathIsUsedWhenPathIsEmpty(t *testing.T) {
	// Reading the real sysfs path must not fail on a host that has no
	// RDMA hardware, which is where the tests run.
	_, err := Inventory{}.ActiveDevices()

	require.NoError(t, err)
}
