package mxlfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeFiles(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for name, body := range files {
		p := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte(body), 0o644))
	}
}

func TestHostNICNode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		online  string // "" for no list
		devices map[string]string
		want    int32
		wantErr string
	}{
		{name: "no NUMA", want: 0},
		{name: "one node", online: "1", devices: map[string]string{"mlx5_0": "-1"}, want: 1},
		{name: "two nodes, devices agree", online: "0-1", devices: map[string]string{"mlx5_0": "1", "mlx5_1": "1"}, want: 1},
		{name: "two nodes, devices disagree", online: "0-1", devices: map[string]string{"mlx5_0": "0", "mlx5_1": "1"},
			wantErr: "NUMA nodes 0 and 1"},
		{name: "two nodes, device without node", online: "0,1", devices: map[string]string{"mlx5_0": "-1"},
			wantErr: "reports NUMA node -1"},
		{name: "two nodes, no device", online: "0-1", wantErr: "reports a NUMA node"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			files := map[string]string{}
			for dev, n := range tc.devices {
				files[filepath.Join("class", dev, "device", "numa_node")] = n + "\n"
			}
			// A software device has no PCI device and is passed over.
			files[filepath.Join("class", "rxe0", "ports")] = ""
			if tc.online != "" {
				files["online"] = tc.online + "\n"
			}
			writeFiles(t, root, files)
			got, err := HostNICNode(filepath.Join(root, "class"), filepath.Join(root, "online"))
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.EqualValues(t, NodeNone, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
