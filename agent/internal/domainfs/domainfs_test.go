package domainfs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

const id = "1ac254d9-a5eb-475f-a2b6-3d02a5cfbc82"

func spec() *mxlv1alpha1.MxlDomainSpec {
	return &mxlv1alpha1.MxlDomainSpec{ID: id, Directory: "domain", Label: "Red Studio"}
}

// BCP-007-03 requires all four keys, tags an object even when empty.
// A function reading the file advertises its id as mxl_domain_id, so
// the file is the domain's identity on the node.
func TestApplyWritesTheDefinitionTheSchemaRequires(t *testing.T) {
	root := t.TempDir()
	res, err := Apply(root, spec())
	require.NoError(t, err)
	assert.True(t, res.CreatedDir)
	assert.True(t, res.WroteDefinition)

	b, err := os.ReadFile(filepath.Join(root, "domain", "domain_def.json"))
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))
	assert.Equal(t, id, m["id"])
	assert.Equal(t, "Red Studio", m["label"])
	assert.Equal(t, "", m["description"])
	assert.Equal(t, map[string]any{}, m["tags"])

	info, err := os.Stat(filepath.Join(root, "domain"))
	require.NoError(t, err)
	assert.Equal(t, DirMode, info.Mode().Perm(), "every function creates flows in it")
}

// A resync that finds the file current writes nothing, so functions
// watching the directory see no churn every period.
func TestApplyIsIdempotent(t *testing.T) {
	root := t.TempDir()
	_, err := Apply(root, spec())
	require.NoError(t, err)
	res, err := Apply(root, spec())
	require.NoError(t, err)
	assert.False(t, res.WroteDefinition)
	assert.False(t, res.CreatedDir)
}

// A directory already carrying another id holds flows written under
// that identity; overwriting it would silently rename a domain a
// controller has already been told about.
func TestApplyRefusesAForeignDomain(t *testing.T) {
	root := t.TempDir()
	other := *spec()
	other.ID = "3310f209-9351-47c0-b9a2-14c59b6a4c23"
	_, err := Apply(root, &other)
	require.NoError(t, err)

	_, err = Apply(root, spec())
	require.ErrorIs(t, err, ErrForeignDomain)
	d, err := ReadDefinition(filepath.Join(root, "domain", "domain_def.json"))
	require.NoError(t, err)
	assert.Equal(t, other.ID, d.ID, "left as it was")
}

// options.json is libmxl's; the spec owns one key of it. Unset leaves
// the file alone, so a cluster that still writes it another way keeps
// its value.
func TestApplyOwnsOnlyTheHistoryOption(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "domain")
	require.NoError(t, os.Mkdir(dir, 0o755))
	optsPath := filepath.Join(dir, "options.json")
	require.NoError(t, os.WriteFile(optsPath, []byte(`{"urn:x-mxl:option:other/v1.0": 7}`), 0o644))

	res, err := Apply(root, spec())
	require.NoError(t, err)
	assert.False(t, res.WroteOptions)
	b, _ := os.ReadFile(optsPath)
	assert.JSONEq(t, `{"urn:x-mxl:option:other/v1.0": 7}`, string(b), "unset leaves it alone")

	s := spec()
	s.HistoryDuration = &metav1.Duration{Duration: 2 * time.Second}
	res, err = Apply(root, s)
	require.NoError(t, err)
	assert.True(t, res.WroteOptions)
	b, _ = os.ReadFile(optsPath)
	assert.JSONEq(t, `{"urn:x-mxl:option:other/v1.0": 7,
		"urn:x-mxl:option:history_duration/v1.0": 2000000000}`, string(b))

	res, err = Apply(root, s)
	require.NoError(t, err)
	assert.False(t, res.WroteOptions)
}
