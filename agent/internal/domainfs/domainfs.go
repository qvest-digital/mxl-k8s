// Package domainfs writes an MxlDomain onto the host: the directory,
// its domain_def.json and, where the spec asks for one, options.json.
package domainfs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	mxlv1alpha1 "github.com/qvest-digital/mxl-k8s/api/v1alpha1"
)

// DirMode is the domain directory's mode. Every function on the node
// creates flows in it under its own uid, and the directory has always
// been world-writable for that reason.
const DirMode fs.FileMode = 0o777

// FileMode is the mode of the files written here. Functions read them;
// nothing but the agent writes them.
const FileMode fs.FileMode = 0o644

// historyOption is libmxl's options.json key for the ring depth.
const historyOption = "urn:x-mxl:option:history_duration/v1.0"

// Definition is domain_def.json, in the shape BCP-007-03's MXL Domain
// definition schema requires: all four keys present, tags an object of
// string arrays.
type Definition struct {
	ID          string              `json:"id"`
	Label       string              `json:"label"`
	Description string              `json:"description"`
	Tags        map[string][]string `json:"tags"`
}

// DefinitionFor is the domain_def.json a spec describes.
func DefinitionFor(spec *mxlv1alpha1.MxlDomainSpec) Definition {
	tags := map[string][]string{}
	for k, v := range spec.Tags {
		tags[k] = append([]string{}, v...)
	}
	return Definition{ID: spec.ID, Label: spec.Label, Description: spec.Description, Tags: tags}
}

// ErrForeignDomain is returned when the directory carries a
// domain_def.json naming a different id and holds flows. It is not
// overwritten: those flows were written under that identity, and a
// function that read the old file has told a controller the old id.
var ErrForeignDomain = errors.New("directory holds another domain")

// Result says what Apply changed.
type Result struct {
	CreatedDir      bool
	WroteDefinition bool
	WroteOptions    bool
}

// Apply makes <root>/<spec.Directory> match the spec. Files are
// written only when their content differs, through a rename so a
// reader never sees half a file.
func Apply(root string, spec *mxlv1alpha1.MxlDomainSpec) (Result, error) {
	var res Result
	dir := filepath.Join(root, spec.Directory)

	if _, err := os.Stat(dir); errors.Is(err, fs.ErrNotExist) {
		if err := os.Mkdir(dir, DirMode); err != nil {
			return res, fmt.Errorf("create %s: %w", dir, err)
		}
		res.CreatedDir = true
	} else if err != nil {
		return res, fmt.Errorf("stat %s: %w", dir, err)
	}
	// Mkdir is subject to the umask; the mode is what functions need.
	if err := os.Chmod(dir, DirMode); err != nil {
		return res, fmt.Errorf("chmod %s: %w", dir, err)
	}

	defPath := filepath.Join(dir, mxlv1alpha1.DomainDefFile)
	if err := checkIdentity(dir, defPath, spec.ID); err != nil {
		return res, err
	}

	def, err := json.MarshalIndent(DefinitionFor(spec), "", "  ")
	if err != nil {
		return res, err
	}
	wrote, err := writeIfChanged(defPath, append(def, '\n'))
	if err != nil {
		return res, err
	}
	res.WroteDefinition = wrote

	if spec.HistoryDuration != nil {
		opts, err := mergeOptions(filepath.Join(dir, mxlv1alpha1.DomainOptionsFile),
			spec.HistoryDuration.Nanoseconds())
		if err != nil {
			return res, err
		}
		wrote, err := writeIfChanged(filepath.Join(dir, mxlv1alpha1.DomainOptionsFile), opts)
		if err != nil {
			return res, err
		}
		res.WroteOptions = wrote
	}
	return res, nil
}

// checkIdentity refuses to replace an identity flows were written
// under.
//
// Only a competing id is refused, and only while the directory holds a
// flow: an unreadable file, one naming no id, or one naming this id in
// another case carries no identity a function could have published,
// and an empty directory has no flow a controller could be pointing at
// under the old id. To take over a directory refused here, remove its
// flow directories (or domain_def.json itself, if the old identity is
// known to be unused) and the next sync writes the new one.
func checkIdentity(dir, defPath, want string) error {
	existing, err := ReadDefinition(defPath)
	if err != nil || existing.ID == "" || strings.EqualFold(existing.ID, want) {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("list %s: %w", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() && strings.HasSuffix(e.Name(), flowDirSuffix) {
			return fmt.Errorf("%w: %s names %s and holds flows; spec names %s",
				ErrForeignDomain, defPath, existing.ID, want)
		}
	}
	return nil
}

// flowDirSuffix marks a flow directory inside a domain.
const flowDirSuffix = ".mxl-flow"

// ReadDefinition parses a domain_def.json.
func ReadDefinition(path string) (Definition, error) {
	var d Definition
	b, err := os.ReadFile(path)
	if err != nil {
		return d, err
	}
	if err := json.Unmarshal(b, &d); err != nil {
		return d, fmt.Errorf("parse %s: %w", path, err)
	}
	return d, nil
}

// mergeOptions sets the history duration in options.json and keeps any
// other option somebody put there: the file is libmxl's, and this owns
// one key of it.
func mergeOptions(path string, historyNs int64) ([]byte, error) {
	opts := map[string]any{}
	if b, err := os.ReadFile(path); err == nil {
		if json.Unmarshal(b, &opts) != nil {
			// Unparseable: libmxl could not read it either, so replace it.
			opts = map[string]any{}
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	opts[historyOption] = historyNs
	b, err := json.MarshalIndent(opts, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

func writeIfChanged(path string, content []byte) (bool, error) {
	if cur, err := os.ReadFile(path); err == nil && bytes.Equal(cur, content) {
		// Content matches; fix only the mode, which a hand edit may have changed.
		return false, os.Chmod(path, FileMode)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return false, fmt.Errorf("write %s: %w", path, err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(content); err != nil {
		_ = tmp.Close()
		return false, fmt.Errorf("write %s: %w", path, err)
	}
	if err := tmp.Chmod(FileMode); err != nil {
		_ = tmp.Close()
		return false, fmt.Errorf("chmod %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return false, fmt.Errorf("write %s: %w", path, err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return false, fmt.Errorf("rename onto %s: %w", path, err)
	}
	return true, nil
}
