// Package observed reports which MXL flows each pod on the node reads
// and writes, from the kernel's own records rather than from anything a
// media function says about itself.
//
// A process that opened a flow through libmxl maps the flow's files:
// /proc/<pid>/maps lists them with the device and inode they live on, so
// a mapping tells which flow, in which domain (the domain's device), and
// which pod (the process's cgroup). A writer also holds a shared flock on
// the flow's data file, which /proc/locks lists by device and inode;
// readers hold none.
package observed

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// Direction is how a pod uses a flow.
type Direction string

const (
	Input  Direction = "Input"
	Output Direction = "Output"
)

// Use is one flow a pod reads or writes.
type Use struct {
	Domain    string // the MxlDomain name; empty for the primary domain
	FlowID    string
	Direction Direction
}

// Scanner reads /proc.
type Scanner struct {
	// ProcRoot is /proc; tests point it elsewhere.
	ProcRoot string
	// Domains maps a domain directory's device ("maj:min", hex as in
	// /proc) to the domain's name.
	Domains func() map[string]string
}

// flowFile matches a file libmxl maps: the flow header, a grain, or the
// channels of a continuous flow.
var flowFile = regexp.MustCompile(`/([0-9a-fA-F-]{36})\.mxl-flow/(data|grains/data\.[0-9]+|channels)$`)

// podUID matches the pod UID in a kubepods cgroup path, in either the
// systemd ("pod<uid with underscores>.slice") or the cgroupfs ("pod<uid>")
// form.
var podUID = regexp.MustCompile(`pod([0-9a-f]{8}[-_][0-9a-f]{4}[-_][0-9a-f]{4}[-_][0-9a-f]{4}[-_][0-9a-f]{12})`)

// Scan returns, per pod UID, the flows the pod's processes use.
func (s *Scanner) Scan() (map[string][]Use, error) {
	root := s.ProcRoot
	if root == "" {
		root = "/proc"
	}
	domains := s.Domains()
	writers, err := s.lockHolders(filepath.Join(root, "locks"))
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	type key struct{ pod, domain, flow string }
	dirs := map[key]Direction{}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		pod := s.podOf(root, pid)
		if pod == "" {
			continue
		}
		for _, m := range s.flowMappings(root, pid, domains) {
			k := key{pod, m.domain, m.flow}
			d := Input
			if m.data && writers[pid][m.devIno] {
				d = Output
			}
			if dirs[k] != Output {
				dirs[k] = d
			}
		}
	}
	out := map[string][]Use{}
	for k, d := range dirs {
		out[k.pod] = append(out[k.pod], Use{Domain: k.domain, FlowID: k.flow, Direction: d})
	}
	for _, uses := range out {
		sort.Slice(uses, func(i, j int) bool {
			if uses[i].Domain != uses[j].Domain {
				return uses[i].Domain < uses[j].Domain
			}
			return uses[i].FlowID < uses[j].FlowID
		})
	}
	return out, nil
}

func (s *Scanner) podOf(root string, pid int) string {
	raw, err := os.ReadFile(filepath.Join(root, strconv.Itoa(pid), "cgroup"))
	if err != nil {
		return ""
	}
	m := podUID.FindStringSubmatch(string(raw))
	if m == nil {
		return ""
	}
	return strings.ReplaceAll(m[1], "_", "-")
}

type mapping struct {
	domain, flow string
	devIno       string // "maj:min:ino" as /proc/locks prints it
	data         bool
}

// flowMappings lists the flow files pid maps on a known domain device.
// A maps line reads "start-end perms offset maj:min inode path".
func (s *Scanner) flowMappings(root string, pid int, domains map[string]string) []mapping {
	f, err := os.Open(filepath.Join(root, strconv.Itoa(pid), "maps"))
	if err != nil {
		return nil
	}
	defer f.Close()
	var out []mapping
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 6 {
			continue
		}
		dev, ino, path := fields[3], fields[4], strings.Join(fields[5:], " ")
		domain, ok := domains[dev]
		if !ok {
			continue
		}
		m := flowFile.FindStringSubmatch(path)
		if m == nil {
			continue
		}
		out = append(out, mapping{
			domain: domain, flow: strings.ToLower(m[1]),
			devIno: dev + ":" + ino, data: m[2] == "data",
		})
	}
	return out
}

// lockHolders returns, per pid, the "maj:min:ino" files it holds a flock
// on. /proc/locks lines read "1: FLOCK  ADVISORY  READ 514830 08:01:11011342 0 EOF".
func (s *Scanner) lockHolders(path string) (map[int]map[string]bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	defer f.Close()
	out := map[int]map[string]bool{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 6 || fields[1] != "FLOCK" {
			continue
		}
		pid, err := strconv.Atoi(fields[4])
		if err != nil {
			continue
		}
		if out[pid] == nil {
			out[pid] = map[string]bool{}
		}
		out[pid][fields[5]] = true
	}
	return out, sc.Err()
}

// Device returns path's device as /proc prints it: "maj:min" in hex.
func Device(path string) (string, error) {
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		return "", err
	}
	return fmt.Sprintf("%02x:%02x", unix.Major(st.Dev), unix.Minor(st.Dev)), nil
}
