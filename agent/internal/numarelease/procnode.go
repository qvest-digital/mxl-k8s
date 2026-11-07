package numarelease

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// NodeRoot is the sysfs directory of the host's NUMA nodes.
const NodeRoot = "/sys/devices/system/node"

// topology maps each CPU to its NUMA node.
type topology map[int]int32

// readTopology reads the cpulist of every node under root.
func readTopology(root string) (topology, error) {
	dirs, err := filepath.Glob(filepath.Join(root, "node[0-9]*"))
	if err != nil {
		return nil, err
	}
	t := topology{}
	for _, d := range dirs {
		n, err := strconv.ParseInt(strings.TrimPrefix(filepath.Base(d), "node"), 10, 32)
		if err != nil {
			continue
		}
		b, err := os.ReadFile(filepath.Join(d, "cpulist"))
		if err != nil {
			return nil, err
		}
		cpus, err := parseList(string(b))
		if err != nil {
			return nil, fmt.Errorf("%s/cpulist: %w", d, err)
		}
		for _, c := range cpus {
			t[c] = int32(n)
		}
	}
	if len(t) == 0 {
		return nil, fmt.Errorf("no NUMA node lists a CPU under %s", root)
	}
	return t, nil
}

// procNode is the NUMA node process pid is confined to: every CPU any of
// its threads may run on is on that node, and the process may allocate
// from it. A process that may run on CPUs of several nodes has none.
func procNode(procRoot string, pid int, topo topology) (int32, bool) {
	dir := filepath.Join(procRoot, strconv.Itoa(pid))
	mems, ok := statusList(filepath.Join(dir, "status"), "Mems_allowed_list")
	if !ok {
		return 0, false
	}
	tasks, _ := filepath.Glob(filepath.Join(dir, "task", "*", "status"))
	if len(tasks) == 0 {
		tasks = []string{filepath.Join(dir, "status")}
	}
	node := int32(-1)
	for _, task := range tasks {
		cpus, ok := statusList(task, "Cpus_allowed_list")
		if !ok {
			// A thread that exited between the glob and the read.
			continue
		}
		for _, c := range cpus {
			n, known := topo[c]
			if !known || (node >= 0 && n != node) {
				return 0, false
			}
			node = n
		}
	}
	if node < 0 {
		return 0, false
	}
	for _, m := range mems {
		if int32(m) == node {
			return node, true
		}
	}
	return 0, false
}

// statusList reads the list field of a /proc status file.
func statusList(path, field string) ([]int, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	for _, line := range strings.Split(string(b), "\n") {
		k, v, ok := strings.Cut(line, ":")
		if !ok || k != field {
			continue
		}
		l, err := parseList(v)
		return l, err == nil && len(l) > 0
	}
	return nil, false
}

// parseList parses a kernel list such as "0-3,8,10-11".
func parseList(s string) ([]int, error) {
	var out []int
	for _, part := range strings.Split(strings.TrimSpace(s), ",") {
		if part == "" {
			continue
		}
		lo, hi, isRange := strings.Cut(part, "-")
		a, err := strconv.Atoi(lo)
		if err != nil {
			return nil, err
		}
		b := a
		if isRange {
			if b, err = strconv.Atoi(hi); err != nil {
				return nil, err
			}
		}
		if b < a {
			return nil, fmt.Errorf("range %q runs backwards", part)
		}
		for i := a; i <= b; i++ {
			out = append(out, i)
		}
	}
	return out, nil
}
