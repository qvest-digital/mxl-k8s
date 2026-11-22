// Package uapitest checks Go mirrors of the mxlfs userspace interface
// against mxlfs_uapi.h, kernel/mxlfs_uapi.h of github.com/danielclasen/mxlfs
// at v0.1.0-rc.10, copied unchanged. It reads the subset of C the header
// uses: structs of fixed-width integers and arrays of them, #define of an
// integer or an _IO, _IOR, _IOW or _IOWR ioctl, anonymous enums, the
// include guard and #include. Anything else fails Parse, so a header
// that grows a construct this package does not read cannot pass it.
package uapitest

import (
	_ "embed"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

//go:embed mxlfs_uapi.h
var source string

// Field is one member of a struct as the C compiler lays it out.
type Field struct {
	Name   string
	Type   string // __u64, __u32, __s32 or char
	Len    int    // array length, 0 for a scalar
	Offset uintptr
	Size   uintptr // of the whole member
}

// Header is what mxlfs_uapi.h declares.
type Header struct {
	// Structs by name without the struct keyword, fields in order.
	Structs map[string][]Field
	// Sizes of the structs, padded to their alignment.
	Sizes map[string]uintptr
	// Consts holds every integer #define and enum constant.
	Consts map[string]int64
	// Ioctls holds the number of every ioctl #define, as Linux encodes
	// it on x86 and arm64.
	Ioctls map[string]uintptr
}

var (
	comment      = regexp.MustCompile(`(?s)/\*.*?\*/`)
	structRe     = regexp.MustCompile(`^struct\s+(\w+)\s*\{$`)
	fieldRe      = regexp.MustCompile(`^(__u64|__u32|__s32|char) (\w+)(?:\[(\w+)\])?;$`)
	enumRe       = regexp.MustCompile(`^enum\s*\{$`)
	defineRe     = regexp.MustCompile(`^#define\s+(\w+)(?:\s+(.*))?$`)
	includeRe    = regexp.MustCompile(`^#include\s+<[\w/.]+>$`)
	enumeratorRe = regexp.MustCompile(`^(\w+)(?:\s*=\s*(.+))?$`)
	ioctlRe      = regexp.MustCompile(`^_IO(R|W|WR)?\('x', (\d+)(?:, (struct \w+|__u32))?\)$`)
	shiftRe      = regexp.MustCompile(`^\((-?\w+) << (\w+)\)$`)
	typeSizes    = map[string]uintptr{"__u64": 8, "__u32": 4, "__s32": 4, "char": 1}
)

// Parse reads the copy of mxlfs_uapi.h this package carries.
func Parse() (*Header, error) {
	return parse(source)
}

func parse(src string) (*Header, error) {
	src = comment.ReplaceAllString(src, "")
	h := &Header{Structs: map[string][]Field{}, Sizes: map[string]uintptr{}, Consts: map[string]int64{}, Ioctls: map[string]uintptr{}}
	var ioctls [][2]string
	guard := ""
	lines := strings.Split(src, "\n")
	for i := 0; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		var block func(body string) error
		switch {
		case line == "", line == "#endif", includeRe.MatchString(line):
			continue
		case strings.HasPrefix(line, "#ifndef "):
			if guard != "" {
				return nil, fmt.Errorf("line %d: a second #ifndef", i+1)
			}
			guard = strings.TrimSpace(strings.TrimPrefix(line, "#ifndef "))
			continue
		case strings.HasPrefix(line, "#"):
			m := defineRe.FindStringSubmatch(line)
			switch {
			case m == nil:
				return nil, fmt.Errorf("line %d: unknown directive %q", i+1, line)
			case m[2] == "" && m[1] == guard:
			case m[2] == "":
				return nil, fmt.Errorf("line %d: #define %s has no value", i+1, m[1])
			case strings.HasPrefix(m[2], "_IO"):
				ioctls = append(ioctls, [2]string{m[1], m[2]})
			default:
				v, err := integer(m[2])
				if err != nil {
					return nil, fmt.Errorf("line %d: #define %s: %w", i+1, m[1], err)
				}
				h.Consts[m[1]] = v
			}
			continue
		case enumRe.MatchString(line):
			block = h.enum
		default:
			m := structRe.FindStringSubmatch(line)
			if m == nil {
				return nil, fmt.Errorf("line %d: unknown declaration %q", i+1, line)
			}
			name := m[1]
			block = func(body string) error { return h.layout(name, body) }
		}
		var body []string
		for i++; i < len(lines) && strings.TrimSpace(lines[i]) != "};"; i++ {
			body = append(body, lines[i])
		}
		if i == len(lines) {
			return nil, fmt.Errorf("%q has no closing };", line)
		}
		if err := block(strings.Join(body, "\n")); err != nil {
			return nil, err
		}
	}
	for _, d := range ioctls {
		m := ioctlRe.FindStringSubmatch(d[1])
		if m == nil {
			return nil, fmt.Errorf("#define %s: unknown ioctl form %q", d[0], d[1])
		}
		dir := map[string]uintptr{"": 0, "W": 1, "R": 2, "WR": 3}[m[1]]
		nr, _ := strconv.Atoi(m[2])
		var size uintptr
		switch arg := m[3]; {
		case arg == "":
		case strings.HasPrefix(arg, "struct "):
			s, ok := h.Sizes[strings.TrimPrefix(arg, "struct ")]
			if !ok {
				return nil, fmt.Errorf("#define %s: no %s", d[0], arg)
			}
			size = s
		default:
			size = typeSizes[arg]
		}
		h.Ioctls[d[0]] = dir<<30 | size<<16 | uintptr('x')<<8 | uintptr(nr)
	}
	return h, nil
}

// enum records the constants of an anonymous enum, numbered as C does.
func (h *Header) enum(body string) error {
	next := int64(0)
	for _, e := range strings.Split(body, ",") {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		m := enumeratorRe.FindStringSubmatch(e)
		if m == nil {
			return fmt.Errorf("enum: unknown enumerator %q", e)
		}
		if m[2] != "" {
			v, err := integer(m[2])
			if err != nil {
				return fmt.Errorf("enum %s: %w", m[1], err)
			}
			next = v
		}
		h.Consts[m[1]] = next
		next++
	}
	return nil
}

// layout lays out struct name with natural alignment and no packing.
func (h *Header) layout(name, body string) error {
	var off, align uintptr = 0, 1
	var fields []Field
	for _, line := range strings.Split(body, "\n") {
		line = strings.Join(strings.Fields(line), " ")
		if line == "" {
			continue
		}
		m := fieldRe.FindStringSubmatch(line)
		if m == nil {
			return fmt.Errorf("struct %s: unknown member %q", name, line)
		}
		f := Field{Name: m[2], Type: m[1]}
		elem := typeSizes[f.Type]
		f.Size = elem
		if m[3] != "" {
			n, err := integer(m[3])
			if err != nil {
				v, ok := h.Consts[m[3]]
				if !ok {
					return fmt.Errorf("struct %s: array length %q", name, m[3])
				}
				n = v
			}
			f.Len = int(n)
			f.Size = elem * uintptr(n)
		}
		align = max(align, elem)
		off = (off + elem - 1) / elem * elem
		f.Offset = off
		off += f.Size
		fields = append(fields, f)
	}
	h.Structs[name] = fields
	h.Sizes[name] = (off + align - 1) / align * align
	return nil
}

// integer evaluates the integer expressions the header uses: a decimal or
// hex literal with U and L suffixes, negated or shifted, in parentheses.
func integer(s string) (int64, error) {
	if m := shiftRe.FindStringSubmatch(s); m != nil {
		a, err := integer(m[1])
		if err != nil {
			return 0, err
		}
		b, err := integer(m[2])
		if err != nil {
			return 0, err
		}
		return a << b, nil
	}
	s = strings.TrimSuffix(strings.TrimPrefix(s, "("), ")")
	s = strings.TrimRight(s, "UL")
	return strconv.ParseInt(s, 0, 64)
}

// CheckStruct reports how the Go type typ differs from struct name: its
// size, and each field's offset, size and integer kind. Names are not
// compared.
func (h *Header) CheckStruct(name string, typ reflect.Type) error {
	fields, ok := h.Structs[name]
	if !ok {
		return fmt.Errorf("the header has no struct %s", name)
	}
	var errs []string
	if typ.Size() != h.Sizes[name] {
		errs = append(errs, fmt.Sprintf("size %d, header %d", typ.Size(), h.Sizes[name]))
	}
	if typ.NumField() != len(fields) {
		errs = append(errs, fmt.Sprintf("%d fields, header %d", typ.NumField(), len(fields)))
	}
	for i := range min(typ.NumField(), len(fields)) {
		g, c := typ.Field(i), fields[i]
		if g.Offset != c.Offset || g.Type.Size() != c.Size || !sameKind(g.Type, c) {
			errs = append(errs, fmt.Sprintf("field %d %s %s at %d (%d bytes), header %s %s at %d (%d bytes)",
				i, g.Name, g.Type, g.Offset, g.Type.Size(), c.Type, c.Name, c.Offset, c.Size))
		}
	}
	if errs != nil {
		return fmt.Errorf("%s does not match struct %s: %s", typ, name, strings.Join(errs, "; "))
	}
	return nil
}

func sameKind(t reflect.Type, f Field) bool {
	if f.Len > 0 {
		if t.Kind() != reflect.Array || t.Len() != f.Len {
			return false
		}
		t = t.Elem()
	}
	want := map[string][]reflect.Kind{
		"__u64": {reflect.Uint64},
		"__u32": {reflect.Uint32},
		"__s32": {reflect.Int32},
		"char":  {reflect.Uint8, reflect.Int8},
	}[f.Type]
	for _, k := range want {
		if t.Kind() == k {
			return true
		}
	}
	return false
}
