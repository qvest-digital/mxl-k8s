//go:build !linux

package statfs

import "errors"

// Stats is unsupported off Linux; the agent only ships for Linux.
func Stats(path string) (int64, int64, error) {
	return 0, 0, errors.New("statfs is only available on Linux")
}
