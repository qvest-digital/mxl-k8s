package intent

import (
	"testing"

	"go.uber.org/goleak"
)

// Materialize polls a mirror's status on a ticker while it waits, so a
// path that returns without stopping it would leak a goroutine per
// intent request and go unnoticed.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
