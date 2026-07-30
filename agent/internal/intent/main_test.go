package intent

import (
	"testing"

	"go.uber.org/goleak"
)

// The package spawns RunMirrorRescan, so a leaked ticker goroutine
// would otherwise go unnoticed.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
