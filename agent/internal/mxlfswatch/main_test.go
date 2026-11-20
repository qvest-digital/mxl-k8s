package mxlfswatch

import (
	"testing"

	"go.uber.org/goleak"
)

// Wanted.Run hands wants to handler goroutines and Trigger.Handle starts
// one per request, so a path that returns without waiting for them would
// leak them.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
