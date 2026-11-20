package main

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/qvest-digital/mxl-k8s/agent/internal/fanotify"
)

// blockingLocations holds every publish of the flow directory slow until
// release is closed and reports every other one on published.
type blockingLocations struct {
	slow      string
	release   chan struct{}
	published chan string
}

func (b blockingLocations) publish(ctx context.Context, dir string) error {
	if dir == b.slow {
		select {
		case <-b.release:
		case <-ctx.Done():
		}
		return nil
	}
	b.published <- dir
	return nil
}

func (b blockingLocations) PublishAppeared(ctx context.Context, dir string) error {
	return b.publish(ctx, dir)
}

func (b blockingLocations) PublishVanished(ctx context.Context, dir string) error {
	return b.publish(ctx, dir)
}

// A flow whose publish hangs on the API must not hold the events of the
// domain's other flows, in the flow directory watch and in the
// placeholder marker watch alike: handled one after another, a vanished
// flow waited for every one before it and a full channel overflowed the
// kernel queue behind them.
func TestASlowFlowDoesNotHoldAnothersEvents(t *testing.T) {
	for name, tc := range map[string]struct {
		run   func(ctx context.Context, in <-chan fanotify.Event, fp blockingLocations)
		mask  uint64
		a, b  string
		slow  string
		wantB string
	}{
		"flow directories": {
			run:   func(ctx context.Context, in <-chan fanotify.Event, fp blockingLocations) { runDispatcher(ctx, in, fp) },
			mask:  fanotify.MaskCreate,
			a:     "a.mxl-flow",
			b:     "b.mxl-flow",
			slow:  "a.mxl-flow",
			wantB: "b.mxl-flow",
		},
		"placeholder markers": {
			run: func(ctx context.Context, in <-chan fanotify.Event, fp blockingLocations) {
				runMarkers(ctx, in, fp, logr.Discard())
			},
			mask:  fanotify.MaskDelete,
			a:     "a",
			b:     "b",
			slow:  "a.mxl-flow",
			wantB: "b.mxl-flow",
		},
	} {
		t.Run(name, func(t *testing.T) {
			// First registered, so it runs after the loop's cleanup below.
			before := goleak.IgnoreCurrent()
			t.Cleanup(func() { goleak.VerifyNone(t, before) })
			ctx, cancel := context.WithCancel(context.Background())
			fp := blockingLocations{slow: tc.slow, release: make(chan struct{}), published: make(chan string, 1)}
			// Buffered, so a reader stuck in the first event's handler
			// leaves the second queued and the test fails, not hangs.
			in := make(chan fanotify.Event, 2)
			done := make(chan struct{})
			go func() { defer close(done); tc.run(ctx, in, fp) }()
			// Registered before any assertion, so a failing run still
			// ends its loop and leaves no goroutine behind.
			t.Cleanup(func() {
				cancel()
				close(fp.release)
				close(in)
				<-done
			})

			in <- fanotify.Event{Mask: tc.mask, Name: tc.a}
			in <- fanotify.Event{Mask: tc.mask, Name: tc.b}
			select {
			case got := <-fp.published:
				require.Equal(t, tc.wantB, got)
			case <-time.After(5 * time.Second):
				t.Fatal("the second flow's event waited for the first flow's")
			}
		})
	}
}
