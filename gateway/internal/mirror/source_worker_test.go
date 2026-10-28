package mirror

import (
	"context"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// worker stands in for the controller's workqueue and its one worker:
// keys are reconciled one at a time, in the order they arrive, and a
// result asking for a requeue brings the key back after its delay. The
// reconciler's redial events feed it as the channel source does.
type worker struct {
	work    chan types.NamespacedName
	results sync.Map // key -> last reconcile.Result
}

// startWorker wires f's reconciler to a worker and runs it until the
// test ends.
func (f *fanoutFixture) startWorker(t *testing.T) *worker {
	t.Helper()
	events := make(chan event.GenericEvent, 64)
	f.r.redialEvents = events
	w := &worker{work: make(chan types.NamespacedName, 64)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		type due struct {
			key types.NamespacedName
			at  time.Time
		}
		var pending []due
		tick := time.NewTicker(2 * time.Millisecond)
		defer tick.Stop()
		for {
			var key types.NamespacedName
			select {
			case <-ctx.Done():
				return
			case ev := <-events:
				key = types.NamespacedName{Namespace: ev.Object.GetNamespace(), Name: ev.Object.GetName()}
			case key = <-w.work:
			case now := <-tick.C:
				for i, d := range pending {
					if !now.Before(d.at) {
						key = d.key
						pending = append(pending[:i], pending[i+1:]...)
						break
					}
				}
				if key.Name == "" {
					continue
				}
			}
			res, err := f.r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			w.results.Store(key, res)
			switch {
			case err != nil || res.Requeue:
				pending = append(pending, due{key, time.Now().Add(5 * time.Millisecond)})
			case res.RequeueAfter > 0:
				pending = append(pending, due{key, time.Now().Add(res.RequeueAfter)})
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return w
}

// enqueue hands key to the worker, as a watch event on it would.
func (w *worker) enqueue(name string) {
	w.work <- types.NamespacedName{Namespace: "ns1", Name: name}
}
