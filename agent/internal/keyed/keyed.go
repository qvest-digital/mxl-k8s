// Package keyed handles a stream of items with each key's items in
// order and apart from every other key's.
package keyed

import (
	"context"
	"sync"
)

// Run reads in and calls handle for each item. Items of one key are
// handled one at a time, in the order they were read. Items of
// different keys are handled concurrently, by at most limit handlers.
//
// An item of a key that is already being handled is queued without
// waiting. An item of any other key waits for a free handler while limit
// keys are being handled, and so does reading in: only limit slow keys
// at once hold the reader, never one.
//
// Run returns once in is closed and every queued item is handled, or
// once ctx ends and every running handler has returned. Items not yet
// handled when ctx ends are dropped. limit must be at least 1.
func Run[T any](ctx context.Context, in <-chan T, limit int, key func(T) string, handle func(T)) {
	if limit < 1 {
		panic("keyed: limit must be at least 1")
	}
	var (
		mu sync.Mutex
		// A key is present while a handler goroutine owns it.
		queued = map[string][]T{}
		slots  = make(chan struct{}, limit)
		wg     sync.WaitGroup
	)
	work := func(k string) {
		defer wg.Done()
		defer func() { <-slots }()
		for ctx.Err() == nil {
			mu.Lock()
			q := queued[k]
			if len(q) == 0 {
				delete(queued, k)
				mu.Unlock()
				return
			}
			queued[k] = q[1:]
			mu.Unlock()
			handle(q[0])
		}
	}
	defer wg.Wait()
	for {
		var item T
		var ok bool
		select {
		case <-ctx.Done():
			return
		case item, ok = <-in:
			if !ok {
				return
			}
		}
		k := key(item)
		mu.Lock()
		q, owned := queued[k]
		queued[k] = append(q, item)
		mu.Unlock()
		if owned {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case slots <- struct{}{}:
		}
		wg.Add(1)
		go work(k)
	}
}
