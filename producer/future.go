package producer

import (
	"context"
	"errors"
)

// Result is the outcome of an asynchronous send.
type Result struct {
	Metadata Metadata
	Err      error
}

// Future waits for an asynchronous send result.
type Future struct {
	done chan Result
}

func newFuture() *Future {
	return &Future{done: make(chan Result, 1)}
}

func (f *Future) complete(metadata Metadata, err error) {
	f.done <- Result{Metadata: metadata, Err: err}
}

// Await waits until the asynchronous send completes or ctx is canceled.
func (f *Future) Await(ctx context.Context) (Metadata, error) {
	if f == nil {
		return Metadata{}, errors.New("future must not be nil")
	}
	select {
	case result := <-f.done:
		return result.Metadata, result.Err
	case <-ctx.Done():
		return Metadata{}, ctx.Err()
	}
}

// Done exposes a read-only completion channel.
func (f *Future) Done() <-chan Result {
	return f.done
}
