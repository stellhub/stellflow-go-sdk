package transport

import (
	"context"
	"sync"
)

// Pool reuses TCP connections by endpoint.
type Pool struct {
	maxFrameLength int
	mu             sync.Mutex
	connections    map[string]*Connection
}

// NewPool creates a connection pool.
func NewPool(maxFrameLength int) *Pool {
	if maxFrameLength <= 0 {
		maxFrameLength = DefaultMaxFrameLength
	}
	return &Pool{maxFrameLength: maxFrameLength, connections: make(map[string]*Connection)}
}

// Get returns a connection for endpoint.
func (p *Pool) Get(ctx context.Context, endpoint Endpoint) (*Connection, error) {
	key := endpoint.Address()
	p.mu.Lock()
	conn := p.connections[key]
	p.mu.Unlock()
	if conn != nil {
		return conn, nil
	}
	created, err := Dial(ctx, endpoint, p.maxFrameLength)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if existing := p.connections[key]; existing != nil {
		_ = created.Close()
		return existing, nil
	}
	p.connections[key] = created
	return created, nil
}

// Close closes all cached connections.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var firstErr error
	for key, conn := range p.connections {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(p.connections, key)
	}
	return firstErr
}
