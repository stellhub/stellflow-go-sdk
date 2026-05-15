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
	dialing        map[string]chan struct{}
}

// NewPool creates a connection pool.
func NewPool(maxFrameLength int) *Pool {
	if maxFrameLength <= 0 {
		maxFrameLength = DefaultMaxFrameLength
	}
	return &Pool{
		maxFrameLength: maxFrameLength,
		connections:    make(map[string]*Connection),
		dialing:        make(map[string]chan struct{}),
	}
}

// Get returns a connection for endpoint.
func (p *Pool) Get(ctx context.Context, endpoint Endpoint) (*Connection, error) {
	key := endpoint.Address()
	for {
		p.mu.Lock()
		if conn := p.connections[key]; conn != nil {
			if conn.IsClosed() {
				delete(p.connections, key)
			} else {
				p.mu.Unlock()
				return conn, nil
			}
		}
		wait, ok := p.dialing[key]
		if !ok {
			wait = make(chan struct{})
			p.dialing[key] = wait
			p.mu.Unlock()
			break
		}
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wait:
		}
	}

	created, err := Dial(ctx, endpoint, p.maxFrameLength)
	p.mu.Lock()
	wait := p.dialing[key]
	delete(p.dialing, key)
	close(wait)
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	if existing := p.connections[key]; existing != nil {
		p.mu.Unlock()
		_ = created.Close()
		return existing, nil
	}
	p.connections[key] = created
	p.mu.Unlock()
	return created, nil
}

// Invalidate removes a broken cached connection for endpoint.
func (p *Pool) Invalidate(endpoint Endpoint, conn *Connection) {
	key := endpoint.Address()
	p.mu.Lock()
	cached := p.connections[key]
	if cached != nil && (conn == nil || cached == conn) {
		delete(p.connections, key)
	}
	p.mu.Unlock()
	if cached != nil && (conn == nil || cached == conn) {
		_ = cached.Close()
	}
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
