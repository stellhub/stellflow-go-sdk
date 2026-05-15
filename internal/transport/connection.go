package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/stellhub/stellflow-go-sdk/protocol"
)

// Request carries an encoded protocol body and request metadata.
type Request struct {
	Header protocol.RequestHeader
	Body   []byte
}

// Response carries one decoded response frame.
type Response struct {
	Header protocol.ResponseHeader
	Body   []byte
}

type pendingResponse struct {
	apiKey     protocol.ApiKey
	apiVersion int16
	ch         chan responseResult
}

type responseResult struct {
	response Response
	err      error
}

// Connection is a single TCP connection with in-flight correlation.
type Connection struct {
	conn           net.Conn
	maxFrameLength int
	writeMu        sync.Mutex
	pendingMu      sync.Mutex
	pending        map[int32]pendingResponse
	closeOnce      sync.Once
	closed         chan struct{}
}

// Dial opens a TCP connection to endpoint.
func Dial(ctx context.Context, endpoint Endpoint, maxFrameLength int) (*Connection, error) {
	if maxFrameLength <= 0 {
		maxFrameLength = DefaultMaxFrameLength
	}
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", endpoint.Address())
	if err != nil {
		return nil, err
	}
	c := &Connection{
		conn:           conn,
		maxFrameLength: maxFrameLength,
		pending:        make(map[int32]pendingResponse),
		closed:         make(chan struct{}),
	}
	go c.readLoop()
	return c, nil
}

// Send writes one request and waits for the correlated response.
func (c *Connection) Send(ctx context.Context, request Request) (Response, error) {
	pending := pendingResponse{
		apiKey:     request.Header.APIKey,
		apiVersion: request.Header.APIVersion,
		ch:         make(chan responseResult, 1),
	}
	c.pendingMu.Lock()
	select {
	case <-c.closed:
		c.pendingMu.Unlock()
		return Response{}, errors.New("connection is closed")
	default:
	}
	if _, exists := c.pending[request.Header.CorrelationID]; exists {
		c.pendingMu.Unlock()
		return Response{}, fmt.Errorf("duplicate correlation id: %d", request.Header.CorrelationID)
	}
	c.pending[request.Header.CorrelationID] = pending
	c.pendingMu.Unlock()

	frame, err := EncodeRequestFrame(request.Header, request.Body)
	if err != nil {
		c.removePending(request.Header.CorrelationID)
		return Response{}, err
	}
	c.writeMu.Lock()
	_, err = c.conn.Write(frame)
	c.writeMu.Unlock()
	if err != nil {
		c.removePending(request.Header.CorrelationID)
		return Response{}, err
	}

	select {
	case result := <-pending.ch:
		return result.response, result.err
	case <-ctx.Done():
		c.removePending(request.Header.CorrelationID)
		return Response{}, ctx.Err()
	case <-c.closed:
		return Response{}, errors.New("connection is closed")
	}
}

// Close closes the connection and all pending requests.
func (c *Connection) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.closed)
		err = c.conn.Close()
		c.failPending(errors.New("connection is closed"))
	})
	return err
}

func (c *Connection) readLoop() {
	for {
		frame, err := ReadFrame(c.conn, c.maxFrameLength)
		if err != nil {
			c.Close()
			c.failPending(err)
			return
		}
		header, body, err := DecodeResponseFrame(frame)
		if err != nil {
			c.Close()
			c.failPending(err)
			return
		}
		pending, ok := c.removePending(header.CorrelationID)
		if !ok {
			continue
		}
		_ = pending.apiKey
		_ = pending.apiVersion
		pending.ch <- responseResult{response: Response{Header: header, Body: body}}
	}
}

func (c *Connection) removePending(correlationID int32) (pendingResponse, bool) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	pending, ok := c.pending[correlationID]
	if ok {
		delete(c.pending, correlationID)
	}
	return pending, ok
}

func (c *Connection) failPending(err error) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	for correlationID, pending := range c.pending {
		delete(c.pending, correlationID)
		pending.ch <- responseResult{err: err}
	}
}
