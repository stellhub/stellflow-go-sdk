package producer

import (
	"context"
	"errors"

	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

// TransactionMetadata describes producer identity returned by transaction APIs.
type TransactionMetadata struct {
	ProducerID       int64
	ProducerEpoch    int16
	TransactionState *string
}

// InitProducerID initializes the producer identity used by idempotent and transactional sends.
func (c *Client) InitProducerID(ctx context.Context) (TransactionMetadata, error) {
	if c.isClosed() {
		return TransactionMetadata{}, ErrClosed
	}
	metadata, err := c.initProducerID(ctx)
	if err != nil {
		return TransactionMetadata{}, err
	}
	return metadata, nil
}

// BeginTransaction starts a transaction for the configured transactional id.
func (c *Client) BeginTransaction(ctx context.Context) (TransactionMetadata, error) {
	if c.isClosed() {
		return TransactionMetadata{}, ErrClosed
	}
	if c.options.TransactionalID == "" {
		return TransactionMetadata{}, errors.New("transactional id must not be blank")
	}
	if err := c.ensureProducerID(ctx); err != nil {
		return TransactionMetadata{}, err
	}
	return c.sendTransaction(ctx, protocol.ApiKeyBeginTxn, false)
}

// CommitTransaction commits the current transaction.
func (c *Client) CommitTransaction(ctx context.Context) (TransactionMetadata, error) {
	return c.EndTransaction(ctx, true)
}

// AbortTransaction aborts the current transaction.
func (c *Client) AbortTransaction(ctx context.Context) (TransactionMetadata, error) {
	return c.EndTransaction(ctx, false)
}

// EndTransaction finishes the current transaction.
func (c *Client) EndTransaction(ctx context.Context, commit bool) (TransactionMetadata, error) {
	if c.isClosed() {
		return TransactionMetadata{}, ErrClosed
	}
	if c.options.TransactionalID == "" {
		return TransactionMetadata{}, errors.New("transactional id must not be blank")
	}
	if err := c.ensureProducerID(ctx); err != nil {
		return TransactionMetadata{}, err
	}
	return c.sendTransaction(ctx, protocol.ApiKeyEndTxn, commit)
}

func (c *Client) ensureProducerID(ctx context.Context) error {
	if !c.options.Idempotent {
		return nil
	}
	c.stateMu.Lock()
	initialized := c.producerID >= 0
	c.stateMu.Unlock()
	if initialized {
		return nil
	}
	_, err := c.initProducerID(ctx)
	return err
}

func (c *Client) initProducerID(ctx context.Context) (TransactionMetadata, error) {
	c.initMu.Lock()
	defer c.initMu.Unlock()
	c.stateMu.Lock()
	if c.producerID >= 0 {
		metadata := TransactionMetadata{ProducerID: c.producerID, ProducerEpoch: c.epoch}
		c.stateMu.Unlock()
		return metadata, nil
	}
	c.stateMu.Unlock()

	endpoint, err := c.metadata.BootstrapEndpoint()
	if err != nil {
		return TransactionMetadata{}, err
	}
	requestCtx, cancel := c.requestContext(ctx)
	defer cancel()
	response, err := c.protocol.Send(requestCtx, endpoint, protocol.ApiKeyInitProducerID, protocol.DefaultAPIVersion, message.InitProducerIDRequestBody{
		TransactionalID: c.transactionalID(),
	})
	if err != nil {
		return TransactionMetadata{}, err
	}
	typed, ok := response.Body.(message.InitProducerIDResponseBody)
	if !ok {
		return TransactionMetadata{}, errors.New("unexpected InitProducerID response body")
	}
	if typed.ErrorCode != protocol.ErrorCodeNone {
		return TransactionMetadata{}, &protocol.ClientError{Code: typed.ErrorCode, Message: "init producer id failed"}
	}
	c.stateMu.Lock()
	c.producerID = typed.ProducerID
	c.epoch = typed.ProducerEpoch
	c.stateMu.Unlock()
	return TransactionMetadata{ProducerID: typed.ProducerID, ProducerEpoch: typed.ProducerEpoch}, nil
}

func (c *Client) sendTransaction(ctx context.Context, apiKey protocol.ApiKey, commit bool) (TransactionMetadata, error) {
	endpoint, err := c.metadata.BootstrapEndpoint()
	if err != nil {
		return TransactionMetadata{}, err
	}
	c.stateMu.Lock()
	producerID := c.producerID
	producerEpoch := c.epoch
	c.stateMu.Unlock()
	requestCtx, cancel := c.requestContext(ctx)
	defer cancel()
	response, err := c.protocol.Send(requestCtx, endpoint, apiKey, protocol.DefaultAPIVersion, message.TransactionRequestBody{
		TransactionalID: c.transactionalID(),
		ProducerID:      producerID,
		ProducerEpoch:   producerEpoch,
		Commit:          commit,
	})
	if err != nil {
		return TransactionMetadata{}, err
	}
	typed, ok := response.Body.(message.TransactionResponseBody)
	if !ok {
		return TransactionMetadata{}, errors.New("unexpected Transaction response body")
	}
	if typed.ErrorCode != protocol.ErrorCodeNone {
		return TransactionMetadata{}, &protocol.ClientError{Code: typed.ErrorCode, Message: "transaction request failed"}
	}
	return TransactionMetadata{
		ProducerID:       typed.ProducerID,
		ProducerEpoch:    typed.ProducerEpoch,
		TransactionState: typed.TransactionState,
	}, nil
}
