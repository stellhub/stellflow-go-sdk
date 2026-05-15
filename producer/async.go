package producer

import (
	"context"
	"time"
)

type asyncRecord struct {
	ctx    context.Context
	record Record
	future *Future
}

type flushRequest struct {
	ctx  context.Context
	done chan error
}

type batchKey struct {
	topic     string
	partition int32
}

type producerBatch struct {
	records []Record
	futures []*Future
	bytes   int
}

func (c *Client) ensureWorker() {
	c.workerOnce.Do(func() {
		go c.runWorker()
	})
}

func (c *Client) runWorker() {
	defer close(c.workerDone)
	ticker := time.NewTicker(c.options.Linger)
	defer ticker.Stop()
	batches := make(map[batchKey]*producerBatch)
	for {
		select {
		case item := <-c.asyncCh:
			c.addAsyncRecord(batches, item)
		case request := <-c.flushCh:
			c.drainAsyncRecords(batches)
			request.done <- c.flushBatches(request.ctx, batches)
		case <-ticker.C:
			_ = c.flushBatches(context.Background(), batches)
		case <-c.stopCh:
			c.drainAsyncRecords(batches)
			_ = c.flushBatches(context.Background(), batches)
			return
		}
	}
}

func (c *Client) drainAsyncRecords(batches map[batchKey]*producerBatch) {
	for {
		select {
		case item := <-c.asyncCh:
			c.addAsyncRecord(batches, item)
		default:
			return
		}
	}
}

func (c *Client) addAsyncRecord(batches map[batchKey]*producerBatch, item asyncRecord) {
	if item.ctx.Err() != nil {
		item.future.complete(Metadata{}, item.ctx.Err())
		return
	}
	partition, err := c.selectPartition(item.ctx, item.record)
	if err != nil {
		item.future.complete(Metadata{}, err)
		return
	}
	key := batchKey{topic: item.record.Topic, partition: partition}
	batch := batches[key]
	if batch == nil {
		batch = &producerBatch{}
		batches[key] = batch
	}
	batch.records = append(batch.records, item.record)
	batch.futures = append(batch.futures, item.future)
	batch.bytes += estimateRecordBytes(item.record)
	if len(batch.records) >= c.options.BatchSize || batch.bytes >= c.options.BatchBytes {
		c.flushBatch(item.ctx, key, batch)
		delete(batches, key)
	}
}

func (c *Client) flushBatches(ctx context.Context, batches map[batchKey]*producerBatch) error {
	var firstErr error
	for key, batch := range batches {
		if len(batch.records) == 0 {
			delete(batches, key)
			continue
		}
		if err := c.flushBatch(ctx, key, batch); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(batches, key)
	}
	return firstErr
}

func (c *Client) flushBatch(ctx context.Context, key batchKey, batch *producerBatch) error {
	metadata, err := c.produceRecords(ctx, key.topic, key.partition, batch.records)
	if err != nil {
		for _, future := range batch.futures {
			future.complete(Metadata{}, err)
		}
		return err
	}
	for index, future := range batch.futures {
		if index >= len(metadata) {
			future.complete(Metadata{}, context.Canceled)
			continue
		}
		future.complete(metadata[index], nil)
	}
	return nil
}

func estimateRecordBytes(record Record) int {
	size := len(record.Key) + len(record.Value) + 32
	for _, header := range record.Headers {
		if header.Key != nil {
			size += len(*header.Key)
		}
		size += len(header.Value) + 8
	}
	return size
}
