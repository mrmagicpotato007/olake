package utils

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/datazip-inc/olake/logger"
	"golang.org/x/sync/errgroup"
)

// Global pool for controlling total concurrent database operations
var ReaderPool struct {
	semaphore chan struct{}
	once      sync.Once
	mutex     sync.Mutex
	active    atomic.Int64
	waiting   atomic.Int64
	total     atomic.Int64
}

// InitReaderPool initializes the global reader pool with the specified concurrency limit.
// This function should be called during application startup to ensure a consistent
// concurrency limit is used. If not called explicitly, the pool will be initialized
// with the concurrency value from the first ConcurrentReader call.
//
// Important: Only the first initialization (either explicit via this function or implicit
// via ConcurrentReader) will set the concurrency limit. Subsequent calls with different
// concurrency values will have no effect on the pool size.
func InitReaderPool(maxConcurrent int) {
	ReaderPool.once.Do(func() {
		ReaderPool.semaphore = make(chan struct{}, maxConcurrent)
		logger.Infof("Initialized global reader pool with max concurrent: %d", maxConcurrent)
	})
}

// ConcurrentReader executes database read operations with a global concurrency limit
// to prevent database connection overload across multiple streams.
//
// Note: If the global reader pool hasn't been initialized via InitReaderPool(),
// this function will initialize it with the provided concurrency parameter.
// Only the first initialization will take effect.
func ConcurrentReader[T any](ctx context.Context, array []T, concurrency int,
	execute func(ctx context.Context, one T, executionNumber int) error) error {

	// Initialize global pool if needed
	ReaderPool.once.Do(func() {
		ReaderPool.semaphore = make(chan struct{}, concurrency)
		logger.Infof("Initialized global reader pool with max concurrent: %d", concurrency)
	})

	executor, ctx := errgroup.WithContext(ctx)

	for idx, one := range array {
		idx, one := idx, one // capture
		executor.Go(func() error {
			// TODO: Consider removing or reducing these debug logs before production release
			// They are useful for development but may generate excessive log volume in production

			// Increment waiting counter
			waiting := ReaderPool.waiting.Add(1)
			logger.Debugf("Chunk waiting for queue slot. Active: %d, Waiting: %d, Total: %d",
				ReaderPool.active.Load(), waiting, ReaderPool.total.Load())

			// Acquire global semaphore
			select {
			case ReaderPool.semaphore <- struct{}{}:
				// Decrement waiting, increment active
				ReaderPool.waiting.Add(-1)
				active := ReaderPool.active.Add(1)
				logger.Debugf("Chunk started execution. Active: %d, Waiting: %d, Total: %d",
					active, ReaderPool.waiting.Load(), ReaderPool.total.Load())

				defer func() {
					<-ReaderPool.semaphore
					active := ReaderPool.active.Add(-1)
					total := ReaderPool.total.Add(1)
					logger.Debugf("Chunk completed execution. Active: %d, Waiting: %d, Total: %d",
						active, ReaderPool.waiting.Load(), total)
				}()
			case <-ctx.Done():
				ReaderPool.waiting.Add(-1)
				return ctx.Err()
			}

			// Execute with global limit enforced
			return execute(ctx, one, idx+1)
		})
	}

	return executor.Wait()
}
