package flux

import (
	"github.com/cuprite-io/flux/internal/cache"
	"github.com/cuprite-io/flux/internal/schematap"
	"github.com/cuprite-io/flux/internal/sink"
)

// Option configures an Engine instance.
type Option func(*Engine)

// WithCache sets the distributed CacheBackend for state and persistence.
func WithCache(backend cache.CacheBackend) Option {
	return func(e *Engine) {
		e.cache = backend
	}
}

// WithWorkers sets the concurrency worker count for the internal parallel pool.
func WithWorkers(numWorkers int) Option {
	return func(e *Engine) {
		e.workers = numWorkers
	}
}

// WithSink registers a named Sink with the engine.
func WithSink(name string, s sink.Sink) Option {
	return func(e *Engine) {
		e.sinks.Register(name, s)
	}
}

// WithSchemaLearning enables streaming schema inference over Spark payloads via assay.
func WithSchemaLearning(enable bool, cfg ...schematap.Config) Option {
	return func(e *Engine) {
		e.schemaLearning = enable
		if len(cfg) > 0 {
			e.schemaConfig = cfg[0]
		} else {
			e.schemaConfig = schematap.DefaultConfig()
		}
		e.schemaConfig.Enabled = enable
	}
}

// WithSchemaDiscriminators configures prioritized discriminator keys for schema partitioning.
func WithSchemaDiscriminators(keys ...string) Option {
	return func(e *Engine) {
		e.schemaConfig.DiscriminatorKeys = keys
	}
}

// WithSchemaAsync sets whether schema learning is offloaded asynchronously from Spark's hot path.
func WithSchemaAsync(async bool) Option {
	return func(e *Engine) {
		e.schemaConfig.Async = async
	}
}

// WithSchemaSampleRate sets the sampling ratio (0.0 to 1.0) for schema inference.
func WithSchemaSampleRate(rate float64) Option {
	return func(e *Engine) {
		e.schemaConfig.SampleRate = rate
	}
}

// WithSchemaQueueSize sets the bounded channel buffer size for asynchronous schema sampling.
func WithSchemaQueueSize(size int) Option {
	return func(e *Engine) {
		e.schemaConfig.QueueSize = size
	}
}

