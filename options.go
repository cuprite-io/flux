package flux

import (
	"github.com/cuprite-io/flux/internal/cache"
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
