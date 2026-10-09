package admin

import (
	"sync"
	"sync/atomic"
)

// SSEBroker coordinates real-time alert event distribution to connected SSE clients.
type SSEBroker struct {
	mu        sync.RWMutex
	clients   map[chan AlertEvent]struct{}
	closed    atomic.Bool
}

// NewSSEBroker creates and initializes a new SSE alert broker.
func NewSSEBroker() *SSEBroker {
	return &SSEBroker{
		clients: make(map[chan AlertEvent]struct{}),
	}
}

// Subscribe registers a new SSE client listener channel with a bounded buffer.
func (b *SSEBroker) Subscribe() chan AlertEvent {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch := make(chan AlertEvent, 64)
	if b.closed.Load() {
		close(ch)
		return ch
	}
	b.clients[ch] = struct{}{}
	return ch
}

// Unsubscribe removes a client listener channel and drains it.
func (b *SSEBroker) Unsubscribe(ch chan AlertEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.clients[ch]; exists {
		delete(b.clients, ch)
		close(ch)
	}
}

// Publish broadcasts an alert event to all currently connected clients non-blockingly.
func (b *SSEBroker) Publish(evt AlertEvent) {
	if b.closed.Load() {
		return
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	for ch := range b.clients {
		select {
		case ch <- evt:
		default:
			// Slow consumer drop: client channel buffer full, skip to preserve real-time streaming
		}
	}
}

// Close gracefully shuts down the broker and closes all client subscriber channels.
func (b *SSEBroker) Close() {
	if b.closed.Swap(true) {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for ch := range b.clients {
		close(ch)
		delete(b.clients, ch)
	}
}
