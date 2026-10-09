package admin

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// Server provides the HTTP/REST and SSE control plane for the Flux Engine.
type Server struct {
	engine         EngineContext
	cfg            Config
	broker         *SSEBroker
	httpServer     *http.Server
	listener       net.Listener
	cancelObserver func()
	running        atomic.Bool
}

// NewServer initializes an Admin control plane server with the provided engine context and configuration.
func NewServer(engine EngineContext, cfg Config) (*Server, error) {
	if engine == nil {
		return nil, fmt.Errorf("flux admin: engine context required")
	}
	if cfg.Addr == "" {
		cfg.Addr = ":9090"
	}

	broker := NewSSEBroker()

	s := &Server{
		engine: engine,
		cfg:    cfg,
		broker: broker,
	}

	mux := http.NewServeMux()
	s.registerRoutes(mux)

	s.httpServer = &http.Server{
		Handler:      s.authMiddleware(mux),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 0, // Unbounded write timeout required for persistent SSE streams
		IdleTimeout:  60 * time.Second,
	}

	return s, nil
}

// Start binds to the configured network address and begins serving HTTP/SSE requests in the background.
func (s *Server) Start() error {
	if s.running.Swap(true) {
		return fmt.Errorf("flux admin: server already running")
	}

	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		s.running.Store(false)
		return fmt.Errorf("flux admin: failed to listen on %s: %w", s.cfg.Addr, err)
	}
	s.listener = ln

	// Hook engine alert events into SSE broker
	s.cancelObserver = s.engine.RegisterAlertObserver(s.broker.Publish)

	go func() {
		if err := s.httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			// Log or handle server error
			_ = err
		}
	}()

	return nil
}

// Stop gracefully shuts down the Admin server, draining SSE connections.
func (s *Server) Stop(ctx context.Context) error {
	if !s.running.Swap(false) {
		return nil
	}

	if s.cancelObserver != nil {
		s.cancelObserver()
		s.cancelObserver = nil
	}

	if s.broker != nil {
		s.broker.Close()
	}

	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}

	return nil
}

// Addr returns the bound network address of the running server.
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.cfg.Addr
}

// URL returns the full HTTP URL of the running server.
func (s *Server) URL() string {
	return fmt.Sprintf("http://%s", s.Addr())
}

// Broker returns the SSE alert event broker.
func (s *Server) Broker() *SSEBroker {
	return s.broker
}
