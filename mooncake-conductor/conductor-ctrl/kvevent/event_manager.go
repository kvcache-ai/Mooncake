package kvevent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"conductor/common"
	"conductor/prefixindex"
	"conductor/zmq"
)

type EventManager struct {
	indexer        *prefixindex.PrefixCacheTable
	services       []common.ServiceConfig
	httpserverport int

	subscribers common.SyncMap[string, *zmq.ZMQClient]

	// Lifecycle management
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.RWMutex
	stopped bool
}

func NewEventManager(
	services []common.ServiceConfig,
	httpserverport int,
) *EventManager {
	ctx, cancel := context.WithCancel(context.Background())
	indexer := prefixindex.NewPrefixCacheTable()

	return &EventManager{
		services:       services,
		indexer:        indexer,
		httpserverport: httpserverport,
		ctx:            ctx,
		cancel:         cancel,
	}
}

func (m *EventManager) Start() error {
	slog.Info("Starting KV Event Manager...")

	// Subscribe to all services concurrently
	var wg sync.WaitGroup
	errCh := make(chan error, len(m.services))

	for _, svc := range m.services {
		wg.Add(1)
		go func(service common.ServiceConfig) {
			defer wg.Done()
			if err := m.subscribeToService(service); err != nil {
				slog.Error("Failed to initiate subscription",
					"service_type", service.Type,
					"service_name", service.Name,
					"service_ip", service.IP,
					"error", err,
				)
				errCh <- fmt.Errorf("failed to subscribe to %s: %w", service.Name, err)
			}
		}(svc)
	}

	wg.Wait()
	close(errCh)

	failureCount := len(errCh)
	successCount := len(m.services) - failureCount
	slog.Info("Static KV Event Manager started. Subscriptions",
		"success", successCount,
		"failed", failureCount,
	)

	return nil
}

func (m *EventManager) Stop() {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return
	}
	m.stopped = true
	m.mu.Unlock()

	slog.Info("Stopping Conductor KV Event Manager.....")

	// Cancel context
	m.cancel()

	// Stop all ZMQ clients
	m.subscribers.Range(func(key string, client *zmq.ZMQClient) bool {
		client.Stop()
		slog.Info("Stopped all subscription",
			"service_key", key,
		)
		return true
	})
}

func (m *EventManager) subscribeToService(svc common.ServiceConfig) error {
	if _, exists := m.subscribers.Load(svc.Name); exists {
		return nil
	}

	handler := &KVEventHandler{
		manager:   m,
		svcName:   svc.IP,
		modelName: svc.ModelName,
		loraID:    svc.LoraID,
	}

	// Configure ZMQ Client
	zmqConfig := &zmq.ZMQClientConfig{
		CachePoolKey:   svc.Name,
		ServiceIP:      svc.IP,
		Port:           svc.Port,
		ModelName:      svc.ModelName,
		PollTimeout:    100 * time.Millisecond,
		ReplayTimeout:  5 * time.Second,
		ReconnectDelay: 1 * time.Second,
		RouterPort:     svc.Port + 1,
	}

	// Validate ZMQ config
	if err := zmq.ValidateConfig(zmqConfig); err != nil {
		return fmt.Errorf("invalid ZMQ config: %w", err)
	}

	// Create and start client
	client := zmq.NewZMQClient(zmqConfig, handler)
	if err := client.Start(); err != nil {
		return fmt.Errorf("failed to start ZMQ client: %w", err)
	}

	m.subscribers.Store(svc.Name, client)
	slog.Info("Successfully subscribed to service",
		"service_type", svc.Type,
		"service_name", svc.Name,
		"service_ip", svc.IP,
		"service_port", svc.Port,
	)

	return nil
}

func (m *EventManager) getIndexer() *prefixindex.PrefixCacheTable {
	return m.indexer
}

func (m *EventManager) StartHTTPServer() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/cache", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var jsonBody map[string]interface{}
		slog.Debug(
			"receive req",
			"method", r.Method,
			"path", r.URL.Path,
			"remote", r.RemoteAddr,
		)
		if err := json.NewDecoder(r.Body).Decode(&jsonBody); err != nil {
			slog.Error("Failed to decode JSON", "err", err)
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		tokenIDs, err := common.ExtractTokenIdFromRequest(jsonBody, "token_ids")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		candidates, err := common.ExtractCandidateEngineFromRequest(jsonBody, "instances")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		modelName, err := common.ExtractStringValueFromRequest(jsonBody, "model_name")
		if err != nil {
			slog.Error("Failed to decode string", "err", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		loraID, err := common.ExtractIntFromRequest(jsonBody, "lora_id")
		if err != nil {
			slog.Error("Failed to decode int", "err", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		cacheHitResult := m.indexer.CacheHitCompute(modelName, loraID, tokenIDs, candidates)
		slog.Debug("cache hit status", "hitresult", cacheHitResult)
		response := map[string]interface{}{
			"HitStatus": cacheHitResult,
			"status":    "ok",
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			slog.Error("Failed to encode response", "err", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", m.httpserverport),
		Handler: mux,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		slog.Info("HTTP server listening", "port", m.httpserverport)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server failed", "err", err)
		}
	}()

	// Start a goroutine to listen for context cancellation, used for graceful shutdown.
	go func() {
		<-m.ctx.Done()
		slog.Info("Shutting down HTTP server")
		// 5-second timeout for forced shutdown
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Error("HTTP server shutdown error", "err", err)
			server.Close()
		}
	}()

	return nil
}
