package kvevent

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"conductor/common"
	"conductor/prefixindex"
	"conductor/zmq"
)

// Define dynamic register structure to match the JSON request body
type RegisterReq struct {
	Endpoint       string  `json:"endpoint"`
	ReplayEndpoint string  `json:"replay_endpoint"`
	Type           string  `json:"type"`
	ModelName      string  `json:"modelname"`
	LoraName       *string `json:"lora_name"`
	TenantID       *string `json:"tenant_id"`
	InstanceID     string  `json:"instance_id"`
	BlockSize      int     `json:"block_size"`
	DPRank         int     `json:"dp_rank"`
	AdditionalSalt string  `json:"additionalsalt"`
}

// Define dynamic unregister structure to match the JSON request body
type UnregisterReq struct {
	Type       string  `json:"type"`
	ModelName  string  `json:"modelname"`
	LoraName   *string `json:"lora_name"`
	TenantID   *string `json:"tenant_id"`
	InstanceID string  `json:"instance_id"`
	BlockSize  int     `json:"block_size"`
	DPRank     int     `json:"dp_rank"`
}

type EventManager struct {
	indexer        *prefixindex.PrefixCacheTable
	services       []common.ServiceConfig
	httpserverport int

	subscribers common.SyncMap[string, *zmq.ZMQClient]

	// Map to store active configurations
	activeConfigs common.SyncMap[string, common.ServiceConfig]

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

func parseEndpoint(ep string) (string, string, int, error) {
	// Protocol parsing section
	protocolEnd := strings.Index(ep, "://")
	if protocolEnd == -1 {
		return "", "", 0, fmt.Errorf("invalid endpoint format: %s", ep)
	}
	protocol := ep[:protocolEnd]

	// Trim protocol header
	trimmed := ep[protocolEnd+3:]
	parts := strings.Split(trimmed, ":")
	if len(parts) != 2 {
		return "", "", 0, fmt.Errorf("invalid endpoint format: %s", ep)
	}

	ip := parts[0]
	portStr := parts[1]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", "", 0, fmt.Errorf("invalid port number: %w", err)
	}

	return protocol, ip, port, nil
}

func loraNameToID(name string) int64 {
	if name == "" {
		return 0
	}
	h := fnv.New64a()
	h.Write([]byte(name))
	return int64(h.Sum64())
}

func (m *EventManager) subscribeToService(svc common.ServiceConfig) error {
	// Use InstanceID as the unique key (with Endpoint as a fallback).
	svcKey := svc.InstanceID
	if svcKey == "" {
		svcKey = svc.Endpoint
	}

	if _, exists := m.subscribers.Load(svcKey); exists {
		return nil
	}

	// Parse the Endpoint to obtain the IP and Port
	protocol, ip, port, err := parseEndpoint(svc.Endpoint)
	if err != nil {
		return fmt.Errorf("invalid endpoint format: %w", err)
	}

	// Parse ReplayEndpoint
	routerPort := port + 1 // Default degradation strategy
	if svc.ReplayEndpoint != "" {
		rProtocol, rIP, rPort, err := parseEndpoint(svc.ReplayEndpoint)
		if err == nil {
			routerPort = rPort
		}
	}

	// Construct an EventHandler to convert LoraName to LoraID to adapt to the old structure.
	handler := &KVEventHandler{
		manager:   m,
		svcName:   svcKey,
		modelName: svc.ModelName,
		loraID:    loraNameToID(svc.LoraName),
	}

	// Configure ZMQ Client
	zmqConfig := &zmq.ZMQClientConfig{
		CachePoolKey:   svcKey,
		ServiceIP:      ip,
		Port:           port,
		ModelName:      svc.ModelName,
		PollTimeout:    100 * time.Millisecond,
		ReplayTimeout:  5 * time.Second,
		ReconnectDelay: 1 * time.Second,
		RouterPort:     routerPort,
	}

	if err := zmq.ValidateConfig(zmqConfig); err != nil {
		return fmt.Errorf("invalid ZMQ config: %w", err)
	}

	client := zmq.NewZMQClient(zmqConfig, handler)
	if err := client.Start(); err != nil {
		return fmt.Errorf("failed to start ZMQ client: %w", err)
	}

	m.subscribers.Store(svcKey, client)
	m.activeConfigs.Store(svcKey, svc) // 记录 Config 用于反向查找

	slog.Info("Successfully subscribed to service",
		"service_type", svc.Type,
		"instance_id", svcKey,
		"endpoint", svc.Endpoint,
		"tenant_id", svc.TenantID,
	)

	return nil
}

func (m *EventManager) unsubscribeFromService(instanceID string) {
	if client, exists := m.subscribers.Load(instanceID); exists {
		client.Stop()
		m.subscribers.Delete(instanceID)
		m.activeConfigs.Delete(instanceID)
		slog.Info("Successfully unsubscribed from service", "instance_id", instanceID)
	}
}

func (m *EventManager) getIndexer() *prefixindex.PrefixCacheTable {
	return m.indexer
}

func (m *EventManager) StartHTTPServer() error {
	mux := http.NewServeMux()

	// Original /cache interface
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

	// Register interface
	mux.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req RegisterReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			slog.Error("Failed to decode register JSON", "err", err)
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		// Handle Optional fields' default values
		tenantID := "default"
		if req.TenantID != nil && *req.TenantID != "" {
			tenantID = *req.TenantID
		}
		loraName := ""
		if req.LoraName != nil {
			loraName = *req.LoraName
		}

		svc := common.ServiceConfig{
			Endpoint:       req.Endpoint,
			ReplayEndpoint: req.ReplayEndpoint,
			Type:           req.Type,
			ModelName:      req.ModelName,
			LoraName:       loraName,
			TenantID:       tenantID,
			InstanceID:     req.InstanceID,
			BlockSize:      req.BlockSize,
			DPRank:         req.DPRank,
			AdditionalSalt: req.AdditionalSalt,
		}

		// Use the existing subscribeToService method
		if err := m.subscribeToService(svc); err != nil {
			slog.Error("Dynamic register failed", "instance_id", req.InstanceID, "err", err)
			http.Error(w, fmt.Sprintf("Failed to subscribe: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status":      "registered successfully",
			"instance_id": svc.InstanceID,
		})
	})

	// Unregister interface
	mux.HandleFunc("/unregister", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req UnregisterReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			slog.Error("Failed to decode unregister JSON", "err", err)
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		targetInstance := req.InstanceID
		targetTenant := ""
		if req.TenantID != nil {
			targetTenant = *req.TenantID
		}

		removedInstances := []string{}

		// Traverse the currently running services, performing precise filtering and unregistration
		m.activeConfigs.Range(func(key string, val common.ServiceConfig) bool {
			// Match Type and ModelName
			if val.Type == req.Type && val.ModelName == req.ModelName {
				// TenantID filter: If empty, unregister all tenants; otherwise, must match
				if targetTenant == "" || val.TenantID == targetTenant {
					// InstanceID filter: Exact match for a single instance
					if targetInstance == "" || val.InstanceID == targetInstance {
						removedInstances = append(removedInstances, key)
					}
				}
			}
			return true // Continue traversal
		})

		for _, instanceKey := range removedInstances {
			m.unsubscribeFromService(instanceKey)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":            "unregistered successfully",
			"removed_instances": removedInstances,
		})
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", m.httpserverport),
		Handler: mux,
	}

	go func() {
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
