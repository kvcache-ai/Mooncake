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

// Dynamic register structure
type RegisterReq struct {
	Endpoint       string  `json:"endpoint"`
	ReplayEndpoint string  `json:"replay_endpoint"`
	Type           string  `json:"type"`
	ModelName      string  `json:"modelname"`
	LoraName       *string `json:"lora_name"`
	TenantID       *string `json:"tenant_id"`
	InstanceID     string  `json:"instance_id"`
	BlockSize      int64   `json:"block_size"`
	DPRank         int     `json:"dp_rank"`
	AdditionalSalt *string `json:"additionalsalt"`
}

// Dynamic unregister structure
type UnregisterReq struct {
	Type       string  `json:"type"`
	ModelName  string  `json:"modelname"`
	LoraName   *string `json:"lora_name"`
	TenantID   *string `json:"tenant_id"`
	InstanceID string  `json:"instance_id"`
	BlockSize  int     `json:"block_size"`
	DPRank     int     `json:"dp_rank"`
}

type QueryReq struct {
	ModelName  string  `json:"model"`
	LoraName   *string `json:"lora_name"`
	LoraID     *int64  `json:"lora_id"`
	TokenIDs   []int32 `json:"token_ids"`
	InstanceID *string `json:"instance_id"`
	TenantID   *string `json:"tenant_id"`
	BlockSize  int64   `json:"block_size"`
	CacheSalt  *string `json:"cache_salt"`
}

type EventManager struct {
	indexer        *prefixindex.PrefixCacheTable
	services       []common.ServiceConfig
	httpserverport int

	subscribers common.SyncMap[string, *zmq.ZMQClient]

	// Map to store active configurations
	activeConfigs common.SyncMap[string, common.ServiceConfig]
	// Map to store tenant instance list
	tenantInstanceMap map[string]map[string]struct{}

	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.RWMutex
	tenantMutex sync.RWMutex
	stopped     bool
}

func NewEventManager(
	services []common.ServiceConfig,
	httpserverport int,
) *EventManager {
	ctx, cancel := context.WithCancel(context.Background())
	indexer := prefixindex.NewPrefixCacheTable()
	// TODO 每个ModelContext创建一个独立的indexer

	return &EventManager{
		services:          services,
		indexer:           indexer,
		httpserverport:    httpserverport,
		ctx:               ctx,
		cancel:            cancel,
		tenantInstanceMap: make(map[string]map[string]struct{}),
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
					"instance_id", service.InstanceID,
					"endpoint", service.Endpoint,
					"error", err,
				)
				errCh <- fmt.Errorf("failed to subscribe to %s: %w", service.InstanceID, err)
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

func makeServiceKey(instanceID string, tenantID string, dpRank int) string {
	return fmt.Sprintf("%s|%s|%d", instanceID, tenantID, dpRank)
}

func (m *EventManager) subscribeToService(svc common.ServiceConfig) error {
	// Use (instance_id, tenant_id) as composite key to support multi-tenant replicas
	svcKey := makeServiceKey(svc.InstanceID, svc.TenantID, svc.DPRank)
	if svc.InstanceID == "" {
		svcKey = makeServiceKey(svc.Endpoint, svc.TenantID, svc.DPRank)
	}

	if _, exists := m.subscribers.Load(svcKey); exists {
		return nil
	}

	// Validate endpoint
	if svc.Endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}

	// Use ReplayEndpoint directly, fallback to empty if not provided
	replayEndpoint := svc.ReplayEndpoint

	handler := &KVEventHandler{
		manager:        m,
		tenant_id:      svc.TenantID,
		modelName:      svc.ModelName,
		loraName:       svc.LoraName,
		instanceID:     svc.InstanceID,
		blockSize:      svc.BlockSize,
		additionalSalt: svc.AdditionalSalt,
	}

	// Configure ZMQ Client
	zmqConfig := &zmq.ZMQClientConfig{
		CachePoolKey:   svcKey,
		Endpoint:       svc.Endpoint,
		ReplayEndpoint: replayEndpoint,
		ModelName:      svc.ModelName,
		PollTimeout:    100 * time.Millisecond,
		ReplayTimeout:  5 * time.Second,
		ReconnectDelay: 1 * time.Second,
	}

	if err := zmq.ValidateConfig(zmqConfig); err != nil {
		return fmt.Errorf("invalid ZMQ config: %w", err)
	}

	client := zmq.NewZMQClient(zmqConfig, handler)
	if err := client.Start(); err != nil {
		return fmt.Errorf("failed to start ZMQ client: %w", err)
	}

	m.subscribers.Store(svcKey, client)
	m.activeConfigs.Store(svcKey, svc)

	//Add instance to tenant's instance map
	m.tenantMutex.Lock()
	if _, exists := m.tenantInstanceMap[svc.TenantID]; !exists {
		m.tenantInstanceMap[svc.TenantID] = make(map[string]struct{})
	}

	m.tenantInstanceMap[svc.TenantID][svc.InstanceID] = struct{}{}
	m.tenantMutex.Unlock()

	slog.Info("Successfully subscribed to service",
		"service_type", svc.Type,
		"service_key", svcKey,
		"instance_id", svc.InstanceID,
		"tenant_id", svc.TenantID,
		"endpoint", svc.Endpoint,
		"replay_endpoint", replayEndpoint,
	)

	return nil
}

func (m *EventManager) unsubscribeFromService(instanceID string, tenantID string, dpRank int) {
	svcKey := makeServiceKey(instanceID, tenantID, dpRank)
	if client, exists := m.subscribers.Load(svcKey); exists {
		client.Stop()
		m.subscribers.Delete(svcKey)
		m.activeConfigs.Delete(svcKey)

		// Remove engine_instance from tenant's instance set
		m.tenantMutex.Lock()
		if instanceSet, exists := m.tenantInstanceMap[tenantID]; exists {
			delete(instanceSet, instanceID)
		}
		m.tenantMutex.Unlock()
		slog.Info("Successfully unsubscribed from service",
			"service_key", svcKey,
			"instance_id", instanceID,
			"tenant_id", tenantID,
		)
	}
}

func (m *EventManager) getIndexer() *prefixindex.PrefixCacheTable {
	return m.indexer
}

func (m *EventManager) StartHTTPServer() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req QueryReq
		slog.Debug(
			"receive req",
			"method", r.Method,
			"path", r.URL.Path,
			"remote", r.RemoteAddr,
		)

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			slog.Error("Failed to decode JSON", "err", err)
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		tenantID := "default"
		if req.TenantID != nil && *req.TenantID != "" {
			tenantID = *req.TenantID
		}

		loraName := ""
		if req.LoraName != nil {
			slog.Debug("LoraName is provided", "lora_name", *req.LoraName)
			loraName = *req.LoraName
		}

		cacheSalt := ""
		if req.CacheSalt != nil {
			slog.Debug("cacheSalt is provided", "cacheSalt", *req.CacheSalt)
			cacheSalt = *req.CacheSalt
		}

		modelContext := &prefixindex.ModelContext{
			TenantID:       tenantID,
			ModelName:      req.ModelName,
			LoraName:       loraName,
			BlockSize:      req.BlockSize,
			AdditionalSalt: cacheSalt,
		}

		// cacheHitResult := m.indexer.CacheHitCompute(modelContext, req.TokenIDs, req.InstanceID)
		response_result := make(map[string]map[string]prefixindex.CacheHitResult)

		if req.InstanceID != nil {
			slog.Info("search all engine instance for tenant. ", "instance_id", req.InstanceID)
			result := m.indexer.CacheHitCompute(modelContext, req.TokenIDs, *req.InstanceID)
			if result != nil {
				if response_result[tenantID] == nil {
					response_result[tenantID] = make(map[string]prefixindex.CacheHitResult)
				}
				response_result[tenantID][*req.InstanceID] = *result
			}
		} else {
			if instanceSet, exists := m.tenantInstanceMap[tenantID]; exists {
				for instanceID := range instanceSet {
					result := m.indexer.CacheHitCompute(modelContext, req.TokenIDs, instanceID)
					if result != nil {
						if response_result[tenantID] == nil {
							response_result[tenantID] = make(map[string]prefixindex.CacheHitResult)
						}
						response_result[tenantID][instanceID] = *result
					}
				}
			} else {
				slog.Warn("current tenant has no engine_instance. ", "tenant_id", tenantID)
			}
		}

		slog.Debug("cache hit status", "hitresult", response_result)

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response_result); err != nil {
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
			slog.Info("LoraName is provided", "lora_name", *req.LoraName)
			loraName = *req.LoraName
		}

		additionalSalt := ""
		if req.AdditionalSalt != nil {
			additionalSalt = *req.AdditionalSalt
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
			AdditionalSalt: additionalSalt,
		}

		if err := m.subscribeToService(svc); err != nil {
			slog.Error("Dynamic register failed", "instance_id", req.InstanceID, "err", err)
			http.Error(w, fmt.Sprintf("Failed to subscribe: %v", err), http.StatusInternalServerError)
			return
		}
		m.services = append(m.services, svc)
		modelContext := &prefixindex.ModelContext{
			TenantID:       tenantID,
			ModelName:      req.ModelName,
			LoraName:       loraName,
			BlockSize:      req.BlockSize,
			AdditionalSalt: additionalSalt,
		}

		m.indexer.AddDpSize(modelContext, svc.InstanceID, int64(svc.DPRank))

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

		// Build target service key from instance_id and tenant_id
		targetTenant := "default"
		if req.TenantID != nil && *req.TenantID != "" {
			targetTenant = *req.TenantID
		}
		targetKey := makeServiceKey(req.InstanceID, targetTenant, req.DPRank)

		// Direct lookup and removal
		if _, exists := m.activeConfigs.Load(targetKey); !exists {
			http.Error(w, fmt.Sprintf("service not found: %s", targetKey), http.StatusNotFound)
			return
		}

		m.unsubscribeFromService(req.InstanceID, targetTenant, req.DPRank)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":            "unregistered successfully",
			"removed_instances": []string{targetKey},
		})
	})

	// Global view interface
	mux.HandleFunc("/global_view", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		globalView := m.indexer.GetGlobalView()

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(globalView); err != nil {
			slog.Error("Failed to encode global view response", "err", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
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
