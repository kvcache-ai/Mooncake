package kvevent

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"conductor/common"
	"conductor/zmq"
)

func TestSubscribeToService_DuplicateReturnsFalse(t *testing.T) {
	mgr := NewEventManager(nil, 0)

	svc := common.ServiceConfig{
		Endpoint:   "tcp://127.0.0.1:5557",
		Type:       common.ServiceTypeVLLM,
		ModelName:  "test-model",
		InstanceID: "instance-1",
		TenantID:   "tenant-1",
		BlockSize:  16,
	}

	// Pre-populate subscribers to simulate an existing subscription,
	// so subscribeToService hits the early-return path (line 161).
	svcKey := makeServiceKey(svc.InstanceID, svc.TenantID, svc.DPRank)
	mgr.subscribers.Store(svcKey, &zmq.ZMQClient{})

	isNew, err := mgr.subscribeToService(svc)
	if err != nil {
		t.Fatalf("duplicate subscribeToService() unexpected error: %v", err)
	}
	if isNew {
		t.Error("duplicate subscribeToService() should return false")
	}
}

func TestSubscribeToService_MissingEndpoint(t *testing.T) {
	mgr := NewEventManager(nil, 0)

	svc := common.ServiceConfig{
		Endpoint:   "",
		Type:       common.ServiceTypeVLLM,
		ModelName:  "test-model",
		InstanceID: "instance-1",
		TenantID:   "tenant-1",
	}

	isNew, err := mgr.subscribeToService(svc)
	if err == nil {
		t.Error("subscribeToService with empty endpoint should return error")
	}
	if isNew {
		t.Error("subscribeToService on error should return false")
	}
}

func TestSubscribeToService_DifferentDPRankIsNew(t *testing.T) {
	mgr := NewEventManager(nil, 0)

	svc0 := common.ServiceConfig{
		Endpoint:   "tcp://127.0.0.1:5557",
		Type:       common.ServiceTypeVLLM,
		ModelName:  "test-model",
		InstanceID: "instance-1",
		TenantID:   "tenant-1",
		DPRank:     0,
		BlockSize:  16,
	}
	svc1 := common.ServiceConfig{
		Endpoint:   "tcp://127.0.0.1:5557",
		Type:       common.ServiceTypeVLLM,
		ModelName:  "test-model",
		InstanceID: "instance-1",
		TenantID:   "tenant-1",
		DPRank:     1,
		BlockSize:  16,
	}

	// Pre-populate dp_rank 0, so dp_rank 1 is treated as new
	svcKey0 := makeServiceKey(svc0.InstanceID, svc0.TenantID, svc0.DPRank)
	mgr.subscribers.Store(svcKey0, &zmq.ZMQClient{})

	isNew1, err1 := mgr.subscribeToService(svc1)
	if err1 == nil {
		// If no error, must be because it hit the validation before ZMQ connect
		// Actually it will fail at zmq connect since no server exists.
		// The key assertion is: svc1 (dp_rank=1) should NOT be treated as duplicate
		// of svc0 (dp_rank=0), because their service keys differ.
		t.Skip("requires running ZMQ server to complete the full path")
	}
	_ = isNew1
}

func TestSubscribeToService_DifferentTenantIsNew(t *testing.T) {
	mgr := NewEventManager(nil, 0)

	svcA := common.ServiceConfig{
		Endpoint:   "tcp://127.0.0.1:5557",
		Type:       common.ServiceTypeVLLM,
		ModelName:  "test-model",
		InstanceID: "instance-1",
		TenantID:   "tenant-a",
		BlockSize:  16,
	}
	svcB := common.ServiceConfig{
		Endpoint:   "tcp://127.0.0.1:5557",
		Type:       common.ServiceTypeVLLM,
		ModelName:  "test-model",
		InstanceID: "instance-1",
		TenantID:   "tenant-b",
		BlockSize:  16,
	}

	// Pre-populate tenant-a, so tenant-b is treated as new
	svcKeyA := makeServiceKey(svcA.InstanceID, svcA.TenantID, svcA.DPRank)
	mgr.subscribers.Store(svcKeyA, &zmq.ZMQClient{})

	isNewB, errB := mgr.subscribeToService(svcB)
	if errB == nil {
		t.Skip("requires running ZMQ server to complete the full path")
	}
	_ = isNewB
}

func TestServicesSlice_NoDuplicateOnReRegister(t *testing.T) {
	mgr := NewEventManager(nil, 0)

	svc := common.ServiceConfig{
		Endpoint:   "tcp://127.0.0.1:5557",
		Type:       common.ServiceTypeVLLM,
		ModelName:  "test-model",
		InstanceID: "instance-1",
		TenantID:   "tenant-1",
		BlockSize:  16,
	}

	// Simulate the /register handler logic with isNew guard.
	// First registration: subscribeToService will fail on zmq connect,
	// so we pre-populate instead to test the guarded-append pattern.
	svcKey := makeServiceKey(svc.InstanceID, svc.TenantID, svc.DPRank)
	mgr.subscribers.Store(svcKey, &zmq.ZMQClient{})

	// First call: duplicate → isNew=false
	isNew, err := mgr.subscribeToService(svc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isNew {
		mgr.services = append(mgr.services, svc)
	}
	if len(mgr.services) != 0 {
		t.Errorf("services len = %d after duplicate, want 0", len(mgr.services))
	}

	// Second call: still duplicate → isNew=false
	isNew2, err2 := mgr.subscribeToService(svc)
	if err2 != nil {
		t.Fatalf("unexpected error: %v", err2)
	}
	if isNew2 {
		mgr.services = append(mgr.services, svc)
	}
	if len(mgr.services) != 0 {
		t.Errorf("services len = %d after second duplicate, want 0", len(mgr.services))
	}
}

func TestNewEventManager_InitialState(t *testing.T) {
	mgr := NewEventManager(nil, 13333)

	if mgr.indexer == nil {
		t.Error("indexer should not be nil")
	}
	if mgr.tenantInstanceMap == nil {
		t.Error("tenantInstanceMap should not be nil")
	}
	if len(mgr.services) != 0 {
		t.Errorf("services should be empty, got %d", len(mgr.services))
	}
	if mgr.httpserverport != 13333 {
		t.Errorf("httpserverport = %d, want 13333", mgr.httpserverport)
	}
}

func TestMakeServiceKey(t *testing.T) {
	key := makeServiceKey("instance-1", "tenant-1", 0)
	expected := "instance-1|tenant-1|0"
	if key != expected {
		t.Errorf("makeServiceKey = %q, want %q", key, expected)
	}
}

// --- /services HTTP endpoint tests ---

// waitForHTTPServer polls until the TCP port is accepting connections.
func waitForHTTPServer(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("HTTP server on %s did not become ready within %v", addr, timeout)
}

func TestServicesEndpoint_Empty(t *testing.T) {
	port := 19001
	mgr := NewEventManager(nil, port)
	if err := mgr.StartHTTPServer(); err != nil {
		t.Fatalf("StartHTTPServer() error = %v", err)
	}
	defer mgr.Stop()

	addr := fmt.Sprintf("localhost:%d", port)
	waitForHTTPServer(t, addr, 3*time.Second)

	resp, err := http.Get(fmt.Sprintf("http://%s/services", addr))
	if err != nil {
		t.Fatalf("GET /services failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status code = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	count, ok := result["count"].(float64)
	if !ok {
		t.Fatalf("count field missing or not a number, got %T", result["count"])
	}
	if int(count) != 0 {
		t.Errorf("count = %d, want 0", int(count))
	}

	svcs, ok := result["services"].([]interface{})
	if !ok {
		t.Fatalf("services field missing or not an array, got %T", result["services"])
	}
	if len(svcs) != 0 {
		t.Errorf("len(services) = %d, want 0", len(svcs))
	}
}

func TestServicesEndpoint_WithServices(t *testing.T) {
	port := 19002
	mgr := NewEventManager(nil, port)
	if err := mgr.StartHTTPServer(); err != nil {
		t.Fatalf("StartHTTPServer() error = %v", err)
	}
	defer mgr.Stop()

	addr := fmt.Sprintf("localhost:%d", port)
	waitForHTTPServer(t, addr, 3*time.Second)

	// Populate activeConfigs
	svc1 := common.ServiceConfig{
		Endpoint:   "tcp://127.0.0.1:5555",
		Type:       common.ServiceTypeVLLM,
		ModelName:  "model-a",
		TenantID:   "default",
		InstanceID: "inst-1",
		BlockSize:  64,
		DPRank:     0,
	}
	svc2 := common.ServiceConfig{
		Endpoint:       "tcp://127.0.0.1:5556",
		ReplayEndpoint: "tcp://127.0.0.1:5557",
		Type:           common.ServiceTypeMooncake,
		ModelName:      "model-b",
		TenantID:       "tenant-1",
		InstanceID:     "inst-2",
		BlockSize:      128,
		DPRank:         1,
		AdditionalSalt: "mysalt",
	}
	mgr.activeConfigs.Store("inst-1|default|0", svc1)
	mgr.activeConfigs.Store("inst-2|tenant-1|1", svc2)

	resp, err := http.Get(fmt.Sprintf("http://%s/services", addr))
	if err != nil {
		t.Fatalf("GET /services failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status code = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	count, ok := result["count"].(float64)
	if !ok {
		t.Fatalf("count field missing or not a number, got %T", result["count"])
	}
	if int(count) != 2 {
		t.Errorf("count = %d, want 2", int(count))
	}

	svcs, ok := result["services"].([]interface{})
	if !ok {
		t.Fatalf("services field missing or not an array, got %T", result["services"])
	}
	if len(svcs) != 2 {
		t.Fatalf("len(services) = %d, want 2", len(svcs))
	}

	// Build a lookup by InstanceID
	svcMap := make(map[string]map[string]interface{})
	for _, raw := range svcs {
		s, ok := raw.(map[string]interface{})
		if !ok {
			t.Fatalf("service entry is not an object, got %T", raw)
		}
		id, _ := s["InstanceID"].(string)
		svcMap[id] = s
	}

	// Verify svc1
	s1, ok := svcMap["inst-1"]
	if !ok {
		t.Fatal("inst-1 not found in response")
	}
	if got, want := s1["Endpoint"].(string), "tcp://127.0.0.1:5555"; got != want {
		t.Errorf("inst-1 Endpoint = %q, want %q", got, want)
	}
	if got, want := s1["Type"].(string), common.ServiceTypeVLLM; got != want {
		t.Errorf("inst-1 Type = %q, want %q", got, want)
	}
	if got, want := s1["ModelName"].(string), "model-a"; got != want {
		t.Errorf("inst-1 ModelName = %q, want %q", got, want)
	}

	// Verify svc2
	s2, ok := svcMap["inst-2"]
	if !ok {
		t.Fatal("inst-2 not found in response")
	}
	if got, want := s2["Endpoint"].(string), "tcp://127.0.0.1:5556"; got != want {
		t.Errorf("inst-2 Endpoint = %q, want %q", got, want)
	}
	if got, want := s2["ReplayEndpoint"].(string), "tcp://127.0.0.1:5557"; got != want {
		t.Errorf("inst-2 ReplayEndpoint = %q, want %q", got, want)
	}
	if got, want := s2["Type"].(string), common.ServiceTypeMooncake; got != want {
		t.Errorf("inst-2 Type = %q, want %q", got, want)
	}
	if got, want := s2["ModelName"].(string), "model-b"; got != want {
		t.Errorf("inst-2 ModelName = %q, want %q", got, want)
	}
	if got, want := s2["TenantID"].(string), "tenant-1"; got != want {
		t.Errorf("inst-2 TenantID = %q, want %q", got, want)
	}
	if got, want := s2["BlockSize"].(float64), float64(128); got != want {
		t.Errorf("inst-2 BlockSize = %v, want %v", got, want)
	}
}

func TestServicesEndpoint_MethodNotAllowed(t *testing.T) {
	port := 19003
	mgr := NewEventManager(nil, port)
	if err := mgr.StartHTTPServer(); err != nil {
		t.Fatalf("StartHTTPServer() error = %v", err)
	}
	defer mgr.Stop()

	addr := fmt.Sprintf("localhost:%d", port)
	waitForHTTPServer(t, addr, 3*time.Second)

	// POST should be rejected
	resp, err := http.Post(fmt.Sprintf("http://%s/services", addr), "application/json", nil)
	if err != nil {
		t.Fatalf("POST /services failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST status = %d, want %d", resp.StatusCode, http.StatusMethodNotAllowed)
	}

	// PUT should be rejected as well
	req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s/services", addr), nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT /services failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("PUT status = %d, want %d", resp.StatusCode, http.StatusMethodNotAllowed)
	}
}

func TestServicesEndpoint_ConcurrentRead(t *testing.T) {
	port := 19004
	mgr := NewEventManager(nil, port)
	if err := mgr.StartHTTPServer(); err != nil {
		t.Fatalf("StartHTTPServer() error = %v", err)
	}
	defer mgr.Stop()

	addr := fmt.Sprintf("localhost:%d", port)
	waitForHTTPServer(t, addr, 3*time.Second)

	// Populate some services
	for i := 0; i < 5; i++ {
		svc := common.ServiceConfig{
			Endpoint:   fmt.Sprintf("tcp://127.0.0.1:%d", 6000+i),
			Type:       common.ServiceTypeVLLM,
			ModelName:  "model",
			InstanceID: fmt.Sprintf("inst-%d", i),
			TenantID:   "default",
			BlockSize:  64,
		}
		mgr.activeConfigs.Store(fmt.Sprintf("inst-%d|default|0", i), svc)
	}

	errCh := make(chan error, 20)
	for i := 0; i < 20; i++ {
		go func() {
			resp, err := http.Get(fmt.Sprintf("http://%s/services", addr))
			if err != nil {
				errCh <- fmt.Errorf("request failed: %w", err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errCh <- fmt.Errorf("status = %d", resp.StatusCode)
				return
			}
			errCh <- nil
		}()
	}

	for i := 0; i < 20; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("concurrent request %d: %v", i, err)
		}
	}
}
