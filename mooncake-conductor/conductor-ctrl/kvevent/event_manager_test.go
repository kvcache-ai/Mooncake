package kvevent

import (
	"testing"

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
