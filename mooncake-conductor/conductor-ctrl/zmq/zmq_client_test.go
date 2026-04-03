package zmq_test

import (
	"testing"
	"time"

	"conductor/zmq"
)

func TestZMQClientConfig_Validate_Success(t *testing.T) {
	config := &zmq.ZMQClientConfig{
		CachePoolKey:   "test-pod",
		Endpoint:       "tcp://127.0.0.1:5557",
		ReplayEndpoint: "tcp://127.0.0.1:5558",
		ModelName:      "test-model",
		PollTimeout:    100 * time.Millisecond,
		ReplayTimeout:  5 * time.Second,
		ReconnectDelay: 1 * time.Second,
	}

	err := zmq.ValidateConfig(config)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestZMQClientConfig_Validate_MissingEndpoint(t *testing.T) {
	config := &zmq.ZMQClientConfig{
		CachePoolKey: "test-pod",
		// Missing Endpoint
	}

	err := zmq.ValidateConfig(config)
	if err == nil {
		t.Error("Expected error for missing endpoint, got nil")
	}
}

func TestZMQClient_NewZMQClient(t *testing.T) {
	config := &zmq.ZMQClientConfig{
		CachePoolKey:   "test-pod",
		Endpoint:       "tcp://127.0.0.1:5557",
		ReconnectDelay: 1 * time.Second,
	}

	handler := &mockHandler{}
	client := zmq.NewZMQClient(config, handler)

	if client == nil {
		t.Fatal("Expected non-nil client")
	}
}

type mockHandler struct{}

func (m *mockHandler) HandleEvent(event zmq.KVEvent, dpRank int64) error {
	return nil
}

func TestZMQClient_Start_InvalidEndpoint(t *testing.T) {
	config := &zmq.ZMQClientConfig{
		CachePoolKey:   "test-pod",
		Endpoint:       "tcp://invalid:9999",
		ReconnectDelay: 10 * time.Millisecond,
	}

	handler := &mockHandler{}
	client := zmq.NewZMQClient(config, handler)

	err := client.Start()
	// Should fail due to invalid endpoint
	// We just verify it doesn't panic
	_ = err

	client.Stop()
}

func TestZMQClient_Stop_WithoutStart(t *testing.T) {
	config := &zmq.ZMQClientConfig{
		CachePoolKey:   "test-pod",
		Endpoint:       "tcp://127.0.0.1:5557",
		ReconnectDelay: 1 * time.Second,
	}

	handler := &mockHandler{}
	client := zmq.NewZMQClient(config, handler)

	// Stop without Start should not panic
	client.Stop()
}