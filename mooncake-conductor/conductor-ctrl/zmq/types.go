package zmq

import (
	"fmt"
	"net"
	"time"
)

// EventHandler processes received KV events
type EventHandler interface {
	HandleEvent(event KVEvent) error
}

// ZMQClientConfig contains configuration for the ZMQ client
type ZMQClientConfig struct {
	CachePoolKey   string
	ServiceIP      string
	ModelName      string
	Port           int
	RouterPort     int
	PollTimeout    time.Duration
	ReplayTimeout  time.Duration
	ReconnectDelay time.Duration
}

const (
	DefaultPubPort    = 5557
	DefaultRouterPort = 5558

	// Timeouts and intervals
	DefaultPollTimeout       = 100 * time.Millisecond
	DefaultReplayTimeout     = 5 * time.Second
	DefaultReconnectInterval = 1 * time.Second
	MaxReconnectInterval     = 30 * time.Second
	ReconnectBackoffFactor   = 2.0

	EventChannelBufferSize = 1000
)

func ValidateConfig(config *ZMQClientConfig) error {
	if config.ServiceIP == "" {
		return fmt.Errorf("publisher IP is required")
	}

	if ip := net.ParseIP(config.ServiceIP); ip == nil {
		return fmt.Errorf("invalid IP address: %s", config.ServiceIP)
	}

	if config.Port <= 0 || config.Port > 65535 {
		return fmt.Errorf("invalid publisher port: %d", config.Port)
	}

	if config.RouterPort <= 0 || config.RouterPort > 65535 {
		return fmt.Errorf("invalid router port: %d", config.RouterPort)
	}

	return nil
}
