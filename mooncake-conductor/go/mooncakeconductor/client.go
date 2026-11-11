// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
// Package mooncakeconductor provides a synchronous Conductor RPC client.
package mooncakeconductor

import (
	"sync"
	"time"
)

// Client serializes operations on one RPC connection. Use separate clients for
// parallel requests. Do not copy a Client after first use. Close it explicitly.
type Client struct {
	mu     sync.Mutex
	native *nativeClient
}

func New() (*Client, error) {
	n, err := newNative()
	if err != nil {
		return nil, err
	}
	return &Client{native: n}, nil
}

func timeoutMillis(value, fallback time.Duration) (int64, error) {
	// Rounded milliseconds must remain representable as native nanoseconds.
	if value < 0 || value > time.Duration((1<<63-1)/int64(time.Millisecond))*time.Millisecond {
		return 0, &Error{Op: "setup", Code: InvalidParams}
	}
	if value == 0 {
		value = fallback
	}
	ms := int64(value / time.Millisecond)
	if value%time.Millisecond != 0 {
		ms++
	}
	return ms, nil
}

// Setup replaces any previous connection, including on failure. Zero timeouts
// use 1s/3s defaults. After Close, Setup creates a fresh native client.
func (c *Client) Setup(config ClientConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	connect, err := timeoutMillis(config.ConnectTimeout, time.Second)
	var request int64
	if err == nil {
		request, err = timeoutMillis(config.RequestTimeout, 3*time.Second)
	}
	if err != nil {
		// Native Setup closes the old connection on valid Go input. Validation
		// failures must also leave the previous connection unavailable.
		if c.native != nil {
			if closeErr := c.native.disconnect(); closeErr != nil {
				return closeErr
			}
		}
		return err
	}
	if c.native == nil {
		c.native, err = newNative()
		if err != nil {
			return err
		}
	}
	return c.native.setup(config.Address, connect, request)
}

// Close releases client resources only; it does not unregister any publisher.
// It waits for an in-flight operation and is idempotent.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.native == nil {
		return nil
	}
	err := c.native.close()
	c.native = nil
	return err
}

// HealthCheck probes RPC reachability, not event progress or engine health.
func (c *Client) HealthCheck() (HealthStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.native == nil {
		return NotInitialized, nil
	}
	return c.native.health()
}

func unavailable(op string) error { return &Error{Op: op, Code: ConductorUnavailable} }

func (c *Client) Register(config ServiceConfig) (RegisterResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.native == nil {
		return RegisterResult{}, unavailable("register")
	}
	return c.native.register(config)
}

func (c *Client) Unregister(instanceID, tenantID string, dpRank int32) (UnregisterResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.native == nil {
		return UnregisterResult{}, unavailable("unregister")
	}
	return c.native.unregister(instanceID, tenantID, dpRank)
}

// Query sends raw tokens. Do not mutate TokenIDs during the call. RequestTimeout
// applies to the native RPC, not time waiting for another operation's mutex.
func (c *Client) Query(request QueryRequest) (QueryResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.native == nil {
		return QueryResult{}, unavailable("query")
	}
	return c.native.query(request)
}

func (c *Client) GetGlobalView() (GlobalView, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.native == nil {
		return GlobalView{}, unavailable("get_global_view")
	}
	return c.native.global()
}

func (c *Client) ListServices() ([]RegisteredService, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.native == nil {
		return nil, unavailable("list_services")
	}
	return c.native.list()
}
