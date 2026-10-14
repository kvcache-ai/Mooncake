//go:build integration

// SPDX-License-Identifier: Apache-2.0
package tests

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	conductor "github.com/kvcache-ai/Mooncake/mooncake-conductor/go/mooncakeconductor"
)

func control(t *testing.T, path string, body any, out any) {
	t.Helper()
	url := os.Getenv("CONDUCTOR_EVENT_HARNESS") + path
	var resp *http.Response
	var err error
	httpClient := &http.Client{Timeout: 8 * time.Second}
	if body == nil {
		resp, err = httpClient.Get(url)
	} else {
		data, e := json.Marshal(body)
		if e != nil {
			t.Fatal(e)
		}
		resp, err = httpClient.Post(url, "application/json", bytes.NewReader(data))
	}
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("harness %s: %d %s", path, resp.StatusCode, data)
	}
	if out != nil {
		if err = json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEventsToGoQuery(t *testing.T) {
	if os.Getenv("CONDUCTOR_EVENT_HARNESS") == "" || os.Getenv("CONDUCTOR_RPC_ADDR") == "" {
		t.Fatal("start event_harness.py and set CONDUCTOR_EVENT_HARNESS/CONDUCTOR_RPC_ADDR")
	}
	var fixtures struct {
		Services []conductor.ServiceConfig `json:"services"`
		SG       []int32                   `json:"sglang_tokens"`
		VL       []int32                   `json:"vllm_tokens"`
	}
	control(t, "/", nil, &fixtures)
	c, err := conductor.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	addr := os.Getenv("CONDUCTOR_RPC_ADDR")
	if err = c.Setup(conductor.ClientConfig{Address: addr}); err != nil {
		t.Fatal(err)
	}
	for _, svc := range fixtures.Services {
		r, err := c.Register(svc)
		if err != nil || !r.IsNew || r.InstanceID != svc.InstanceID {
			t.Fatalf("register: %+v %v", r, err)
		}
		t.Cleanup(func() { c.Unregister(svc.InstanceID, svc.TenantID, svc.DPRank) })
		r, err = c.Register(svc)
		if err != nil || r.IsNew {
			t.Fatalf("idempotency: %+v %v", r, err)
		}
	}
	conflict := fixtures.Services[0]
	conflict.BlockSize = 8
	if _, err = c.Register(conflict); !errors.Is(err, &conductor.Error{Code: conductor.InvalidParams}) {
		t.Fatal(err)
	}
	if status, err := c.HealthCheck(); status != conductor.Healthy || err != nil {
		t.Fatalf("health %v %v", status, err)
	}
	svcs, err := c.ListServices()
	if err != nil || len(svcs) != 4 {
		t.Fatalf("services %v %v", svcs, err)
	}
	for _, s := range svcs {
		if s.TenantID != "default" || len(s.RootDigest) != 64 && s.PublisherType == conductor.PublisherVLLM {
			t.Fatalf("unresolved service %+v", s)
		}
	}
	query := conductor.QueryRequest{ModelName: "go-sg", BlockSize: 4, TokenIDs: fixtures.SG}
	for _, invalid := range []conductor.QueryRequest{{BlockSize: 4}, {ModelName: "go-sg"}} {
		if _, e := c.Query(invalid); !errors.Is(e, &conductor.Error{Code: conductor.InvalidParams}) {
			t.Fatal(e)
		}
	}
	if missing, e := c.Query(conductor.QueryRequest{ModelName: "unregistered", BlockSize: 4}); e != nil || len(missing.Instances) != 0 {
		t.Fatalf("missing context %+v %v", missing, e)
	}
	empty := query
	empty.TokenIDs = nil
	if result, e := c.Query(empty); e != nil {
		t.Fatal(e)
	} else {
		for _, hit := range result.Instances {
			if hit.LongestMatched != 0 {
				t.Fatal(hit)
			}
		}
	}
	publish := func(name, action string) {
		control(t, "/publish", map[string]string{"name": name, "action": action}, nil)
	}
	wait := func(req conductor.QueryRequest, want map[string]conductor.TierMatch) conductor.QueryResult {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			r, e := c.Query(req)
			if e != nil {
				t.Fatal(e)
			}
			ok := len(r.Instances) == len(want)
			for id, m := range want {
				h, exists := r.Instances[id]
				ok = ok && exists && h.TierMatch == m && h.LongestMatched == m.Disk
			}
			if ok {
				return r
			}
			if time.Now().After(deadline) {
				t.Fatalf("want %v got %+v", want, r)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	for _, name := range []string{"sg0", "sg12", "vl0"} {
		publish(name, "seed")
	}
	initial := conductor.TierMatch{Device: 4, CPULocal: 8, CPUShare: 8, Disk: 12}
	wait(query, map[string]conductor.TierMatch{"sg0": initial, "sg12": initial})
	vlQuery := conductor.QueryRequest{ModelName: "go-vl", BlockSize: 4, TokenIDs: fixtures.VL}
	wait(vlQuery, map[string]conductor.TierMatch{"vl0": {Device: 8, CPULocal: 8, CPUShare: 8, Disk: 8}})
	publish("pool", "seed")
	shared := conductor.TierMatch{Device: 4, CPULocal: 8, CPUShare: 16, Disk: 16}
	r := wait(query, map[string]conductor.TierMatch{"sg0": shared, "sg12": shared})
	for id, rank := range map[string]int64{"sg0": 0, "sg12": 12} {
		h := r.Instances[id]
		if !reflect.DeepEqual(h.DP, map[int64]int64{rank: 4}) || !reflect.DeepEqual(h.RankMatches, map[int64]conductor.TierMatch{rank: shared}) {
			t.Fatalf("rank %+v", h)
		}
	}
	var httpResult conductor.QueryResult
	control(t, "/query", map[string]any{"model": "go-sg", "block_size": 4, "token_ids": fixtures.SG}, &httpResult)
	if !reflect.DeepEqual(r, httpResult) {
		t.Fatalf("Go %+v HTTP %+v", r, httpResult)
	}
	view, err := c.GetGlobalView()
	if err != nil || view.ContextCount != 2 || len(view.Contexts) != 2 {
		t.Fatalf("global %+v %v", view, err)
	}
	for _, v := range view.Contexts {
		if v.ModelName == "go-sg" && (v.PrefixCount != 4 || !reflect.DeepEqual(v.Instances, map[string][]int64{"sg0": {0}, "sg12": {12}})) {
			t.Fatalf("context %+v", v)
		}
	}
	filtered := query
	filtered.InstanceFilter = "sg12"
	wait(filtered, map[string]conductor.TierMatch{"sg12": shared})
	publish("sg0", "remove")
	withoutDevice := shared
	withoutDevice.Device = 0
	wait(query, map[string]conductor.TierMatch{"sg0": withoutDevice, "sg12": shared})
	publish("sg0", "clear")
	sharedOnly := conductor.TierMatch{CPUShare: 16, Disk: 16}
	wait(query, map[string]conductor.TierMatch{"sg0": sharedOnly, "sg12": shared})
	publish("pool", "remove")
	removed := shared
	removed.CPUShare = 12
	wait(query, map[string]conductor.TierMatch{"sg0": {CPUShare: 12, Disk: 16}, "sg12": removed})
	publish("pool", "clear")
	wait(query, map[string]conductor.TierMatch{"sg0": {}, "sg12": initial})
	publish("vl0", "remove")
	wait(vlQuery, map[string]conductor.TierMatch{"vl0": {}})
	publish("vl0", "seed")
	publish("vl0", "clear")
	wait(vlQuery, map[string]conductor.TierMatch{"vl0": {}})
	// Go result ownership survives native result frees and connection close.
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	if r.Instances["sg12"].RankMatches[12] != shared || len(view.Contexts) != 2 {
		t.Fatal("borrowed output memory")
	}
	if err = c.Setup(conductor.ClientConfig{Address: addr}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		config conductor.ClientConfig
		code   conductor.ErrorCode
	}{
		{conductor.ClientConfig{Address: "invalid"}, conductor.InvalidParams},
		{conductor.ClientConfig{Address: addr, ConnectTimeout: -1}, conductor.InvalidParams},
		{conductor.ClientConfig{Address: addr, RequestTimeout: -1}, conductor.InvalidParams},
		{conductor.ClientConfig{Address: addr, RequestTimeout: time.Duration(1<<63 - 1)}, conductor.InvalidParams},
		{conductor.ClientConfig{Address: "127.0.0.1:0"}, conductor.RPCFail},
	} {
		if err = c.Setup(tc.config); !errors.Is(err, &conductor.Error{Code: tc.code}) {
			t.Fatal(err)
		}
		if status, e := c.HealthCheck(); status != conductor.NotInitialized || e != nil {
			t.Fatalf("failed setup health %v %v", status, e)
		}
		if _, e := c.ListServices(); !errors.Is(e, &conductor.Error{Code: conductor.ConductorUnavailable}) {
			t.Fatal(e)
		}
		if err = c.Setup(conductor.ClientConfig{Address: addr}); err != nil {
			t.Fatal(err)
		}
	}
	for _, svc := range fixtures.Services {
		u, e := c.Unregister(svc.InstanceID, svc.TenantID, svc.DPRank)
		if e != nil || u.RemovedKey == "" {
			t.Fatalf("unregister %+v %v", u, e)
		}
		if _, e = c.Unregister(svc.InstanceID, svc.TenantID, svc.DPRank); !errors.Is(e, &conductor.Error{Code: conductor.ServiceNotFound}) {
			t.Fatal(e)
		}
	}
	if v, e := c.GetGlobalView(); e != nil {
		t.Fatal(e)
	} else {
		// Context hash recipes remain after the last instance is unregistered.
		for _, ctx := range v.Contexts {
			if ctx.PrefixCount != 0 || len(ctx.Instances) != 0 {
				t.Fatalf("cleanup %+v", ctx)
			}
		}
	}
	if s, e := c.ListServices(); e != nil || len(s) != 0 {
		t.Fatalf("cleanup %+v %v", s, e)
	}
	// Exercise successful RPC calls racing with Close, not just nil handles.
	var wg sync.WaitGroup
	ready := make(chan struct{}, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, e := c.Query(query); e != nil {
				t.Error(e)
			}
			ready <- struct{}{}
			for j := 0; j < 20; j++ {
				if _, e := c.Query(query); e != nil && !errors.Is(e, &conductor.Error{Code: conductor.ConductorUnavailable}) {
					t.Error(e)
				}
			}
		}()
	}
	for i := 0; i < 6; i++ {
		<-ready
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	fmt.Println("validated vLLM/SGLang/Mooncake events, four tiers, ranks, HTTP equivalence and client lifecycle")
}
