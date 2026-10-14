// SPDX-License-Identifier: Apache-2.0
package mooncakeconductor

import (
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestTimeoutMillis(t *testing.T) {
	for _, tc := range []struct {
		input   time.Duration
		want    int64
		invalid bool
	}{
		{0, 1000, false}, {time.Nanosecond, 1, false}, {1500 * time.Microsecond, 2, false},
		{time.Second, 1000, false}, {-1, 0, true}, {time.Duration(1<<63 - 1), 0, true},
	} {
		got, err := timeoutMillis(tc.input, time.Second)
		if got != tc.want || (err != nil) != tc.invalid {
			t.Fatalf("%v: got %d, %v", tc.input, got, err)
		}
	}
}

func TestTypedErrorAndQueryJSON(t *testing.T) {
	err := nativeError("query", -901)
	if !errors.Is(err, &Error{Code: RPCTimeout}) {
		t.Fatal(err)
	}
	var native *Error
	if !errors.As(err, &native) || native.Op != "query" {
		t.Fatal(err)
	}
	if errors.Is(err, &Error{Code: RPCFail}) || nativeError("query", 0) != nil {
		t.Fatal("incorrect error mapping")
	}
	if unknown := nativeError("query", -4321); !errors.As(unknown, &native) || native.Code != -4321 {
		t.Fatal(unknown)
	}
	out := QueryResult{Instances: map[string]CacheHit{"engine": {
		TierMatch: TierMatch{1, 2, 3, 4}, LongestMatched: 4,
		DP: map[int64]int64{12: 1}, RankMatches: map[int64]TierMatch{12: {1, 2, 3, 4}},
	}}}
	data, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	var raw struct {
		Instances map[string]map[string]json.RawMessage `json:"instances"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	keys := raw.Instances["engine"]
	for _, key := range []string{"device", "cpu_local", "cpu_share", "disk", "longest_matched", "dp", "rank_matches"} {
		if _, ok := keys[key]; !ok {
			t.Fatalf("missing %s in %s", key, data)
		}
	}
	if len(keys) != 7 {
		t.Fatalf("extra aliases: %s", data)
	}
	var roundtrip QueryResult
	if err := json.Unmarshal(data, &roundtrip); err != nil || !reflect.DeepEqual(out, roundtrip) {
		t.Fatalf("%s: %v", data, err)
	}
}

func TestLifecycleAndConcurrentClose(t *testing.T) {
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if status, err := c.HealthCheck(); status != NotInitialized || err != nil {
		t.Fatalf("%v %v", status, err)
	}
	if _, err := c.Query(QueryRequest{ModelName: "model", BlockSize: 16}); !errors.Is(err, &Error{Code: ConductorUnavailable}) {
		t.Fatal(err)
	}
	if err := c.Setup(ClientConfig{Address: "invalid"}); !errors.Is(err, &Error{Code: InvalidParams}) {
		t.Fatal(err)
	}
	if err := c.Setup(ClientConfig{Address: "invalid", RequestTimeout: -1}); !errors.Is(err, &Error{Code: InvalidParams}) {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				c.HealthCheck()
				if _, err := c.ListServices(); !errors.Is(err, &Error{Code: ConductorUnavailable}) {
					t.Error(err)
				}
				if err := c.Close(); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	if err := c.Setup(ClientConfig{Address: "invalid"}); !errors.Is(err, &Error{Code: InvalidParams}) {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}
