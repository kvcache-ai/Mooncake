// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prefix

import (
	"strings"
	"testing"
)

func seq(n int) []int32 {
	t := make([]int32, n)
	for i := range t {
		t[i] = int32(i + 1)
	}
	return t
}

func TestPagesInvalidPageSize(t *testing.T) {
	c := Config{PageSize: 0, Model: "m"}
	if _, err := c.Pages(seq(8)); err != ErrInvalidPageSize {
		t.Fatalf("expected ErrInvalidPageSize, got %v", err)
	}
}

func TestPagesDropsPartialTrailingPage(t *testing.T) {
	c := Config{PageSize: 4, Model: "m"}
	pages, err := c.Pages(seq(10)) // 2 full pages + 2 leftover tokens
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) != 2 {
		t.Fatalf("expected 2 full pages, got %d", len(pages))
	}
	if pages[0].StartToken != 0 || pages[0].EndToken != 4 {
		t.Errorf("page0 range = [%d,%d), want [0,4)", pages[0].StartToken, pages[0].EndToken)
	}
	if pages[1].StartToken != 4 || pages[1].EndToken != 8 {
		t.Errorf("page1 range = [%d,%d), want [4,8)", pages[1].StartToken, pages[1].EndToken)
	}
}

func TestPagesDeterministic(t *testing.T) {
	c := Config{PageSize: 4, Model: "qwen2.5:7b"}
	a, err := c.Pages(seq(16))
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Pages(seq(16))
	if err != nil {
		t.Fatal(err)
	}
	for i := range a {
		if a[i].Key != b[i].Key {
			t.Fatalf("page %d key not deterministic: %q vs %q", i, a[i].Key, b[i].Key)
		}
	}
}

func TestSharedPrefixProducesIdenticalKeys(t *testing.T) {
	c := Config{PageSize: 4, Model: "m"}

	base := seq(16)
	// Same first 12 tokens (3 pages), different tail.
	other := append([]int32(nil), base[:12]...)
	other = append(other, 900, 901, 902, 903)

	pa, _ := c.Pages(base)
	pb, _ := c.Pages(other)

	for i := 0; i < 3; i++ {
		if pa[i].Key != pb[i].Key {
			t.Fatalf("shared page %d should match: %q vs %q", i, pa[i].Key, pb[i].Key)
		}
	}
	if pa[3].Key == pb[3].Key {
		t.Fatal("diverging page 3 must not share a key")
	}
}

func TestEarlyDivergenceChangesAllSubsequentKeys(t *testing.T) {
	c := Config{PageSize: 4, Model: "m"}

	base := seq(16)
	changed := append([]int32(nil), base...)
	changed[1] = 12345 // flip one token inside page 0

	pa, _ := c.Pages(base)
	pb, _ := c.Pages(changed)

	if pa[0].Key == pb[0].Key {
		t.Fatal("page 0 must change when a token in it changes")
	}
	for i := 1; i < len(pa); i++ {
		if pa[i].Key == pb[i].Key {
			t.Fatalf("page %d must change after an earlier divergence", i)
		}
	}
}

func TestModelIsolation(t *testing.T) {
	tokens := seq(16)
	ka, _ := Config{PageSize: 4, Model: "llama3:8b"}.Keys(tokens)
	kb, _ := Config{PageSize: 4, Model: "qwen2.5:7b"}.Keys(tokens)
	for i := range ka {
		if ka[i] == kb[i] {
			t.Fatalf("different models must not share key at page %d", i)
		}
	}
}

func TestKeyFormat(t *testing.T) {
	c := Config{PageSize: 4, Model: "m"}
	pages, _ := c.Pages(seq(4))
	key := pages[0].Key
	if !strings.HasPrefix(key, DefaultKeyPrefix+"/m/") {
		t.Fatalf("unexpected key prefix: %q", key)
	}
	hexPart := key[len(DefaultKeyPrefix+"/m/"):]
	if len(hexPart) != keyHashBytes*2 {
		t.Fatalf("hex part length = %d, want %d", len(hexPart), keyHashBytes*2)
	}
}

func TestCustomKeyPrefix(t *testing.T) {
	c := Config{PageSize: 4, Model: "m", KeyPrefix: "custom/ns"}
	pages, _ := c.Pages(seq(4))
	if !strings.HasPrefix(pages[0].Key, "custom/ns/m/") {
		t.Fatalf("custom prefix not applied: %q", pages[0].Key)
	}
}

func TestNumFullPages(t *testing.T) {
	cases := []struct{ n, ps, want int }{
		{0, 4, 0},
		{3, 4, 0},
		{4, 4, 1},
		{9, 4, 2},
		{16, 4, 4},
		{10, 0, 0},
	}
	for _, tc := range cases {
		if got := NumFullPages(tc.n, tc.ps); got != tc.want {
			t.Errorf("NumFullPages(%d,%d) = %d, want %d", tc.n, tc.ps, got, tc.want)
		}
	}
}

func TestLongestPresentRun(t *testing.T) {
	cases := []struct {
		present []bool
		want    int
	}{
		{nil, 0},
		{[]bool{}, 0},
		{[]bool{false, true, true}, 0},
		{[]bool{true, false, true}, 1},
		{[]bool{true, true, false}, 2},
		{[]bool{true, true, true}, 3},
	}
	for _, tc := range cases {
		if got := LongestPresentRun(tc.present); got != tc.want {
			t.Errorf("LongestPresentRun(%v) = %d, want %d", tc.present, got, tc.want)
		}
	}
}

func TestKeysMatchPages(t *testing.T) {
	c := Config{PageSize: 4, Model: "m"}
	tokens := seq(12)
	pages, _ := c.Pages(tokens)
	keys, _ := c.Keys(tokens)
	if len(keys) != len(pages) {
		t.Fatalf("Keys/Pages length mismatch: %d vs %d", len(keys), len(pages))
	}
	for i := range pages {
		if keys[i] != pages[i].Key {
			t.Errorf("Keys[%d]=%q != Pages[%d].Key=%q", i, keys[i], i, pages[i].Key)
		}
	}
}
