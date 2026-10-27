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

package p2pstore

import "sync"

type CatalogParams struct {
	IsGold       bool
	AddrList     []uintptr
	SizeList     []uint64
	MaxShardSize uint64
}

type Catalog struct {
	entries map[string]CatalogParams
	mu      sync.Mutex
}

func NewCatalog() *Catalog {
	catalog := &Catalog{
		entries: make(map[string]CatalogParams),
	}
	return catalog
}

func (catalog *Catalog) Contains(name string) bool {
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	_, exist := catalog.entries[name]
	return exist
}

func (catalog *Catalog) Get(name string) (CatalogParams, bool) {
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	params, exist := catalog.entries[name]
	return params, exist
}

func (catalog *Catalog) Add(name string, params CatalogParams) {
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	catalog.entries[name] = params
}

func (catalog *Catalog) Remove(name string) {
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	delete(catalog.entries, name)
}
