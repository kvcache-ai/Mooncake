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

// basic demonstrates using Mooncake Store from Go.
//
// Prerequisites:
//
//	mooncake_master --enable_http_metadata_server=true \
//	  --http_metadata_server_port=8080
package main

import (
	"fmt"
	"log"
	"os"

	store "github.com/kvcache-ai/Mooncake/mooncake-store/go/mooncakestore"
)

func main() {
	metadataServer := envOrDefault("MC_METADATA_SERVER", "http://localhost:8080/metadata")
	masterAddr := envOrDefault("MC_MASTER_ADDR", "localhost:50051")

	s, err := store.New()
	if err != nil {
		log.Fatalf("store.New: %v", err)
	}
	defer s.Close()

	err = s.Setup(
		"localhost",
		metadataServer,
		512*1024*1024, // 512 MB global segment
		128*1024*1024, // 128 MB local buffer
		"tcp",
		"",
		masterAddr,
	)
	if err != nil {
		log.Fatalf("Setup: %v", err)
	}
	fmt.Println("Connected to Mooncake Store")

	// Put
	key := "hello_from_go"
	value := []byte("Hello, Mooncake!")
	if err := s.Put(key, value, nil); err != nil {
		log.Fatalf("Put: %v", err)
	}
	fmt.Printf("Put: %s => %s\n", key, value)

	// Exists
	exists, err := s.Exists(key)
	if err != nil {
		log.Fatalf("Exists: %v", err)
	}
	fmt.Printf("Exists(%s) = %v\n", key, exists)

	// GetSize
	size, err := s.GetSize(key)
	if err != nil {
		log.Fatalf("GetSize: %v", err)
	}
	fmt.Printf("GetSize(%s) = %d bytes\n", key, size)

	// Get
	buf := make([]byte, size)
	n, err := s.Get(key, buf)
	if err != nil {
		log.Fatalf("Get: %v", err)
	}
	fmt.Printf("Get: %s => %s\n", key, buf[:n])

	// Remove
	if err := s.Remove(key, false); err != nil {
		log.Fatalf("Remove: %v", err)
	}
	fmt.Printf("Removed: %s\n", key)

	fmt.Println("Done!")
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
