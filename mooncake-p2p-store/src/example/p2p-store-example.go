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

package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"syscall"
	"time"
	"unsafe"

	"github.com/kvcache-ai/Mooncake/mooncake-p2p-store/src/p2pstore"
)

var (
	command               string
	metadataServer        string
	localServerName       string
	deviceName            string
	nicPriorityMatrixPath string
	fileSize              int
	fileSizeMB            int
)

func main() {
	flag.StringVar(&command, "cmd", "trainer", "Command: trainer|inferencer")
	flag.StringVar(&metadataServer, "metadata_server", "localhost:2379", "Metadata server address")
	flag.StringVar(&localServerName, "local_server_name", "", "Local server name")
	flag.StringVar(&deviceName, "device_name", "mlx5_2", "RNIC device name")
	flag.StringVar(&nicPriorityMatrixPath, "nic_priority_matrix", "", "Path to NIC priority matrix file (Advanced)")
	flag.IntVar(&fileSizeMB, "file_size_mb", 2048, "File size in MB")
	flag.Parse()

	fileSize = fileSizeMB * 1024 * 1024

	if len(localServerName) == 0 {
		var err error
		localServerName, err = os.Hostname()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting hostname: %v\n", err)
			os.Exit(1)
		}
	}

	switch command {
	case "trainer":
		trainer()
	case "inferencer":
		inferencer()
	default:
		fmt.Printf("You must specify a command, either 'trainer' or 'inferencer'\n")
		os.Exit(1)
	}
}

func doTrainer(ctx context.Context, store *p2pstore.P2PStore, name string) {
	addr, err := syscall.Mmap(-1, 0, fileSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Mmap failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Object registration: name %s base address %x file size %d MB\n",
		name,
		uintptr(unsafe.Pointer(&addr[0])),
		fileSizeMB)

	startTimestamp := time.Now()
	addrList := []uintptr{uintptr(unsafe.Pointer(&addr[0]))}
	sizeList := []uint64{uint64(fileSize)}

	const MAX_SHARD_SIZE uint64 = 64 * 1024 * 1024
	const MEMORY_LOCATION string = "cpu:0"

	err = store.Register(ctx, name, addrList, sizeList, MAX_SHARD_SIZE, MEMORY_LOCATION, true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Object registration failed: %v\n", err)
		os.Exit(1)
	}

	phaseOneTimestamp := time.Now()
	duration := phaseOneTimestamp.Sub(startTimestamp).Milliseconds()

	fmt.Printf("Object registration done: duration (ms) %d throughput (GB/s) %.2f\n",
		duration,
		float64(fileSizeMB)/float64(duration))

	checkpointInfoList, err := store.List(ctx, "foo")
	if err != nil {
		fmt.Fprintf(os.Stderr, "List failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(checkpointInfoList)
	fmt.Println("Idle for 100 seconds, now you can start another terminal to simulate inference")
	time.Sleep(100 * time.Second)

	err = store.Unregister(ctx, name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unregister failed: %v\n", err)
		os.Exit(1)
	}

	if err := syscall.Munmap(addr); err != nil {
		fmt.Fprintf(os.Stderr, "Munmap failed: %v\n", err)
		os.Exit(1)
	}
}

func trainer() {
	fmt.Println("Simulated training process started")
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	defer cancel()

	store, err := p2pstore.NewP2PStore(metadataServer, localServerName, getPriorityMatrix())
	if err != nil {
		fmt.Fprintf(os.Stderr, "P2PStore: initialization failed: %v\n", err)
		os.Exit(1)
	}

	doTrainer(ctx, store, "foo/bar")

	err = store.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "P2PStore: close failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Simulated training process stopped gracefully")
}

func getPriorityMatrix() string {
	if len(nicPriorityMatrixPath) != 0 {
		data, err := ioutil.ReadFile(nicPriorityMatrixPath)
		if err != nil {
			fmt.Println("Error reading file:", err)
			os.Exit(1)
		}
		return string(data)
	} else {
		return "{ \"cpu:0\": [[\"" + deviceName + "\"], []]}"
	}
}

func doInferencer(ctx context.Context, store *p2pstore.P2PStore, name string) {
	addr, err := syscall.Mmap(-1, 0, fileSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Mmap failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Object retrieval started: name", name)
	startTimestamp := time.Now()
	addrList := []uintptr{uintptr(unsafe.Pointer(&addr[0]))}
	sizeList := []uint64{uint64(fileSize)}
	err = store.GetReplica(ctx, name, addrList, sizeList)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Object retrieval failed: %v\n", err)
		os.Exit(1)
	}

	phaseOneTimestamp := time.Now()
	duration := phaseOneTimestamp.Sub(startTimestamp).Milliseconds()

	fmt.Printf("Object retrieval done: duration (ms) %d throughput (GB/s) %.2f\n",
		duration,
		float64(fileSizeMB)/float64(duration))

	err = store.DeleteReplica(ctx, name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "DeleteReplica failed: %v\n", err)
		os.Exit(1)
	}

	if err := syscall.Munmap(addr); err != nil {
		fmt.Fprintf(os.Stderr, "Munmap failed: %v\n", err)
		os.Exit(1)
	}
}

func inferencer() {
	fmt.Println("Simulated inference process started")

	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	defer cancel()

	store, err := p2pstore.NewP2PStore(metadataServer, localServerName, getPriorityMatrix())
	if err != nil {
		fmt.Fprintf(os.Stderr, "P2PStore: initialization failed: %v\n", err)
		os.Exit(1)
	}

	doInferencer(ctx, store, "foo/bar")

	err = store.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "P2PStore: close failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Simulated inference process stopped gracefully")
}
