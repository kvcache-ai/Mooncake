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
	"os"
	"syscall"
	"time"
	"unsafe"

	"github.com/kvcache-ai/Mooncake/mooncake-p2p-store/src/p2pstore"
)

var (
	command         string
	metadataServer  string
	localServerName string
	fileSize        int
)

func main() {
	flag.StringVar(&command, "cmd", "trainer", "Command: trainer|inferencer")
	flag.StringVar(&metadataServer, "metadata_server", "localhost:2379", "Metadata server address")
	flag.StringVar(&localServerName, "local_server_name", "", "Local server name")
	flag.IntVar(&fileSize, "file_size_mb", 2048, "File size in MB")
	flag.Parse()

	fileSize = fileSize * 1024 * 1024
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
		fmt.Printf("Invalid command: %s\n", command)
		os.Exit(1)
	}
}

func doTrainer(ctx context.Context, store *p2pstore.P2PStore, name string) {
	addr, err := syscall.Mmap(-1, 0, fileSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Mmap failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("After training, register new object:", name, "file size:", fileSize)
	startTimestamp := time.Now()
	addrList := []uintptr{uintptr(unsafe.Pointer(&addr[0]))}
	sizeList := []uint64{uint64(fileSize)}
	err = store.Register(ctx, name, addrList, sizeList, 64*1024*1024, "cpu:0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Register failed: %v\n", err)
		os.Exit(1)
	}

	phaseOneTimestamp := time.Now()
	fmt.Println("Register done, duration (ms):", phaseOneTimestamp.Sub(startTimestamp).Milliseconds())

	checkpointInfoList, err := store.List(ctx, "foo")
	if err != nil {
		fmt.Fprintf(os.Stderr, "List failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(checkpointInfoList)
	fmt.Println("Idle for 100 seconds")
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
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	defer cancel()

	store, err := p2pstore.NewP2PStore(metadataServer, localServerName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating checkpoint engine: %v\n", err)
		os.Exit(1)
	}

	doTrainer(ctx, store, "foo/bar")

	err = store.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Shutdown failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("ALL DONE")
}

func doInferencer(ctx context.Context, store *p2pstore.P2PStore, name string) {
	addr, err := syscall.Mmap(-1, 0, fileSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Mmap failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Expecting to retrieve from object", name)
	startTimestamp := time.Now()
	addrList := []uintptr{uintptr(unsafe.Pointer(&addr[0]))}
	sizeList := []uint64{uint64(fileSize)}
	err = store.GetReplica(ctx, name, addrList, sizeList)
	if err != nil {
		fmt.Fprintf(os.Stderr, "GetLocalCheckpoint failed: %v\n", err)
		os.Exit(1)
	}

	phaseOneTimestamp := time.Now()
	fmt.Println("GetReplica done, duration (ms):", phaseOneTimestamp.Sub(startTimestamp).Milliseconds())

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
	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	defer cancel()

	store, err := p2pstore.NewP2PStore(metadataServer, localServerName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating checkpoint engine: %v\n", err)
		os.Exit(1)
	}

	doInferencer(ctx, store, "foo/bar")
	err = store.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Shutdown failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("ALL DONE")
}
