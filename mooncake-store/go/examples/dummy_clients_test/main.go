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

// multi_process_bench demonstrates multi-process benchmark with DummyClient.
package main

/*
#cgo LDFLAGS: -L${SRCDIR}/../../../build/mooncake-store/src -Wl,-rpath,${SRCDIR}/../../../build/mooncake-store/src -lmooncake_store -lstdc++ -lnuma -lglog -lgflags -ljsoncpp -lcurl -luring
#cgo CFLAGS: -I/home/w00889253/Mooncake/mooncake-store/include

#include <sched.h>
#include <stdlib.h>

static int set_cpu_affinity(int pid, const unsigned long *mask, size_t size) {
    return sched_setaffinity(pid, size, (cpu_set_t*)mask);
}

static int get_cpu_affinity(int pid, unsigned long *mask, size_t size) {
    return sched_getaffinity(pid, size, (cpu_set_t*)mask);
}
*/
import "C"

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/edsrzf/mmap-go"

	store "github.com/kvcache-ai/Mooncake/mooncake-store/go/mooncakestore"
)

// CPU_SETSIZE is the maximum number of CPUs supported by Linux
const cpuSetSizeBytes = 128

// cpuBindResult 绑核结果信息
type cpuBindResult struct {
	Success       bool
	Pid           int
	TargetList    string
	ActualList    string
	BeforeCount   int
	AfterCount    int
	ErrorMessage  string
	ActualMatches bool
}

// Global flags - 统一使用 uint64 类型避免类型不匹配问题
var (
	flagWorkers         = flag.Uint64("workers", 4, "Number of worker processes")
	flagValueSize       = flag.Uint64("value-size", 4*1024*1024, "Size of each value in bytes")
	flagNumKeys         = flag.Uint64("num-keys", 100, "Number of keys to write/read")
	flagBatchSize       = flag.Uint64("batch-size", 32, "Batch size for operations")
	flagIsWorker        = flag.Bool("worker", false, "Run as worker process")
	flagWorkerID        = flag.Uint64("worker-id", 0, "Worker process ID")
	flagPhase           = flag.String("phase", "write", "Phase: write/read/verify/all")
	flagReplicaNum      = flag.Uint64("replica-num", 1, "Number of replicas for each object")
	flagWaitSeconds     = flag.Uint64("wait-seconds", 5, "Seconds to wait between phases")
	flagGlobalSize      = flag.Uint64("global-size", 512*1024*1024, "Global segment size in bytes")
	flagLocalBufferSize = flag.Uint64("local-buffer-size", 128*1024*1024, "Local buffer size in bytes")
	flagPutBufferSize   = flag.Uint64("put-buffer-size", 512*1024*1024, "Put buffer size in bytes")
)

func main() {
	flag.Parse()

	// 参数验证
	if err := validateConfig(); err != nil {
		log.Fatalf("Configuration validation failed: %v", err)
	}
	log.Println("[INIT] Configuration validation passed")

	// 如果是子进程，执行子进程逻辑
	if *flagIsWorker {
		log.Printf("[WORKER-%d] Starting worker process", *flagWorkerID)
		runWorker(uint(*flagWorkerID))
		log.Printf("[WORKER-%d] Worker process completed", *flagWorkerID)
		return
	}

	// 主进程逻辑
	runMaster()
}

// validateConfig 验证配置参数的有效性
func validateConfig() error {
	if *flagWorkers == 0 {
		return fmt.Errorf("workers must be > 0, got %d", *flagWorkers)
	}
	if *flagValueSize == 0 {
		return fmt.Errorf("value-size must be > 0, got %d", *flagValueSize)
	}
	if *flagNumKeys == 0 {
		return fmt.Errorf("num-keys must be > 0, got %d", *flagNumKeys)
	}
	if *flagBatchSize == 0 {
		return fmt.Errorf("batch-size must be > 0, got %d", *flagBatchSize)
	}
	if *flagGlobalSize == 0 {
		return fmt.Errorf("global-size must be > 0, got %d", *flagGlobalSize)
	}
	if *flagLocalBufferSize == 0 {
		return fmt.Errorf("local-buffer-size must be > 0, got %d", *flagLocalBufferSize)
	}
	if *flagPutBufferSize == 0 {
		return fmt.Errorf("put-buffer-size must be > 0, got %d", *flagPutBufferSize)
	}
	if *flagPhase != "write" && *flagPhase != "read" && *flagPhase != "all" {
		return fmt.Errorf("invalid phase: %s (must be write/read/all)", *flagPhase)
	}
	if *flagIsWorker && *flagWorkerID >= *flagWorkers {
		return fmt.Errorf("invalid worker-id: %d (must be in [0, %d))", *flagWorkerID, *flagWorkers)
	}
	return nil
}

// calculateKeyRange 计算每个 worker 负责的键范围
func calculateKeyRange(workerID uint, totalWorkers, totalKeys uint64) (keyOffset uint64, keysPerWorker uint64) {
	keysPerWorker = totalKeys / totalWorkers
	keyOffset = uint64(workerID) * keysPerWorker

	// 处理余数，前 N 个 worker 多分配一个 key
	remainder := totalKeys % totalWorkers
	if uint64(workerID) < remainder {
		keysPerWorker++
		keyOffset = uint64(workerID)*(totalKeys/totalWorkers) + uint64(workerID)
	} else {
		keyOffset = uint64(workerID)*(totalKeys/totalWorkers) + remainder
	}

	return keyOffset, keysPerWorker
}

func runMaster() {
	log.Println("========================================")
	log.Println("    Mooncake Multi-Process Benchmark    ")
	log.Println("========================================")
	log.Println("[MASTER] Configuration Summary:")
	log.Printf("[MASTER]   Workers:          %d", *flagWorkers)
	log.Printf("[MASTER]   Value Size:       %d MB", *flagValueSize/(1024*1024))
	log.Printf("[MASTER]   Num Keys:         %d", *flagNumKeys)
	log.Printf("[MASTER]   Batch Size:       %d", *flagBatchSize)
	log.Printf("[MASTER]   Replicas:         %d", *flagReplicaNum)
	log.Printf("[MASTER]   Phase:            %s", *flagPhase)
	log.Printf("[MASTER]   Wait Seconds:     %d", *flagWaitSeconds)
	log.Printf("[MASTER]   Global Size:      %d MB", *flagGlobalSize/(1024*1024))
	log.Printf("[MASTER]   Local Buffer:     %d MB", *flagLocalBufferSize/(1024*1024))
	log.Printf("[MASTER]   Put Buffer:       %d MB", *flagPutBufferSize/(1024*1024))
	log.Println("========================================")

	if *flagPhase == "all" || *flagPhase == "write" {
		log.Println("\n--- Phase 1: WRITE PHASE ---")
		runPhase("write")
		log.Println("--- Write phase completed ---")

		if *flagWaitSeconds > 0 {
			log.Printf("Waiting %d seconds...", *flagWaitSeconds)
			time.Sleep(time.Duration(*flagWaitSeconds) * time.Second)
		}
	}

	if *flagPhase == "all" || *flagPhase == "read" {
		log.Println("\n--- Phase 2: READ PHASE ---")
		log.Println("(with data verification)")
		runPhase("read")
		log.Println("--- Read phase completed ---")
	}

	log.Println("\n========================================")
	log.Println("        Benchmark Completed             ")
	log.Println("========================================")
}

func runPhase(phase string) {
	var processes []*os.Process
	startTime := time.Now()

	log.Printf("[MASTER] ===============================")
	log.Printf("[MASTER] Starting %s phase", phase)
	log.Printf("[MASTER] Workers: %d", *flagWorkers)
	log.Printf("[MASTER] ===============================")

	// 启动所有 worker 进程
	for i := uint64(0); i < *flagWorkers; i++ {
		log.Printf("[MASTER] Starting worker %d...", i)
		cmd := exec.Command(
			os.Args[0],
			"-worker",
			fmt.Sprintf("-worker-id=%d", i),
			fmt.Sprintf("-workers=%d", *flagWorkers),
			fmt.Sprintf("-value-size=%d", *flagValueSize),
			fmt.Sprintf("-num-keys=%d", *flagNumKeys),
			fmt.Sprintf("-batch-size=%d", *flagBatchSize),
			fmt.Sprintf("-phase=%s", phase),
			fmt.Sprintf("-replica-num=%d", *flagReplicaNum),
			fmt.Sprintf("-wait-seconds=%d", *flagWaitSeconds),
			fmt.Sprintf("-global-size=%d", *flagGlobalSize),
			fmt.Sprintf("-local-buffer-size=%d", *flagLocalBufferSize),
			fmt.Sprintf("-put-buffer-size=%d", *flagPutBufferSize),
		)

		cmd.Env = os.Environ()
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			log.Fatalf("[MASTER] Failed to start worker %d: %v", i, err)
		}

		processes = append(processes, cmd.Process)
		log.Printf("[MASTER] Worker %d started successfully (PID: %d)", i, cmd.Process.Pid)
	}

	log.Printf("[MASTER] All %d workers started, waiting for completion...", *flagWorkers)

	// 等待所有进程完成
	var failedCount, successCount uint64
	for i, p := range processes {
		log.Printf("[MASTER] Waiting for worker %d (PID: %d)...", i, p.Pid)
		if _, err := p.Wait(); err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
					if status.Signaled() {
						log.Printf("[MASTER] Worker %d (PID: %d) killed by signal %d",
							i, p.Pid, status.Signal())
					} else {
						log.Printf("[MASTER] Worker %d (PID: %d) exited with status %d: %v",
							i, p.Pid, status.ExitStatus(), err)
					}
				} else {
					log.Printf("[MASTER] Worker %d (PID: %d) failed: %v", i, p.Pid, err)
				}
			} else {
				log.Printf("[MASTER] Worker %d (PID: %d) failed: %v", i, p.Pid, err)
			}
			failedCount++
		} else {
			log.Printf("[MASTER] Worker %d (PID: %d) completed successfully", i, p.Pid)
			successCount++
		}
	}

	duration := time.Since(startTime)
	log.Printf("[MASTER] ===============================")
	log.Printf("[MASTER] %s phase summary:", phase)
	log.Printf("[MASTER]   Duration:   %v", duration)
	log.Printf("[MASTER]   Success:    %d/%d workers", successCount, *flagWorkers)
	log.Printf("[MASTER]   Failed:     %d/%d workers", failedCount, *flagWorkers)
	if failedCount > 0 {
		log.Printf("[MASTER]   WARNING: Some workers failed during %s phase", phase)
	}
	log.Printf("[MASTER] ===============================")
}

func runWorker(workerID uint) {
	log.Printf("[WORKER-%d] Starting worker initialization", workerID)

	// 1. 绑核
	log.Printf("[WORKER-%d] Step 1/5: Attempting to bind to CPU %d", workerID, workerID)
	bindCPUForWorker(int(workerID))

	// 2. 创建并配置 store
	log.Printf("[WORKER-%d] Step 2/5: Creating DummyClient...", workerID)
	s := createAndSetupStore(int(workerID))
	defer func() {
		log.Printf("[WORKER-%d] Cleanup: Closing store...", workerID)
		s.Close()
		log.Printf("[WORKER-%d] Cleanup: Store closed successfully", workerID)
	}()

	// 3. 分配并注册 buffer（返回 mmap.MMap 类型和指针）
	bufBytes, _, cleanup := allocateAndRegisterBuffer(s, int(workerID))
	defer cleanup() // 关键：确保资源释放

	// 4. 根据阶段执行操作
	log.Printf("[WORKER-%d] Step 4/5: Executing %s phase...", workerID, *flagPhase)
	executePhase(s, bufBytes, int(workerID))

	// 5. 完成
	log.Printf("[WORKER-%d] Step 5/5: Worker completed successfully", workerID)
}

// createAndSetupStore 创建并配置 store
func createAndSetupStore(workerID int) *store.Store {
	s, err := store.NewWithType(store.MOONCAKE_CLIENT_DUMMY)
	if err != nil {
		log.Fatalf("[WORKER-%d] Failed to create store: %v", workerID, err)
	}
	log.Printf("[WORKER-%d] DummyClient created successfully", workerID)

	serverAddress := envOrDefault("MC_SERVERADDRESS", "")
	ipcSocketPath := envOrDefault("MC_IPCSOCKETPATH", "")

	log.Printf("[WORKER-%d] Store configuration:", workerID)
	log.Printf("[WORKER-%d]   Global Size:      %d MB", workerID, *flagGlobalSize/(1024*1024))
	log.Printf("[WORKER-%d]   Local Buffer:     %d MB", workerID, *flagLocalBufferSize/(1024*1024))
	log.Printf("[WORKER-%d]   Put Buffer:       %d MB", workerID, *flagPutBufferSize/(1024*1024))

	err = s.DummySetup(
		*flagPutBufferSize,
		*flagLocalBufferSize,
		serverAddress,
		ipcSocketPath,
	)
	if err != nil {
		log.Fatalf("[WORKER-%d] Setup failed: %v", workerID, err)
	}
	log.Printf("[WORKER-%d] Store setup completed successfully", workerID)

	return s
}

// allocateAndRegisterBuffer 分配并注册共享内存 buffer
func allocateAndRegisterBuffer(s *store.Store, workerID int) (buf []byte, ptr uintptr, cleanup func()) {
	// 1. 优先使用 RealClient 已存在的预注册缓冲区（如果存在）
	count, err := s.RegisteredBufferCount()
	if err == nil && count > 0 {
		bufInfo, err := s.RegisteredBufferAt(0)
		if err == nil && bufInfo.Size > 0 {
			ptr = bufInfo.Ptr
			// 通过 unsafe 构造 []byte 切片，便于后续使用
			buf = unsafe.Slice((*byte)(unsafe.Pointer(ptr)), bufInfo.Size)
			log.Printf("[WORKER-%d] Using pre-registered buffer: ptr=0x%x, size=%d bytes", workerID, ptr, bufInfo.Size)
			// 清理函数：仅注销，不 unmapping（因为内存由 RealClient 管理）
			cleanup = func() {
				log.Printf("[WORKER-%d] Cleanup: Unregistering pre-registered buffer", workerID)
				if err := s.UnregisterBuffer(ptr); err != nil {
					log.Printf("[WORKER-%d] Warning: UnregisterBuffer failed: %v", workerID, err)
				}
			}
			return
		}
	}

	// 2. 没有预注册缓冲区，则自行 mmap 匿名共享内存（RealClient 支持注册任意内存）
	bufSize := *flagBatchSize * *flagValueSize
	log.Printf("[WORKER-%d] No pre-registered buffer, allocating via mmap. Size: %d MB", workerID, bufSize/(1024*1024))

	pageSize := int64(syscall.Getpagesize())
	alignedSize := (int64(bufSize) + pageSize - 1) / pageSize * pageSize
	mmapBytes, err := syscall.Mmap(-1, 0, int(alignedSize),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED|syscall.MAP_ANONYMOUS)
	if err != nil {
		log.Fatalf("[WORKER-%d] Mmap failed: %v", workerID, err)
	}

	ptr = uintptr(unsafe.Pointer(&mmapBytes[0]))
	if err := s.RegisterBuffer(ptr, uint64(bufSize)); err != nil {
		syscall.Munmap(mmapBytes)
		log.Fatalf("[WORKER-%d] RegisterBuffer failed: %v", workerID, err)
	}
	log.Printf("[WORKER-%d] Buffer allocated via mmap: ptr=0x%x, size=%d bytes", workerID, ptr, bufSize)

	// 清理函数：注销并解除 mmap 映射
	cleanup = func() {
		log.Printf("[WORKER-%d] Cleanup: Unregistering and unmapping buffer", workerID)
		if err := s.UnregisterBuffer(ptr); err != nil {
			log.Printf("[WORKER-%d] Warning: UnregisterBuffer failed: %v", workerID, err)
		}
		if err := syscall.Munmap(mmapBytes); err != nil {
			log.Printf("[WORKER-%d] Warning: Munmap failed: %v", workerID, err)
		}
	}
	return
}

func allocateAndRegisterBufferViaMmap(s *store.Store, workerID int) (mmap.MMap, uintptr) {
	bufSize := *flagBatchSize * *flagValueSize * 16
	log.Printf("[WORKER-%d] Buffer size required: %d MB", workerID, bufSize/(1024*1024))

	pageSize := int64(syscall.Getpagesize())
	alignedSize := (int64(bufSize) + pageSize - 1) / pageSize * pageSize
	log.Printf("[WORKER-%d] Aligned buffer size: %d bytes (page size: %d)", workerID, alignedSize, pageSize)

	filePath := fmt.Sprintf("/dev/shm/mooncake_dummy_client%d.dat", workerID) // 每个worker独立文件
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("[WORKER-%d] Open file failed: %v", workerID, err)
	}
	defer f.Close()

	if err := f.Truncate(alignedSize); err != nil {
		log.Fatalf("[WORKER-%d] Truncate file failed: %v", workerID, err)
	}

	mm, err := mmap.MapRegion(f, int(alignedSize), mmap.RDWR, 0, 0)
	if err != nil {
		log.Fatalf("[WORKER-%d] mmap.MapRegion failed: %v", workerID, err)
	}

	ptr := uintptr(unsafe.Pointer(&mm[0]))
	if err := s.RegisterBuffer(ptr, uint64(bufSize)); err != nil {
		mm.Unmap()
		log.Fatalf("[WORKER-%d] RegisterBuffer failed: %v", workerID, err)
	}
	return mm, ptr
}

// executePhase 根据阶段执行操作
func executePhase(s *store.Store, buf []byte, workerID int) {
	switch *flagPhase {
	case "write":
		log.Printf("[WORKER-%d] Starting write phase...", workerID)
		writePhase(s, buf, workerID)
		log.Printf("[WORKER-%d] Write phase completed", workerID)
	case "read":
		log.Printf("[WORKER-%d] Starting read phase...", workerID)
		readPhase(s, buf, workerID)
		log.Printf("[WORKER-%d] Read phase completed", workerID)
	default:
		log.Fatalf("[WORKER-%d] Unknown phase: %s", workerID, *flagPhase)
	}
}

func writePhase(s *store.Store, buf []byte, workerID int) {
	keyOffset, keysPerWorker := calculateKeyRange(uint(workerID), *flagWorkers, *flagNumKeys)
	valueSize := int(*flagValueSize)

	log.Printf("[WORKER-%d] Write phase: keys %d-%d (total %d keys)",
		workerID, keyOffset, keyOffset+keysPerWorker-1, keysPerWorker)

	start := time.Now()
	var written, failed uint64

	for i := uint64(0); i < keysPerWorker; i++ {
		keyIdx := keyOffset + i
		key := MakeKey(keyIdx)

		// 填充数据
		FillBuffer(buf[:valueSize], keyIdx)

		// 使用 PutFrom 写入（零拷贝）
		if err := s.PutFrom(key, uintptr(unsafe.Pointer(&buf[0])), *flagValueSize, nil); err != nil {
			log.Printf("[WORKER-%d] PutFrom failed for key=%s: %v", workerID, key, err)
			failed++
		} else {
			written++
		}

		// 定期打印进度
		if (i+1)%10 == 0 || i == keysPerWorker-1 {
			log.Printf("[WORKER-%d] Written %d/%d keys (key=%s)",
				workerID, i+1, keysPerWorker, key)
		}
	}

	duration := time.Since(start)
	throughput := float64(written*(*flagValueSize)) / duration.Seconds() / (1024 * 1024)

	log.Printf("[WORKER-%d] Write phase completed:", workerID)
	log.Printf("[WORKER-%d]   Written:    %d", workerID, written)
	log.Printf("[WORKER-%d]   Failed:     %d", workerID, failed)
	log.Printf("[WORKER-%d]   Duration:   %v", workerID, duration)
	log.Printf("[WORKER-%d]   Throughput: %.2f MB/s", workerID, throughput)
}

func readPhase(s *store.Store, buf []byte, workerID int) {
	keyOffset, keysPerWorker := calculateKeyRange(uint(workerID), *flagWorkers, *flagNumKeys)
	valueSize := int(*flagValueSize)

	log.Printf("[WORKER-%d] Read phase: keys %d-%d (total %d keys)",
		workerID, keyOffset, keyOffset+keysPerWorker-1, keysPerWorker)

	start := time.Now()
	var totalBytes, failedOps, verifyErrors, successOps, totalOps uint64

	if *flagBatchSize > 1 {
		// 批量读取
		batchSizeVal := *flagBatchSize
		valueSize64 := *flagValueSize

		for i := uint64(0); i < keysPerWorker; i += batchSizeVal {
			batchEnd := uint64(math.Min(float64(i+batchSizeVal), float64(keysPerWorker)))
			batchSize := batchEnd - i

			batchStart := time.Now()

			for j := uint64(0); j < batchSize; j++ {
				keyIdx := keyOffset + i + j
				key := MakeKey(keyIdx)
				offset := int(j * valueSize64)

				n, err := s.GetInto(key, uintptr(unsafe.Pointer(&buf[offset])), *flagValueSize)
				totalOps++

				if err != nil || n < 0 {
					log.Printf("[WORKER-%d] GetInto failed for key=%s: %v", workerID, key, err)
					failedOps++
				} else {
					totalBytes += uint64(n)

					// 数据验证
					if !CheckBuffer(buf[offset:offset+valueSize], keyIdx) {
						log.Printf("[WORKER-%d] Data mismatch for key=%s", workerID, key)
						verifyErrors++
					} else {
						successOps++
					}
				}
			}

			batchDuration := time.Since(batchStart)
			if batchDuration.Seconds() > 0 {
				batchThroughput := float64(batchSize*valueSize64) / batchDuration.Seconds() / (1024 * 1024)
				log.Printf("[WORKER-%d] Batch %d-%d completed in %v (%.2f MB/s)",
					workerID, i, batchEnd-1, batchDuration, batchThroughput)
			}
		}
	} else {
		// 单条读取
		for i := uint64(0); i < keysPerWorker; i++ {
			keyIdx := keyOffset + i
			key := MakeKey(keyIdx)

			n, err := s.GetInto(key, uintptr(unsafe.Pointer(&buf[0])), *flagValueSize)
			totalOps++

			if err != nil || n < 0 {
				log.Printf("[WORKER-%d] GetInto failed for key=%s: %v", workerID, key, err)
				failedOps++
			} else {
				totalBytes += uint64(n)

				// 数据验证
				if !CheckBuffer(buf[:n], keyIdx) {
					log.Printf("[WORKER-%d] Data mismatch for key=%s", workerID, key)
					verifyErrors++
				} else {
					successOps++
				}
			}

			if (i+1)%10 == 0 {
				log.Printf("[WORKER-%d] Read %d/%d keys", workerID, i+1, keysPerWorker)
			}
		}
	}

	duration := time.Since(start)
	throughput := float64(totalBytes) / duration.Seconds() / (1024 * 1024)

	log.Printf("[WORKER-%d] Read phase completed:", workerID)
	log.Printf("[WORKER-%d]   Total Bytes:   %d", workerID, totalBytes)
	log.Printf("[WORKER-%d]   Total Ops:     %d", workerID, totalOps)
	log.Printf("[WORKER-%d]   Success Ops:   %d", workerID, successOps)
	log.Printf("[WORKER-%d]   Failed Ops:    %d", workerID, failedOps)
	log.Printf("[WORKER-%d]   Verify Errors: %d", workerID, verifyErrors)
	log.Printf("[WORKER-%d]   Duration:      %v", workerID, duration)
	log.Printf("[WORKER-%d]   Throughput:    %.2f MB/s", workerID, throughput)
}

// MakeKey 生成 key（模拟 C++ 的 MakeKey）
func MakeKey(idx uint64) string {
	return fmt.Sprintf("bench_key_%d", idx)
}

// FillBuffer 使用特定模式填充 buffer（模拟 C++ 的 FillBuffer）
func FillBuffer(buf []byte, seed uint64) {
	pattern := seed * 0x9E3779B97F4A7C15
	numWords := len(buf) / 8

	ptr := (*[1 << 30]uint64)(unsafe.Pointer(&buf[0]))
	for i := 0; i < numWords; i++ {
		ptr[i] = pattern + uint64(i)
	}

	// 处理剩余字节
	remaining := len(buf) % 8
	if remaining > 0 {
		lastWord := pattern + uint64(numWords)
		for i := 0; i < remaining; i++ {
			buf[numWords*8+i] = byte(lastWord >> (i * 8))
		}
	}
}

// CheckBuffer 验证数据完整性
func CheckBuffer(buf []byte, seed uint64) bool {
	if len(buf) == 0 {
		return false
	}

	pattern := seed * 0x9E3779B97F4A7C15
	numWords := len(buf) / 8

	ptr := (*[1 << 30]uint64)(unsafe.Pointer(&buf[0]))
	for i := 0; i < numWords; i++ {
		if ptr[i] != pattern+uint64(i) {
			log.Printf("CheckBuffer: Mismatch at word %d: expected %d, got %d",
				i, pattern+uint64(i), ptr[i])
			return false
		}
	}

	// 验证剩余字节
	remaining := len(buf) % 8
	if remaining > 0 {
		lastWord := pattern + uint64(numWords)
		for i := 0; i < remaining; i++ {
			expected := byte(lastWord >> (i * 8))
			if buf[numWords*8+i] != expected {
				log.Printf("CheckBuffer: Mismatch at byte %d: expected %d, got %d",
					numWords*8+i, expected, buf[numWords*8+i])
				return false
			}
		}
	}
	return true
}

// bindCPUForWorker 为 worker 进程绑定 CPU
func bindCPUForWorker(workerID int) {
	cpuList := fmt.Sprintf("%d", workerID)
	result := bindProcessAffinity(cpuList)

	if result.Success {
		log.Printf("[WORKER-%d] CPU binding succeeded: target=%s, actual=%s",
			workerID, result.TargetList, result.ActualList)
	} else {
		log.Printf("[WORKER-%d] CPU binding failed: %s", workerID, result.ErrorMessage)
	}
}

// parseRangeToSet 解析 CPU 范围字符串到 set
func parseRangeToSet(s string) map[int]bool {
	set := make(map[int]bool)
	if s == "" {
		return set
	}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.Contains(part, "-") {
			r := strings.SplitN(part, "-", 2)
			start, err1 := strconv.Atoi(strings.TrimSpace(r[0]))
			end, err2 := strconv.Atoi(strings.TrimSpace(r[1]))
			if err1 != nil || err2 != nil {
				continue
			}
			for i := start; i <= end; i++ {
				set[i] = true
			}
		} else {
			cpu, err := strconv.Atoi(part)
			if err == nil {
				set[cpu] = true
			}
		}
	}
	return set
}

// setToString 将 CPU 集合格式化为字符串
func setToString(set map[int]bool) string {
	if len(set) == 0 {
		return ""
	}
	cpus := make([]int, 0, len(set))
	for cpu := range set {
		cpus = append(cpus, cpu)
	}
	for i := 0; i < len(cpus); i++ {
		for j := i + 1; j < len(cpus); j++ {
			if cpus[j] < cpus[i] {
				cpus[i], cpus[j] = cpus[j], cpus[i]
			}
		}
	}
	parts := make([]string, len(cpus))
	for i, cpu := range cpus {
		parts[i] = strconv.Itoa(cpu)
	}
	return strings.Join(parts, ",")
}

// getCurrentAffinityList 获取当前进程的 CPU 亲和性列表
func getCurrentAffinityList() (string, int, error) {
	mask := make([]C.ulong, cpuSetSizeBytes/8)

	result := C.get_cpu_affinity(0, &mask[0], C.size_t(cpuSetSizeBytes))
	if result != 0 {
		return "", 0, fmt.Errorf("sched_getaffinity failed: result=%d", result)
	}

	set := make(map[int]bool)
	for i := 0; i < cpuSetSizeBytes*8; i++ {
		wordIdx := i / (8 * 8)
		bitIdx := i % (8 * 8)
		if mask[wordIdx]&(1<<C.ulong(bitIdx)) != 0 {
			set[i] = true
		}
	}
	return setToString(set), len(set), nil
}

// bindProcessAffinity 使用 CGO 调用 C 标准库实现绑核
func bindProcessAffinity(cpuList string) cpuBindResult {
	result := cpuBindResult{
		Pid:        os.Getpid(),
		TargetList: cpuList,
	}

	if runtime.GOOS != "linux" {
		result.ErrorMessage = "CPU binding only supported on Linux"
		return result
	}

	if cpuList == "" {
		result.ErrorMessage = "empty cpu list"
		return result
	}

	targetSet := parseRangeToSet(cpuList)
	if len(targetSet) == 0 {
		result.ErrorMessage = "no valid CPU in list"
		return result
	}

	_, beforeCount, _ := getCurrentAffinityList()
	result.BeforeCount = beforeCount

	mask := make([]C.ulong, cpuSetSizeBytes/8)
	for cpu := range targetSet {
		if cpu >= cpuSetSizeBytes*8 {
			log.Printf("[CPU-BIND] Warning: CPU %d exceeds mask size %d, ignored", cpu, cpuSetSizeBytes*8)
			continue
		}
		wordIdx := cpu / (8 * 8)
		bitIdx := cpu % (8 * 8)
		mask[wordIdx] |= 1 << C.ulong(bitIdx)
	}

	cResult := C.set_cpu_affinity(0, &mask[0], C.size_t(cpuSetSizeBytes))
	if cResult != 0 {
		result.ErrorMessage = fmt.Sprintf("sched_setaffinity failed: result=%d", cResult)
		return result
	}

	afterList, afterCount, err := getCurrentAffinityList()
	if err != nil {
		result.ErrorMessage = fmt.Sprintf("getaffinity after failed: %v", err)
		return result
	}

	result.Success = true
	result.ActualList = afterList
	result.AfterCount = afterCount

	actualSet := parseRangeToSet(afterList)
	match := true
	for cpu := range targetSet {
		if !actualSet[cpu] {
			match = false
			break
		}
	}
	if len(actualSet) != len(targetSet) {
		match = false
	}
	result.ActualMatches = match

	return result
}

// envOrDefault 获取环境变量，不存在则返回默认值
func envOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
