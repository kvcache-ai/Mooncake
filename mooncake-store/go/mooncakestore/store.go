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

// Package mooncakestore provides Go bindings for the Mooncake distributed
// KVCache store. It wraps the C API (store_c.h) via CGo, giving Go programs
// access to high-performance, RDMA-capable put/get operations.
package mooncakestore

/*
#include "store_c.h"
#include <stdlib.h>
*/
import "C"
import (
	"unsafe"
)

// Store wraps a Mooncake Store client handle.
type Store struct {
	handle C.mooncake_store_t
}

// New creates a new Store instance. Call Setup before performing operations,
// and Close when done.
func New() (*Store, error) {
	h := C.mooncake_store_create()
	if h == nil {
		return nil, ErrStoreNil
	}
	return &Store{handle: h}, nil
}

// Setup initialises the store client and connects to the cluster.
//
// Parameters:
//   - localHostname: hostname/IP of this node
//   - metadataServer: metadata server URL (e.g. "http://host:8080/metadata")
//   - globalSegmentSize: size of the global memory segment in bytes
//   - localBufferSize: size of the local transfer buffer in bytes
//   - protocol: transport protocol ("tcp" or "rdma")
//   - deviceName: RDMA device name (empty for TCP or auto-discovery)
//   - masterServerAddr: master service address (e.g. "host:50051")
func (s *Store) Setup(localHostname, metadataServer string,
	globalSegmentSize, localBufferSize uint64,
	protocol, deviceName, masterServerAddr string) error {
	if s.handle == nil {
		return ErrStoreNil
	}

	cLocalHostname := C.CString(localHostname)
	cMetadataServer := C.CString(metadataServer)
	cProtocol := C.CString(protocol)
	cDeviceName := C.CString(deviceName)
	cMasterAddr := C.CString(masterServerAddr)
	defer C.free(unsafe.Pointer(cLocalHostname))
	defer C.free(unsafe.Pointer(cMetadataServer))
	defer C.free(unsafe.Pointer(cProtocol))
	defer C.free(unsafe.Pointer(cDeviceName))
	defer C.free(unsafe.Pointer(cMasterAddr))

	ret := C.mooncake_store_setup(s.handle,
		cLocalHostname, cMetadataServer,
		C.uint64_t(globalSegmentSize), C.uint64_t(localBufferSize),
		cProtocol, cDeviceName, cMasterAddr)
	if ret != 0 {
		return ErrSetupFailed
	}
	return nil
}

// InitAll mounts additional memory segments after setup.
func (s *Store) InitAll(protocol, deviceName string, mountSegmentSize uint64) error {
	if s.handle == nil {
		return ErrStoreNil
	}

	cProtocol := C.CString(protocol)
	cDeviceName := C.CString(deviceName)
	defer C.free(unsafe.Pointer(cProtocol))
	defer C.free(unsafe.Pointer(cDeviceName))

	ret := C.mooncake_store_init_all(s.handle, cProtocol, cDeviceName,
		C.uint64_t(mountSegmentSize))
	if ret != 0 {
		return ErrInitAllFailed
	}
	return nil
}

// HealthCheck verifies connectivity to the master and cluster.
// Returns nil if healthy.
func (s *Store) HealthCheck() error {
	if s.handle == nil {
		return ErrStoreNil
	}
	ret := C.mooncake_store_health_check(s.handle)
	if ret != 0 {
		return ErrHealthCheck
	}
	return nil
}

// Close tears down the store client and frees resources.
func (s *Store) Close() {
	if s.handle != nil {
		C.mooncake_store_destroy(s.handle)
		s.handle = nil
	}
}

// ---------------------------------------------------------------------------
// Put operations
// ---------------------------------------------------------------------------

func (s *Store) toCConfig(cfg *ReplicateConfig) (C.mooncake_replicate_config_t, []*C.char, error) {
	var cc C.mooncake_replicate_config_t
	var cSegs []*C.char

	if cfg == nil {
		cc.replica_num = 1
		return cc, nil, nil
	}

	if cfg.ReplicaNum <= 0 {
		return cc, nil, ErrInvalidArgument
	}

	cc.replica_num = C.size_t(cfg.ReplicaNum)
	if cfg.WithSoftPin {
		cc.with_soft_pin = 1
	}
	if cfg.WithHardPin {
		cc.with_hard_pin = 1
	}

	if len(cfg.PreferredSegments) > 0 {
		cSegs = make([]*C.char, len(cfg.PreferredSegments))
		for i, seg := range cfg.PreferredSegments {
			cSegs[i] = C.CString(seg)
		}
		cc.preferred_segments = &cSegs[0]
		cc.preferred_segments_count = C.size_t(len(cSegs))
	}

	return cc, cSegs, nil
}

func freeCStrings(strs []*C.char) {
	for _, s := range strs {
		C.free(unsafe.Pointer(s))
	}
}

// Put stores a byte slice under the given key.
func (s *Store) Put(key string, value []byte, config *ReplicateConfig) error {
	if s.handle == nil {
		return ErrStoreNil
	}

	cc, cSegs, err := s.toCConfig(config)
	if err != nil {
		return err
	}
	defer freeCStrings(cSegs)

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	var ptr unsafe.Pointer
	if len(value) > 0 {
		ptr = unsafe.Pointer(&value[0])
	}

	ret := C.mooncake_store_put(s.handle, cKey, ptr, C.size_t(len(value)), &cc)
	if ret != 0 {
		return ErrPut
	}
	return nil
}

// PutFrom stores data directly from a pre-registered memory buffer.
// The buffer at ptr must have been registered via RegisterBuffer.
func (s *Store) PutFrom(key string, ptr uintptr, size uint64, config *ReplicateConfig) error {
	if s.handle == nil {
		return ErrStoreNil
	}
	if ptr == 0 || size == 0 {
		return ErrInvalidArgument
	}

	cc, cSegs, err := s.toCConfig(config)
	if err != nil {
		return err
	}
	defer freeCStrings(cSegs)

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	ret := C.mooncake_store_put_from(s.handle, cKey,
		unsafe.Pointer(ptr), C.size_t(size), &cc)
	if ret != 0 {
		return ErrPut
	}
	return nil
}

// BatchPutFrom stores multiple objects from pre-registered memory buffers.
// Returns per-key result codes (0 = success).
func (s *Store) BatchPutFrom(keys []string, ptrs []uintptr, sizes []uint64,
	config *ReplicateConfig) ([]int, error) {
	if s.handle == nil {
		return nil, ErrStoreNil
	}
	count := len(keys)
	if count == 0 {
		return nil, nil
	}
	if len(ptrs) != count || len(sizes) != count {
		return nil, ErrInvalidArgument
	}

	cKeys := make([]*C.char, count)
	for i, k := range keys {
		cKeys[i] = C.CString(k)
	}
	defer func() {
		for _, ck := range cKeys {
			C.free(unsafe.Pointer(ck))
		}
	}()

	cBufs := make([]unsafe.Pointer, count)
	cSizes := make([]C.size_t, count)
	for i := range ptrs {
		cBufs[i] = unsafe.Pointer(ptrs[i])
		cSizes[i] = C.size_t(sizes[i])
	}

	cc, cSegs, err := s.toCConfig(config)
	if err != nil {
		return nil, err
	}
	defer freeCStrings(cSegs)

	results := make([]C.int, count)
	ret := C.mooncake_store_batch_put_from(s.handle,
		&cKeys[0], &cBufs[0], &cSizes[0], C.size_t(count),
		&cc, &results[0])
	if ret != 0 {
		return nil, ErrBatchOp
	}

	goResults := make([]int, count)
	for i := range results {
		goResults[i] = int(results[i])
	}
	return goResults, nil
}

// ---------------------------------------------------------------------------
// Get operations
// ---------------------------------------------------------------------------

// GetInto reads an object directly into a pre-registered buffer.
// Returns the number of bytes read.
func (s *Store) GetInto(key string, ptr uintptr, size uint64) (int64, error) {
	if s.handle == nil {
		return 0, ErrStoreNil
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	ret := C.mooncake_store_get_into(s.handle, cKey,
		unsafe.Pointer(ptr), C.size_t(size))
	if ret < 0 {
		return 0, ErrGet
	}
	return int64(ret), nil
}

// Get is a convenience method that copies value bytes into a Go slice.
// For high-performance use cases, prefer GetInto with registered buffers.
func (s *Store) Get(key string, buf []byte) (int64, error) {
	if s.handle == nil {
		return 0, ErrStoreNil
	}
	if len(buf) == 0 {
		return 0, ErrInvalidArgument
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	ret := C.mooncake_store_get_into(s.handle, cKey,
		unsafe.Pointer(&buf[0]), C.size_t(len(buf)))
	if ret < 0 {
		return 0, ErrGet
	}
	return int64(ret), nil
}

// BatchGetInto reads multiple objects into pre-registered buffers.
// Returns per-key byte counts (negative = error).
func (s *Store) BatchGetInto(keys []string, ptrs []uintptr, sizes []uint64) ([]int64, error) {
	if s.handle == nil {
		return nil, ErrStoreNil
	}
	count := len(keys)
	if count == 0 {
		return nil, nil
	}
	if len(ptrs) != count || len(sizes) != count {
		return nil, ErrInvalidArgument
	}

	cKeys := make([]*C.char, count)
	for i, k := range keys {
		cKeys[i] = C.CString(k)
	}
	defer func() {
		for _, ck := range cKeys {
			C.free(unsafe.Pointer(ck))
		}
	}()

	cBufs := make([]unsafe.Pointer, count)
	cSizes := make([]C.size_t, count)
	for i := range ptrs {
		cBufs[i] = unsafe.Pointer(ptrs[i])
		cSizes[i] = C.size_t(sizes[i])
	}

	results := make([]C.int64_t, count)
	ret := C.mooncake_store_batch_get_into(s.handle,
		&cKeys[0], &cBufs[0], &cSizes[0], C.size_t(count), &results[0])
	if ret != 0 {
		return nil, ErrBatchOp
	}

	goResults := make([]int64, count)
	for i := range results {
		goResults[i] = int64(results[i])
	}
	return goResults, nil
}

// ---------------------------------------------------------------------------
// Existence / size / hostname
// ---------------------------------------------------------------------------

// Exists checks whether a key exists in the store.
func (s *Store) Exists(key string) (bool, error) {
	if s.handle == nil {
		return false, ErrStoreNil
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	ret := C.mooncake_store_is_exist(s.handle, cKey)
	if ret < 0 {
		return false, ErrExist
	}
	return ret == 1, nil
}

// BatchExists checks existence for multiple keys.
func (s *Store) BatchExists(keys []string) ([]bool, error) {
	if s.handle == nil {
		return nil, ErrStoreNil
	}
	count := len(keys)
	if count == 0 {
		return nil, nil
	}

	cKeys := make([]*C.char, count)
	for i, k := range keys {
		cKeys[i] = C.CString(k)
	}
	defer func() {
		for _, ck := range cKeys {
			C.free(unsafe.Pointer(ck))
		}
	}()

	results := make([]C.int, count)
	ret := C.mooncake_store_batch_is_exist(s.handle, &cKeys[0],
		C.size_t(count), &results[0])
	if ret != 0 {
		return nil, ErrBatchOp
	}

	goResults := make([]bool, count)
	for i := range results {
		goResults[i] = results[i] == 1
	}
	return goResults, nil
}

// GetSize returns the size in bytes of the object stored under key.
func (s *Store) GetSize(key string) (int64, error) {
	if s.handle == nil {
		return 0, ErrStoreNil
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	ret := C.mooncake_store_get_size(s.handle, cKey)
	if ret < 0 {
		return 0, ErrGetSize
	}
	return int64(ret), nil
}

// Hostname returns the hostname of this store client node.
func (s *Store) Hostname() (string, error) {
	if s.handle == nil {
		return "", ErrStoreNil
	}
	const bufLen = 256
	buf := make([]byte, bufLen)
	ret := C.mooncake_store_get_hostname(s.handle,
		(*C.char)(unsafe.Pointer(&buf[0])), C.size_t(bufLen))
	if ret != 0 {
		return "", ErrHostname
	}
	return C.GoString((*C.char)(unsafe.Pointer(&buf[0]))), nil
}

// ---------------------------------------------------------------------------
// Remove operations
// ---------------------------------------------------------------------------

// Remove deletes an object by key. If force is true, the object is removed
// even if it has active leases.
func (s *Store) Remove(key string, force bool) error {
	if s.handle == nil {
		return ErrStoreNil
	}

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	cForce := C.int(0)
	if force {
		cForce = 1
	}
	ret := C.mooncake_store_remove(s.handle, cKey, cForce)
	if ret != 0 {
		return ErrRemove
	}
	return nil
}

// RemoveByRegex removes all objects whose keys match the given regex pattern.
// Returns the number of objects removed.
func (s *Store) RemoveByRegex(pattern string, force bool) (int64, error) {
	if s.handle == nil {
		return 0, ErrStoreNil
	}

	cPattern := C.CString(pattern)
	defer C.free(unsafe.Pointer(cPattern))

	cForce := C.int(0)
	if force {
		cForce = 1
	}
	ret := C.mooncake_store_remove_by_regex(s.handle, cPattern, cForce)
	if ret < 0 {
		return 0, ErrRemove
	}
	return int64(ret), nil
}

// RemoveAll removes every object in the store.
// Returns the number of objects removed.
func (s *Store) RemoveAll(force bool) (int64, error) {
	if s.handle == nil {
		return 0, ErrStoreNil
	}

	cForce := C.int(0)
	if force {
		cForce = 1
	}
	ret := C.mooncake_store_remove_all(s.handle, cForce)
	if ret < 0 {
		return 0, ErrRemove
	}
	return int64(ret), nil
}

// ---------------------------------------------------------------------------
// Buffer registration
// ---------------------------------------------------------------------------

// RegisterBuffer registers a memory region for zero-copy operations.
// The memory at ptr must remain valid until UnregisterBuffer is called.
func (s *Store) RegisterBuffer(ptr uintptr, size uint64) error {
	if s.handle == nil {
		return ErrStoreNil
	}
	if ptr == 0 || size == 0 {
		return ErrInvalidArgument
	}
	ret := C.mooncake_store_register_buffer(s.handle,
		unsafe.Pointer(ptr), C.size_t(size))
	if ret != 0 {
		return ErrRegisterBuffer
	}
	return nil
}

// UnregisterBuffer unregisters a previously registered memory region.
func (s *Store) UnregisterBuffer(ptr uintptr) error {
	if s.handle == nil {
		return ErrStoreNil
	}
	ret := C.mooncake_store_unregister_buffer(s.handle, unsafe.Pointer(ptr))
	if ret != 0 {
		return ErrUnregisterBuffer
	}
	return nil
}
