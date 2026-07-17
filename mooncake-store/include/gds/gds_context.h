// Copyright 2025 KVCache.AI
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

#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>

#include "mutex.h"
#include "gds/gds_device_ops.h"
#include "types.h"

namespace mooncake {

// On-disk record layout (single definition, shared by GDS and non-GDS
// paths of OffsetAllocatorStorageBackend):
//
//   [u32 key_len][u32 value_len][key bytes][zero padding][value bytes]
//
// The value region always starts at a kValueAlignment boundary within the
// record so that cuFile DMA operates on aligned file offsets.  cuFile
// falls back to a CPU bounce buffer (silently, with no error) when the
// file offset is not aligned to the device logical block size, which
// would defeat the purpose of GDS.  The padding is a pure function of
// key_len, so writer and reader derive the same layout independently.
struct RecordHeader {
    uint32_t key_len;
    uint32_t value_len;
    static constexpr size_t SIZE = sizeof(uint32_t) * 2;  // 8 bytes

    // File-offset alignment for the value region.  4 KiB covers the
    // logical block size of all currently supported NVMe devices.
    static constexpr uint32_t kValueAlignment = 4096;

    // Zero-padding between key and value for the given key length.
    static constexpr uint32_t ValuePadding(uint32_t key_len) {
        const uint64_t head = SIZE + key_len;
        return static_cast<uint32_t>(
            (kValueAlignment - head % kValueAlignment) % kValueAlignment);
    }

    // Offset of the value region relative to the record start.
    static constexpr uint64_t ValueOffsetInRecord(uint32_t key_len) {
        return SIZE + key_len + ValuePadding(key_len);
    }

    // Total on-disk record size including padding.
    static constexpr uint64_t RecordSize(uint32_t key_len, uint32_t value_len) {
        return ValueOffsetInRecord(key_len) + value_len;
    }

    // Validate header against expected metadata
    bool ValidateAgainstMetadata(uint32_t expected_value_len) const {
        return value_len == expected_value_len;
    }

    // Validate key matches expected key
    tl::expected<void, ErrorCode> ValidateKey(
        const std::string& expected_key, const std::string& stored_key) const {
        if (stored_key.size() != key_len) {
            LOG(ERROR) << "Key length mismatch: expected " << key_len
                       << ", got " << stored_key.size();
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        if (stored_key != expected_key) {
            LOG(ERROR) << "Key mismatch: expected " << expected_key << ", got "
                       << stored_key;
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        return {};
    }
};

struct GdsContext {
    std::atomic<bool> enabled_{false};
    int gds_fd_ = -1;
    void* cu_file_handle_ = nullptr;     // GdsDeviceFileHandle (via ops_)
    std::unique_ptr<GdsDeviceOps> ops_;  // vendor implementation

    // Concurrency: record I/O (WriteRecord/ReadRecord) takes this mutex in
    // SHARED mode — pwrite/cuFileWrite/cuFileRead all carry explicit file
    // offsets, so concurrent records never interleave and cuFile is
    // thread-safe per handle.  Read-before-DMA-complete is prevented by the
    // dirty_ flag on ObjectEntry, not by this lock.  Shutdown() takes it in
    // EXCLUSIVE mode to drain in-flight DMA before deregistering the handle
    // and closing the fd.
    SharedMutex io_mutex_;

    // Buffer registration cache. WriteRecord/ReadRecord call
    // EnsureBufferRegistered() to reuse registrations across multiple
    // I/O operations on the same GPU address.
    Mutex buf_mutex_;
    std::unordered_map<void*, size_t> registered_buffers_;

    // Initialize GDS: probe -> open gds_fd_ -> fallocate ->
    // gds_device_ops::FileHandleRegister. Returns error on failure;
    // caller should Shutdown() + reset().
    tl::expected<void, ErrorCode> Init(const std::string& data_file_path,
                                       uint64_t capacity);

    // InitClientDma opens an *existing* data file for cuFile DMA I/O.
    // Does NOT posix_fallocate / O_TRUNC / I/O-probe — the file is owned
    // by store_service (which already did those steps).
    // Used by vLLM in normal-mode + GDS to obtain a cuFile handle on the
    // shared kv_cache.data for DMA writes.
    tl::expected<void, ErrorCode> InitClientDma(
        const std::string& existing_file_path);

    // Release: registered_buffers_ -> cu_file_handle_ -> gds_fd_
    // (cuFile requires buffers deregistered before handle deregister)
    void Shutdown();

    // Probe whether GDS is available on the given data directory.
    // Performs device node check, driver open (process-level singleton),
    // and end-to-end DMA write/read/verify.
    bool ProbeGdsAvailable(const std::string& data_dir);

    // Write one record to the data file at the given offset:
    //   header + key -> ::pwrite (CPU path, always)
    //   value (GPU slice) -> cuFileWrite (DMA)
    //   value (CPU slice) -> ::pwrite (fallback)
    tl::expected<void, ErrorCode> WriteRecord(const std::string& key,
                                              const std::vector<Slice>& slices,
                                              uint64_t offset);

    // Read one record from the data file:
    //   header + key -> ::pread (CPU) + verification
    //   value -> one or more destination slices (multi-fragment values
    //   are read consecutively into each slice in order):
    //     GPU dst -> cuFileRead (DMA), CPU dst -> ::pread (fallback)
    tl::expected<void, ErrorCode> ReadRecord(
        const std::string& key, const std::vector<Slice>& dest_slices,
        uint64_t offset, uint32_t expected_value_size);

    // Register a GPU buffer for GDS I/O. Checks the registration cache:
    // if the same (ptr, size) is already registered, returns true
    // immediately. If ptr is registered with a different size, the old
    // registration is replaced. Registration failure does not block
    // I/O — cuFile falls back to an internal bounce buffer.
    bool EnsureBufferRegistered(void* gpu_ptr, size_t size);

    // Static check — uses gds_device_ops::ProbeDeviceNode().
    // Does not open/close the driver.
    static bool IsGdsAvailable();
};

}  // namespace mooncake
