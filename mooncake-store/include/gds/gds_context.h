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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "mutex.h"
#include "gds/gds_device_ops.h"
#include "types.h"

namespace mooncake {

struct RecordHeader {
    uint32_t key_len;
    uint32_t value_len;
    static constexpr size_t SIZE = sizeof(uint32_t) * 2;  // 8 bytes
};

struct GdsContext {
    bool enabled_ = false;
    int gds_fd_ = -1;
    void* cu_file_handle_ = nullptr;     // GdsDeviceFileHandle (via ops_)
    std::unique_ptr<GdsDeviceOps> ops_;  // vendor implementation

    // Serializes record I/O: header + key + value are written/read as
    // a contiguous unit so concurrent callers do not interleave.
    Mutex io_mutex_;

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
    //   value (GPU dst) -> cuFileRead (DMA)
    //   value (CPU dst) -> ::pread (fallback)
    tl::expected<void, ErrorCode> ReadRecord(const std::string& key,
                                             Slice& dest_slice, uint64_t offset,
                                             uint32_t expected_value_size);

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
