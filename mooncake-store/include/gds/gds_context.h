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
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "mutex.h"
#include "gds/gds_device_ops.h"
#include "types.h"

namespace mooncake {

// On-disk record layout (single definition, shared by GDS and non-GDS
// paths of OffsetAllocatorStorageBackend):
//
//   [u32 key_len][u32 value_len][u64 seq][u32 flags][u32 crc32]
//   [key bytes][zero padding][value bytes]
//
// The value region always starts at a kValueAlignment boundary within the
// record so that cuFile DMA operates on aligned file offsets.  cuFile
// falls back to a CPU bounce buffer (silently, with no error) when the
// file offset is not aligned to the device logical block size, which
// would defeat the purpose of GDS.  The padding is a pure function of
// key_len, so writer and reader derive the same layout independently.
struct RecordHeader {
    // Length of key in bytes
    uint32_t key_len;

    // Length of value in bytes
    uint32_t value_len;

    // insert_seq_ stamp of this write (monotonic per BatchOffload entry).
    // GDS DMA writes leave this as 0; recovery orders via checkpoint seq.
    uint64_t seq;

    // Record flags; see kFlag* constants below.
    uint32_t flags;

    // CRC-32C over [key_len|value_len|seq|flags] + key + value.
    // Valid only when (flags & kFlagHasCrc).  GDS writes set this to 0.
    uint32_t crc32;

    // flags: crc32 field carries a valid CRC-32C of this record.
    static constexpr uint32_t kFlagHasCrc = 1u << 0;

    // flags: reservation placeholder.  ReserveOffloadSpace stamps a
    // header with this bit at the allocated offset before the client
    // DMA writes the real record.  Deliberately NOT part of kKnownFlags:
    // recovery must reject any header still carrying this bit (the
    // record was never written).
    static constexpr uint32_t kFlagReservationPlaceholder = 1u << 31;

    // All currently defined flag bits; recovery drops records with
    // unknown bits set (written by a newer format).
    static constexpr uint32_t kKnownFlags = kFlagHasCrc;

    // File-offset alignment for the value region.  4 KiB covers the
    // logical block size of all currently supported NVMe devices.
    static constexpr uint32_t kValueAlignment = 4096;

    // Header size: 24 bytes on disk (fields are (de)serialized
    // field-by-field; do NOT use sizeof(RecordHeader), which includes
    // padding).
    static constexpr size_t SIZE =
        sizeof(uint32_t) * 2 + sizeof(uint64_t) + sizeof(uint32_t) * 2;

    // Size of the crc-covered header prefix (everything before crc32).
    static constexpr size_t PREFIX_SIZE =
        sizeof(uint32_t) * 2 + sizeof(uint64_t) + sizeof(uint32_t);

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

    bool HasCrc() const { return (flags & kFlagHasCrc) != 0; }

    void WritePrefixTo(char* out) const {
        std::memcpy(out, &key_len, sizeof(key_len));
        std::memcpy(out + sizeof(key_len), &value_len, sizeof(value_len));
        std::memcpy(out + sizeof(key_len) + sizeof(value_len), &seq,
                    sizeof(seq));
        std::memcpy(out + sizeof(key_len) + sizeof(value_len) + sizeof(seq),
                    &flags, sizeof(flags));
    }

    void WriteTo(char* out) const {
        WritePrefixTo(out);
        std::memcpy(out + PREFIX_SIZE, &crc32, sizeof(crc32));
    }

    static RecordHeader ReadFrom(const char* buf) {
        RecordHeader h{};
        size_t off = 0;
        std::memcpy(&h.key_len, buf + off, sizeof(h.key_len));
        off += sizeof(h.key_len);
        std::memcpy(&h.value_len, buf + off, sizeof(h.value_len));
        off += sizeof(h.value_len);
        std::memcpy(&h.seq, buf + off, sizeof(h.seq));
        off += sizeof(h.seq);
        std::memcpy(&h.flags, buf + off, sizeof(h.flags));
        off += sizeof(h.flags);
        std::memcpy(&h.crc32, buf + off, sizeof(h.crc32));
        return h;
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
    // Buffer registration cache (range-aware, ordered by base address).
    // Registrations are snapped to the whole GPU allocation (via
    // GdsDeviceOps::GetAddressRange) rather than the requested span, so
    // steady-state I/O is pure IsRangeCovered() hits with zero
    // register/deregister churn in the nvidia-fs driver.
    struct RegisteredExtent {
        size_t size;
        uint64_t lru_tick;  // last-use tick, for cap eviction
    };
    std::map<void*, RegisteredExtent> registered_buffers_;
    // Monotonic tick source for RegisteredExtent::lru_tick; incremented
    // under buf_mutex_ on every cache access.
    uint64_t lru_clock_ = 0;

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
                                              uint64_t offset,
                                              uint64_t seq = 0);

    // Read one record from the data file:
    //   header + key -> ::pread (CPU) + verification
    //   value -> one or more destination slices (multi-fragment values
    //   are read consecutively into each slice in order):
    //     GPU dst -> cuFileRead (DMA), CPU dst -> ::pread (fallback)
    tl::expected<void, ErrorCode> ReadRecord(
        const std::string& key, const std::vector<Slice>& dest_slices,
        uint64_t offset, uint32_t expected_value_size);

    // Register a GPU buffer for GDS I/O. Checks the registration cache:
    // if (ptr, size) is already covered by a registered extent, returns
    // true immediately. On a miss the registration is snapped to the
    // whole GPU allocation containing ptr (GetAddressRange); when the
    // vendor cannot report allocation bounds, the requested span is
    // registered as-is. Registration failure does not block I/O —
    // cuFile falls back to an internal bounce buffer.
    bool EnsureBufferRegistered(void* gpu_ptr, size_t size);

    bool IsRangeCovered(void* ptr, size_t size);

    bool RegisterAndCache(void* gpu_ptr, size_t size);

    // Static check — uses gds_device_ops::ProbeDeviceNode().
    // Does not open/close the driver.
    static bool IsGdsAvailable();
};

}  // namespace mooncake
