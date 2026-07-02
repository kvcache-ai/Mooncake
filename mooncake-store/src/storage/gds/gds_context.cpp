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

#include "gds/gds_context.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <glog/logging.h>

#ifdef USE_GDS_BACKEND
#include "gds/gds_device_ops.h"
#include "gpu_staging_utils.h"
#endif

namespace mooncake {

// ===================================================================
// GdsContext::Init()
// ===================================================================
tl::expected<void, ErrorCode> GdsContext::Init(
    const std::string& data_file_path, uint64_t capacity) {
#ifdef USE_GDS_BACKEND
    // 0. Create parent directory
    std::filesystem::path p(data_file_path);
    std::string data_dir = p.parent_path().string();
    std::error_code ec;
    std::filesystem::create_directories(data_dir, ec);
    if (ec) {
        LOG(ERROR) << "GDS: failed to create data directory: " << data_dir
                   << ", error: " << ec.message();
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    // 1. Probe GDS availability
    if (!ProbeGdsAvailable(data_dir)) {
        return tl::make_unexpected(ErrorCode::GDS_NOT_AVAILABLE);
    }

    // 2. Open the data file (no O_DIRECT — cuFile handles alignment internally).
    // If a previous GDS run left data behind, unlink and create fresh.
    // O_TRUNC alone is insufficient: cuFile DMA can fail on NVMe blocks
    // that still have stale physical mappings from the old file.
    {
        struct stat existing_st;
        if (::stat(data_file_path.c_str(), &existing_st) == 0 &&
            existing_st.st_size > 0) {
            const char* allow_reopen =
                ::getenv("MOONCAKE_GDS_ALLOW_REOPEN");
            if (!allow_reopen || strcmp(allow_reopen, "1") != 0) {
                LOG(ERROR)
                    << "GDS: data file already exists ("
                    << existing_st.st_size
                    << " bytes). Refusing to overwrite. "
                    << "Remove the file manually or set "
                    << "MOONCAKE_GDS_ALLOW_REOPEN=1 to override.";
                return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
            }
            LOG(WARNING) << "GDS: removing existing data file ("
                         << existing_st.st_size
                         << " bytes), MOONCAKE_GDS_ALLOW_REOPEN=1 set";
            if (::unlink(data_file_path.c_str()) != 0) {
                LOG(ERROR) << "GDS: failed to unlink existing data file: "
                           << strerror(errno);
                return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
            }
        }
    }
    gds_fd_ = ::open(data_file_path.c_str(),
                     O_CLOEXEC | O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (gds_fd_ < 0) {
        LOG(ERROR) << "GDS: failed to open data file: " << data_file_path
                   << ", errno=" << errno << " (" << strerror(errno) << ")";
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    // 3. Pre-allocate physical blocks for cuFile DMA.
    // cuFile DMA bypasses the kernel write path and writes directly
    // to NVMe — it cannot extend a sparse file. posix_fallocate
    // guarantees real block allocation (unlike fallocate which may
    // produce sparse files on some ext4 kernel versions).
    int alloc_ret = ::posix_fallocate(gds_fd_, 0,
                                       static_cast<off_t>(capacity));
    if (alloc_ret != 0) {
        LOG(ERROR) << "GDS: posix_fallocate failed for "
                   << data_file_path << " (capacity=" << capacity
                   << "): errno=" << alloc_ret
                   << " (" << strerror(alloc_ret) << ")";
        ::close(gds_fd_);
        gds_fd_ = -1;
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    // 4. Register cuFile handle via abstraction layer
    auto status = gds_device_ops::FileHandleRegister(&cu_file_handle_, gds_fd_);
    if (status.IsErr()) {
        LOG(ERROR) << "GDS: FileHandleRegister failed: err=" << status.err;
        ::close(gds_fd_);
        gds_fd_ = -1;
        return tl::make_unexpected(ErrorCode::GDS_HANDLE_REGISTER_FAIL);
    }

    enabled_ = true;
    LOG(INFO) << "GDS initialized: native mode enabled, data_file="
              << data_file_path << ", capacity=" << capacity;
    return {};
#else
    (void)data_file_path; (void)capacity;
    return tl::make_unexpected(ErrorCode::GDS_NOT_AVAILABLE);
#endif
}

// ===================================================================
// GdsContext::ProbeGdsAvailable()
// ===================================================================
bool GdsContext::ProbeGdsAvailable(const std::string& data_dir) {
#ifdef USE_GDS_BACKEND
    // 1. Check device node via abstraction layer
    if (!gds_device_ops::ProbeDeviceNode()) {
        LOG(WARNING) << "GDS probe: device node not available";
        return false;
    }

    // 2. Driver open — process-level singleton via std::call_once.
    // If the first call fails the flag is marked "done" and no retry
    // occurs for the lifetime of this process.
    static std::once_flag gds_driver_once_;
    static bool gds_driver_ok_ = false;
    std::call_once(gds_driver_once_, []() {
        gds_driver_ok_ = gds_device_ops::DriverOpen().IsOk();
        if (!gds_driver_ok_)
            LOG(WARNING) << "GDS probe: DriverOpen failed, "
                         << "GDS will not be available for this process";
    });
    if (!gds_driver_ok_) return false;

    // 3. End-to-end DMA write/read/verify (RAII cleanup)
    std::string probe_path = data_dir + "/.gds_probe_" +
                             std::to_string(getpid());
    struct ProbeCleanup {
        std::string path;
        int fd = -1;
        gds_device_ops::GdsDeviceFileHandle fh = nullptr;
        void* gpu_buf = nullptr;       // write buffer (registered for GDS)
        bool   gpu_buf_ok = false;     // true if gpu_buf was successfully registered
        void* verify_buf = nullptr;    // read-back verify buffer (not GDS-registered)
        // Note: driver is managed by std::call_once at process level

        ~ProbeCleanup() {
            // Deregister buffers before freeing GPU memory, then
            // deregister file handle before closing fd (cuFile order).
            if (gpu_buf_ok) gds_device_ops::BufDeregister(gpu_buf);
            if (gpu_buf)    gds_device_ops::Free(gpu_buf);
            if (verify_buf) gds_device_ops::Free(verify_buf);
            if (fh)         gds_device_ops::FileHandleDeregister(fh);
            if (fd >= 0)    ::close(fd);
            ::unlink(path.c_str());
        }
    } cleanup{probe_path};

    // Create temporary file
    cleanup.fd = ::open(probe_path.c_str(),
                        O_CLOEXEC | O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (cleanup.fd < 0) {
        LOG(WARNING) << "GDS probe: cannot create probe file: "
                     << strerror(errno);
        return false;
    }

    // Register file handle
    if (!gds_device_ops::FileHandleRegister(&cleanup.fh, cleanup.fd).IsOk()) {
        LOG(WARNING) << "GDS probe: FileHandleRegister failed";
        return false;
    }

    // Allocate GPU buffer
    int probe_device = gds_device_ops::GetDevice();
    LOG(INFO) << "GDS probe: using GPU device " << probe_device;
    mooncake::gpu_staging::SetDevice(probe_device);

    cleanup.gpu_buf = gds_device_ops::Malloc(4096);
    if (!cleanup.gpu_buf) {
        LOG(WARNING) << "GDS probe: GPU Malloc failed";
        return false;
    }

    // Register GPU buffer — failure means probe failure (no bounce buffer)
    if (!gds_device_ops::BufRegister(cleanup.gpu_buf, 4096).IsOk()) {
        LOG(WARNING) << "GDS probe: BufRegister failed";
        return false;
    }
    cleanup.gpu_buf_ok = true;

    // 4. Write known pattern via DMA
    constexpr uint8_t kPattern = 0xA5;
    gds_device_ops::Memset(cleanup.gpu_buf, kPattern, 4096);
    gds_device_ops::DeviceSynchronize();
    if (gds_device_ops::Write(cleanup.fh, cleanup.gpu_buf, 4096, 0) != 4096) {
        LOG(WARNING) << "GDS probe: DMA write failed";
        return false;
    }

    // 5. Read back via DMA and verify byte-by-byte.
    // verify_buf is tracked by ProbeCleanup RAII — freed on all paths.
    // probe_device was set via gpu_staging::SetDevice() above; no device switches occur between Malloc and this point.
    cleanup.verify_buf = gds_device_ops::Malloc(4096);
    if (!cleanup.verify_buf) {
        LOG(WARNING) << "GDS probe: verify buffer Malloc failed";
        return false;
    }
    gds_device_ops::Memset(cleanup.verify_buf, 0, 4096);

    if (gds_device_ops::Read(cleanup.fh, cleanup.verify_buf, 4096, 0) != 4096) {
        LOG(WARNING) << "GDS probe: DMA read failed";
        return false;
    }

    std::vector<uint8_t> host(4096);
    if (!mooncake::gpu_staging::CopyDeviceToHost(host.data(),
                                                  cleanup.verify_buf, 4096)) {
        LOG(WARNING) << "GDS probe: CopyDeviceToHost failed";
        return false;
    }
    gds_device_ops::DeviceSynchronize();
    // verify_buf freed by ~ProbeCleanup()

    for (size_t i = 0; i < 4096; ++i) {
        if (host[i] != kPattern) {
            LOG(WARNING) << "GDS probe: data mismatch at byte " << i;
            return false;
        }
    }

    LOG(INFO) << "GDS probe: SUCCESS, native mode available";
    return true;
#else
    (void)data_dir;
    return false;
#endif
}

// ===================================================================
// GdsContext::WriteRecord()
// ===================================================================
tl::expected<void, ErrorCode> GdsContext::WriteRecord(
    const std::string& key,
    const std::vector<Slice>& slices,
    uint64_t offset) {
#ifdef USE_GDS_BACKEND
    uint32_t klen = static_cast<uint32_t>(key.size());
    size_t t = 0;
    for (const auto& s : slices) t += s.size;
    // Defensive: reject objects larger than 4 GiB. The caller
    // (OffsetAllocatorStorageBackend::BatchOffload) also checks this
    // but this guards against future direct callers.
    if (t > UINT32_MAX) {
        LOG(ERROR) << "WriteRecord: total value size " << t
                   << " exceeds UINT32_MAX for key " << key
                   << ", key_len=" << klen;
        return tl::make_unexpected(ErrorCode::GDS_IO_FAIL);
    }
    uint32_t vsz = static_cast<uint32_t>(t);

    MutexLocker io_lock(&io_mutex_);  // serialize record I/O

    RecordHeader hdr{.key_len = klen, .value_len = vsz};

    // header + key -> pwrite (CPU path, always)
    if (::pwrite(gds_fd_, &hdr, RecordHeader::SIZE,
                 static_cast<off_t>(offset)) != RecordHeader::SIZE)
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    if (::pwrite(gds_fd_, key.data(), klen,
                 static_cast<off_t>(offset + RecordHeader::SIZE))
        != static_cast<ssize_t>(klen))
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);

    // value slices
    gds_device_ops::GdsDeviceFileHandle cfh = cu_file_handle_;
    uint64_t vo = offset + RecordHeader::SIZE + klen;

    for (const auto& s : slices) {
        if (s.size == 0) continue;
        if (!s.ptr)
            return tl::make_unexpected(ErrorCode::FILE_INVALID_BUFFER);

        int dev = -1;
        if (mooncake::gpu_staging::IsDevicePointer(s.ptr, &dev)) {
            mooncake::gpu_staging::SetDevice(dev);

            // Use registration cache to avoid repeated Register/Deregister
            // for the same GPU address across multiple I/O operations.
            bool buf_ok = EnsureBufferRegistered(s.ptr, s.size);
            if (!buf_ok) {
                // Registration failed — cuFile will use internal bounce
                // buffer. Fall through to Write which handles this.
                VLOG(1) << "GDS WRITE: buffer not registered, relying on "
                        << "cuFile bounce buffer for ptr=" << s.ptr
                        << " size=" << s.size;
            }

            ssize_t w = gds_device_ops::Write(cfh, s.ptr, s.size,
                                               static_cast<off_t>(vo));
            LOG(INFO) << "[GDS WRITE] cuFileWrite DMA: size=" << s.size
                      << " offset=" << vo << " ret=" << w;
            if (w != static_cast<ssize_t>(s.size))
                return tl::make_unexpected(ErrorCode::GDS_IO_FAIL);
        } else {
            LOG(INFO) << "[GDS WRITE] pwrite fallback: size=" << s.size
                      << " offset=" << vo;
            if (::pwrite(gds_fd_, s.ptr, s.size, static_cast<off_t>(vo))
                != static_cast<ssize_t>(s.size))
                return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        vo += s.size;
    }
    return {};
#else
    (void)key; (void)slices; (void)offset;
    return tl::make_unexpected(ErrorCode::GDS_NOT_AVAILABLE);
#endif
}

// ===================================================================
// GdsContext::ReadRecord()
// ===================================================================
tl::expected<void, ErrorCode> GdsContext::ReadRecord(
    const std::string& key, Slice& dest_slice,
    uint64_t offset, uint32_t expected_value_size) {
#ifdef USE_GDS_BACKEND
    MutexLocker io_lock(&io_mutex_);  // serialize record I/O

    RecordHeader hdr;
    if (::pread(gds_fd_, &hdr, RecordHeader::SIZE,
                static_cast<off_t>(offset)) != RecordHeader::SIZE
        || hdr.value_len != expected_value_size)
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);

    std::string sk(hdr.key_len, '\0');
    if (::pread(gds_fd_, sk.data(), hdr.key_len,
                static_cast<off_t>(offset + RecordHeader::SIZE))
        != static_cast<ssize_t>(hdr.key_len) || sk != key)
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);

    gds_device_ops::GdsDeviceFileHandle cfh = cu_file_handle_;
    uint64_t vo = offset + RecordHeader::SIZE + hdr.key_len;
    int dev = -1;

    if (mooncake::gpu_staging::IsDevicePointer(dest_slice.ptr, &dev)) {
        mooncake::gpu_staging::SetDevice(dev);

        // Use registration cache to avoid repeated Register/Deregister.
        bool buf_ok = EnsureBufferRegistered(dest_slice.ptr,
                                              dest_slice.size);
        if (!buf_ok) {
            VLOG(1) << "GDS READ: buffer not registered, relying on "
                    << "cuFile bounce buffer for ptr=" << dest_slice.ptr
                    << " size=" << dest_slice.size;
        }

        ssize_t r = gds_device_ops::Read(cfh, dest_slice.ptr, dest_slice.size,
                                          static_cast<off_t>(vo));
        LOG(INFO) << "[GDS READ] cuFileRead DMA: size=" << dest_slice.size
                  << " offset=" << vo << " ret=" << r;
        if (r != static_cast<ssize_t>(dest_slice.size))
            return tl::make_unexpected(ErrorCode::GDS_IO_FAIL);
    } else {
        LOG(INFO) << "[GDS READ] pread fallback: size=" << dest_slice.size
                  << " offset=" << vo;
        if (::pread(gds_fd_, dest_slice.ptr, dest_slice.size,
                    static_cast<off_t>(vo))
            != static_cast<ssize_t>(dest_slice.size))
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    return {};
#else
    (void)key; (void)dest_slice; (void)offset; (void)expected_value_size;
    return tl::make_unexpected(ErrorCode::GDS_NOT_AVAILABLE);
#endif
}

// ===================================================================
// GdsContext::Shutdown()
// ===================================================================
void GdsContext::Shutdown() {
#ifdef USE_GDS_BACKEND
    // Release order: buffers -> handle -> fd
    // (cuFile requires buffers deregistered before handle deregister)
    for (auto& [ptr, _] : registered_buffers_) {
        gds_device_ops::BufDeregister(ptr);
    }
    registered_buffers_.clear();

    if (cu_file_handle_) {
        gds_device_ops::FileHandleDeregister(cu_file_handle_);
        cu_file_handle_ = nullptr;
    }

    if (gds_fd_ >= 0) {
        ::close(gds_fd_);
        gds_fd_ = -1;
    }

    enabled_ = false;
#endif
}

// ===================================================================
// GdsContext::EnsureBufferRegistered()
// ===================================================================
// Cache GPU buffer registrations to avoid repeated cuFileBufRegister /
// cuFileBufDeregister per-I/O. kv-cache blocks are reused across many
// Put operations at the same GPU addresses, so hit rate is near 100%.
bool GdsContext::EnsureBufferRegistered(void* gpu_ptr, size_t size) {
#ifdef USE_GDS_BACKEND
    MutexLocker lock(&buf_mutex_);

    auto it = registered_buffers_.find(gpu_ptr);
    if (it != registered_buffers_.end()) {
        if (it->second == size) return true;       // already registered, same size
        gds_device_ops::BufDeregister(gpu_ptr);    // size changed, re-register
        registered_buffers_.erase(it);
    }

    if (gds_device_ops::BufRegister(gpu_ptr, size).IsOk()) {
        registered_buffers_[gpu_ptr] = size;
        return true;
    }

    VLOG(1) << "BufRegister failed for ptr=" << gpu_ptr
            << " size=" << size << ", relying on cuFile bounce buffer";
    return false;
#else
    (void)gpu_ptr; (void)size;
    return false;
#endif
}

#ifdef USE_GDS_BACKEND
bool GdsContext::IsGdsAvailable() {
    return gds_device_ops::ProbeDeviceNode();
}
#endif

// ===================================================================
// USE_GDS_BACKEND=OFF stub implementations
// ===================================================================
#ifndef USE_GDS_BACKEND

tl::expected<void, ErrorCode> GdsContext::Init(
    const std::string&, uint64_t) {
    return tl::make_unexpected(ErrorCode::GDS_NOT_AVAILABLE);
}

void GdsContext::Shutdown() {}

bool GdsContext::ProbeGdsAvailable(const std::string&) {
    return false;
}

tl::expected<void, ErrorCode> GdsContext::WriteRecord(
    const std::string&, const std::vector<Slice>&, uint64_t) {
    return tl::make_unexpected(ErrorCode::GDS_NOT_AVAILABLE);
}

tl::expected<void, ErrorCode> GdsContext::ReadRecord(
    const std::string&, Slice&, uint64_t, uint32_t) {
    return tl::make_unexpected(ErrorCode::GDS_NOT_AVAILABLE);
}

bool GdsContext::EnsureBufferRegistered(void*, size_t) {
    return false;
}

bool GdsContext::IsGdsAvailable() {
    return false;
}

#endif  // !USE_GDS_BACKEND

}  // namespace mooncake
