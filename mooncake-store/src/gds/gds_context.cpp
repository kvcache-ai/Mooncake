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
#include <atomic>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <glog/logging.h>

#include "utils.h"    // GetEnvOr<>
#include <sys/uio.h>  // pwritev

#ifdef USE_GDS_BACKEND
#include "gds/gds_device_ops.h"
#include "device/accelerator_registry.h"
#include <cufile.h>
#endif

namespace mooncake {

static constexpr size_t kMaxRegisteredBuffers = 8192;

// ===================================================================
// GdsContext::Init()
// ===================================================================
tl::expected<void, ErrorCode> GdsContext::Init(
    const std::string& data_file_path, uint64_t capacity) {
#ifdef USE_GDS_BACKEND
    // 0. Lazy-init ops_
    if (!ops_) ops_ = CreateGdsDeviceOps();

    // 1. Create parent directory
    std::filesystem::path p(data_file_path);
    std::string data_dir = p.parent_path().string();
    std::error_code ec;
    std::filesystem::create_directories(data_dir, ec);
    if (ec) {
        LOG(ERROR) << "GDS: failed to create data directory: " << data_dir
                   << ", error: " << ec.message();
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    // 2. Probe GDS availability
    if (!ProbeGdsAvailable(data_dir)) {
        return tl::make_unexpected(ErrorCode::GDS_NOT_AVAILABLE);
    }

    // 3. Open the data file (no O_DIRECT — cuFile handles alignment
    // internally). If a previous GDS run left data behind, unlink and create
    // fresh. O_TRUNC alone is insufficient: cuFile DMA can fail on NVMe blocks
    // that still have stale physical mappings from the old file.
    {
        struct stat existing_st;
        if (::stat(data_file_path.c_str(), &existing_st) == 0 &&
            existing_st.st_size > 0) {
            const char* allow_reopen = ::getenv("MOONCAKE_GDS_ALLOW_REOPEN");
            if (!allow_reopen || strcmp(allow_reopen, "1") != 0) {
                LOG(ERROR) << "GDS: data file already exists ("
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

    // 4. Pre-allocate physical blocks for cuFile DMA.
    // cuFile DMA bypasses the kernel write path and writes directly
    // to NVMe — it cannot extend a sparse file. posix_fallocate
    // guarantees real block allocation (unlike fallocate which may
    // produce sparse files on some ext4 kernel versions).
    int alloc_ret = ::posix_fallocate(gds_fd_, 0, static_cast<off_t>(capacity));
    if (alloc_ret != 0) {
        LOG(ERROR) << "GDS: posix_fallocate failed for " << data_file_path
                   << " (capacity=" << capacity << "): errno=" << alloc_ret
                   << " (" << strerror(alloc_ret) << ")";
        ::close(gds_fd_);
        gds_fd_ = -1;
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    // 5. Register cuFile handle via ops_
    auto status = ops_->FileHandleRegister(&cu_file_handle_, gds_fd_);
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
    (void)data_file_path;
    (void)capacity;
    return tl::make_unexpected(ErrorCode::GDS_NOT_AVAILABLE);
#endif
}

// ===================================================================
// GdsContext::InitClientDma()
// Opens an existing data file for cuFile DMA. Used by vLLM in normal-mode
// + GDS to obtain a cuFile handle on the shared kv_cache.data.
// Does NOT posix_fallocate / O_TRUNC / I/O-probe — the file is owned by
// store_service.
// ===================================================================
tl::expected<void, ErrorCode> GdsContext::InitClientDma(
    const std::string& existing_file_path) {
#ifdef USE_GDS_BACKEND
    if (!ops_) ops_ = CreateGdsDeviceOps();
    if (!ops_->ProbeDeviceNode()) {
        LOG(WARNING) << "GDS InitClientDma: device node not available";
        return tl::make_unexpected(ErrorCode::GDS_NOT_AVAILABLE);
    }

    // DriverOpen — process-level singleton with optional retry.
    // Uses a manual double-checked lock pattern instead of std::call_once
    // so that MOONCAKE_GDS_RETRY_DRIVER=1 can force a fresh DriverOpen
    // attempt after a previous failure (e.g., nvidia-fs.ko loaded late).
    // NOTE: this is a *separate* singleton from GdsContext::Init() /
    // ProbeGdsAvailable(). If both are called in the same process,
    // DriverOpen() may be invoked twice — this is harmless because
    // cuFileDriverOpen() uses internal reference counting.
    static std::mutex driver_mutex;
    static bool driver_attempted = false;
    static bool driver_ok = false;
    auto* raw_ops = ops_.get();
    {
        bool should_retry = GetEnvOr<bool>("MOONCAKE_GDS_RETRY_DRIVER", false);
        std::lock_guard<std::mutex> lock(driver_mutex);
        if (!driver_attempted || (should_retry && !driver_ok)) {
            driver_ok = raw_ops->DriverOpen().IsOk();
            driver_attempted = true;
            if (!driver_ok)
                LOG(WARNING) << "GDS InitClientDma: DriverOpen failed";
            else if (should_retry)
                LOG(INFO) << "GDS InitClientDma: DriverOpen retry succeeded";
        }
    }
    if (!driver_ok) return tl::make_unexpected(ErrorCode::GDS_NOT_AVAILABLE);

    // Open the existing file — no O_CREAT, no O_TRUNC.
    // Guard against double-init: if a previous Init() or InitClientDma()
    // left gds_fd_ open, close it now to prevent fd leak.
    if (gds_fd_ >= 0) {
        LOG(WARNING) << "GDS InitClientDma: closing previous fd " << gds_fd_;
        // cuFile requires buffers deregistered before the file handle.
        {
            MutexLocker lock(&buf_mutex_);
            for (auto& [ptr, _] : registered_buffers_) {
                ops_->BufDeregister(ptr);
            }
            registered_buffers_.clear();
        }
        if (cu_file_handle_) {
            ops_->FileHandleDeregister(cu_file_handle_);
            cu_file_handle_ = nullptr;
        }
        ::close(gds_fd_);
        gds_fd_ = -1;
    }

    // Open the existing file — no O_CREAT, no O_TRUNC.
    gds_fd_ = ::open(existing_file_path.c_str(), O_CLOEXEC | O_RDWR);
    if (gds_fd_ < 0) {
        LOG(ERROR) << "GDS InitClientDma: cannot open " << existing_file_path
                   << ": " << strerror(errno);
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    auto status = ops_->FileHandleRegister(&cu_file_handle_, gds_fd_);
    if (status.IsErr()) {
        LOG(ERROR) << "GDS InitClientDma: FileHandleRegister failed: err="
                   << status.err;
        ::close(gds_fd_);
        gds_fd_ = -1;
        return tl::make_unexpected(ErrorCode::GDS_HANDLE_REGISTER_FAIL);
    }

    enabled_ = true;
    LOG(INFO) << "GDS InitClientDma: ready for DMA on " << existing_file_path;
    return {};
#else
    (void)existing_file_path;
    return tl::make_unexpected(ErrorCode::GDS_NOT_AVAILABLE);
#endif
}

// ===================================================================
// GdsContext::ProbeGdsAvailable()
// ===================================================================
bool GdsContext::ProbeGdsAvailable(const std::string& data_dir) {
#ifdef USE_GDS_BACKEND
    // 1. Check device node via abstraction layer
    if (!ops_) ops_ = CreateGdsDeviceOps();
    if (!ops_->ProbeDeviceNode()) {
        LOG(WARNING) << "GDS probe: device node not available";
        return false;
    }

    // 2. Driver open — process-level singleton with optional retry.
    // Uses a manual double-checked lock pattern so that
    // MOONCAKE_GDS_RETRY_DRIVER=1 can force a fresh attempt after
    // a previous failure (e.g., nvidia-fs.ko not yet loaded during
    // the first probe but loaded later by an admin).
    static std::mutex probe_driver_mutex;
    static bool probe_driver_attempted = false;
    static bool probe_driver_ok = false;
    auto* ops_raw = ops_.get();  // capture by value in lambda
    {
        bool should_retry = GetEnvOr<bool>("MOONCAKE_GDS_RETRY_DRIVER", false);
        std::lock_guard<std::mutex> lock(probe_driver_mutex);
        if (!probe_driver_attempted || (should_retry && !probe_driver_ok)) {
            probe_driver_ok = ops_raw->DriverOpen().IsOk();
            probe_driver_attempted = true;
            if (!probe_driver_ok)
                LOG(WARNING) << "GDS probe: DriverOpen failed, "
                             << "GDS will not be available for this process";
            else if (should_retry)
                LOG(INFO) << "GDS probe: DriverOpen retry succeeded";
        }
    }
    if (!probe_driver_ok) return false;

    // 3. End-to-end DMA write/read/verify (RAII cleanup)
    static std::atomic<uint64_t> probe_counter{0};
    std::string probe_path =
        data_dir + "/.gds_probe_" + std::to_string(getpid()) + "_" +
        std::to_string(probe_counter.fetch_add(1, std::memory_order_relaxed));
    struct ProbeCleanup {
        std::string path;
        int fd = -1;
        GdsDeviceFileHandle fh = nullptr;
        void* gpu_buf = nullptr;  // write buffer (registered for GDS)
        bool gpu_buf_ok = false;  // true if gpu_buf was successfully registered
        GdsDeviceOps* ops = nullptr;  // for cleanup (not owned)
        // Note: driver is managed by std::call_once at process level

        ~ProbeCleanup() {
            // Deregister buffers before freeing GPU memory, then
            // deregister file handle before closing fd (cuFile order).
            if (gpu_buf_ok && ops) ops->BufDeregister(gpu_buf);
            if (gpu_buf && ops) ops->Free(gpu_buf);
            if (fh && ops) ops->FileHandleDeregister(fh);
            if (fd >= 0) ::close(fd);
            ::unlink(path.c_str());
        }
    } cleanup{probe_path, -1, nullptr, nullptr, false, ops_.get()};

    // Create temporary file
    cleanup.fd = ::open(probe_path.c_str(),
                        O_CLOEXEC | O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (cleanup.fd < 0) {
        LOG(WARNING) << "GDS probe: cannot create probe file: "
                     << strerror(errno);
        return false;
    }

    // Register file handle
    if (!ops_->FileHandleRegister(&cleanup.fh, cleanup.fd).IsOk()) {
        LOG(WARNING) << "GDS probe: FileHandleRegister failed";
        return false;
    }

    // Allocate GPU buffer. Use cudaSetDevice directly — avoids any
    // virtual dispatch on ops_ (which may not be fully constructed
    // when called from a static initializer context).
    int probe_device = ops_->GetDevice();
    LOG(INFO) << "GDS probe: using GPU device " << probe_device;
    ops_->SetDevice(probe_device);

    cleanup.gpu_buf = ops_->Malloc(4096);
    if (!cleanup.gpu_buf) {
        LOG(WARNING) << "GDS probe: GPU Malloc failed";
        return false;
    }

    // Register GPU buffer — failure means probe failure (no bounce buffer)
    if (!ops_->BufRegister(cleanup.gpu_buf, 4096).IsOk()) {
        LOG(WARNING) << "GDS probe: BufRegister failed";
        return false;
    }
    cleanup.gpu_buf_ok = true;

    // 4. Write known pattern via DMA
    constexpr uint8_t kPattern = 0xA5;
    ops_->Memset(cleanup.gpu_buf, kPattern, 4096);
    ops_->DeviceSynchronize();
    if (ops_->Write(cleanup.fh, cleanup.gpu_buf, 4096, 0) != 4096) {
        LOG(WARNING) << "GDS probe: DMA write failed";
        return false;
    }

    // 5. Read back via DMA and verify byte-by-byte.
    // Reuse gpu_buf (already registered with cuFile): zero it, read
    // back via DMA, then D2H copy and compare. Avoids allocating a
    // separate buffer that would use the internal bounce-buffer path
    // because it is never registered with BufRegister.
    // probe_device was set via SetContext() above; no device
    // switches occur between Malloc and this point.
    ops_->Memset(cleanup.gpu_buf, 0, 4096);
    ops_->DeviceSynchronize();

    if (ops_->Read(cleanup.fh, cleanup.gpu_buf, 4096, 0) != 4096) {
        LOG(WARNING) << "GDS probe: DMA read failed";
        return false;
    }

    std::vector<uint8_t> host(4096);
    ops_->CopyDeviceToHost(host.data(), cleanup.gpu_buf, 4096);
    ops_->DeviceSynchronize();

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
// Header-last: value DMA before header.  Any failure leaves the
// placeholder intact, so recovery rejects the torn record.
tl::expected<void, ErrorCode> GdsContext::WriteRecord(
    const std::string& key, const std::vector<Slice>& slices, uint64_t offset,
    uint64_t seq) {
#ifdef USE_GDS_BACKEND
    if (key.size() > UINT32_MAX) {
        LOG(ERROR) << "WriteRecord: key size " << key.size()
                   << " exceeds UINT32_MAX";
        return tl::make_unexpected(ErrorCode::GDS_IO_FAIL);
    }
    uint32_t klen = static_cast<uint32_t>(key.size());
    size_t t = 0;
    for (const auto& s : slices) t += s.size;
    if (t > UINT32_MAX) {
        LOG(ERROR) << "WriteRecord: total value size " << t
                   << " exceeds UINT32_MAX for key " << key
                   << ", key_len=" << klen;
        return tl::make_unexpected(ErrorCode::GDS_IO_FAIL);
    }
    uint32_t vsz = static_cast<uint32_t>(t);

    SharedMutexLocker io_lock(&io_mutex_, shared_lock);

    // Value region (DMA/pwrite) BEFORE header+key+pad.
    GdsDeviceFileHandle cfh = cu_file_handle_;
    uint64_t vo = offset + RecordHeader::ValueOffsetInRecord(klen);

    size_t coalesced_start = 0;
    size_t coalesced_size = 0;
    bool coalescing_gpu = false;

    static const bool no_merge = GetEnvOr<bool>("MOONCAKE_GDS_NO_MERGE", false);
    auto runtime = device::GetAcceleratorRegistry().RuntimeAccelerators(true);

    auto flush_coalesced_group =
        [&](size_t upto) -> tl::expected<void, ErrorCode> {
        if (!coalescing_gpu || coalesced_size == 0) return {};

        bool buf_ok =
            EnsureBufferRegistered(slices[coalesced_start].ptr, coalesced_size);
        if (!buf_ok) {
            VLOG(1) << "GDS WRITE: coalesced buffer not registered, relying on"
                    << " cuFile bounce buffer for ptr="
                    << slices[coalesced_start].ptr
                    << " size=" << coalesced_size;
        }

        ssize_t w = ops_->Write(cfh, slices[coalesced_start].ptr,
                                coalesced_size, static_cast<off_t>(vo));
        const int saved_errno = (w == -1) ? errno : 0;
        VLOG(1) << "[GDS WRITE] cuFileWrite DMA (coalesced "
                << (upto - coalesced_start)
                << " slices): size=" << coalesced_size << " offset=" << vo
                << " ret=" << w;
        if (w != static_cast<ssize_t>(coalesced_size)) {
            if (w == -1) {
                char err_buf[128];
                VLOG(1) << "[GDS WRITE] cuFileWrite errno=" << saved_errno
                        << " ("
                        << strerror_r(saved_errno, err_buf, sizeof(err_buf))
                        << ")" << " offset=" << vo
                        << " size=" << coalesced_size;
            } else if (w < 0) {
                VLOG(1) << "[GDS WRITE] cuFileWrite cu_err=" << w << " ("
                        << cufileop_status_error(static_cast<CUfileOpError>(w))
                        << ")";
            }
            return tl::make_unexpected(ErrorCode::GDS_IO_FAIL);
        }

        ::posix_fadvise(gds_fd_, static_cast<off_t>(vo),
                        static_cast<off_t>(coalesced_size),
                        POSIX_FADV_DONTNEED);

        vo += coalesced_size;
        coalesced_size = 0;
        coalescing_gpu = false;
        return {};
    };

    for (size_t i = 0; i < slices.size(); ++i) {
        const auto& s = slices[i];
        if (s.size == 0) continue;
        if (!s.ptr) return tl::make_unexpected(ErrorCode::FILE_INVALID_BUFFER);

        device::PointerInfo wr_info;
        const auto* wr_dev = runtime.FindDeviceForPointer(s.ptr, &wr_info);
        if (wr_dev) {
            wr_dev->SetContext(wr_info.device_id);

            if (!no_merge && coalescing_gpu &&
                static_cast<char*>(slices[coalesced_start].ptr) +
                        coalesced_size ==
                    static_cast<char*>(s.ptr)) {
                coalesced_size += s.size;
                continue;
            }
            {
                auto rc = flush_coalesced_group(i);
                if (!rc) return rc;
            }
            coalesced_start = i;
            coalesced_size = s.size;
            coalescing_gpu = true;
        } else {
            auto rc = flush_coalesced_group(i);
            if (!rc) return rc;

            if (runtime.FindDeviceForPointer(s.ptr) != nullptr) {
                LOG(ERROR) << "GDS WRITE: device pointer " << s.ptr
                           << " not matched by main lookup but found"
                           << " by safety check; refusing pwrite";
                return tl::make_unexpected(ErrorCode::GDS_IO_FAIL);
            }
            VLOG(1) << "[GDS WRITE] pwrite fallback: size=" << s.size
                    << " offset=" << vo;
            if (::pwrite(gds_fd_, s.ptr, s.size, static_cast<off_t>(vo)) !=
                static_cast<ssize_t>(s.size))
                return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
            vo += s.size;
        }
    }
    auto rc = flush_coalesced_group(slices.size());
    if (!rc) return rc;

    // Header + key + pad -> single pwritev (COMMIT POINT)
    RecordHeader hdr{
        .key_len = klen, .value_len = vsz, .seq = seq, .flags = 0, .crc32 = 0};
    char hdr_buf[RecordHeader::SIZE];
    hdr.WriteTo(hdr_buf);

    const uint32_t pad = RecordHeader::ValuePadding(klen);
    static const char kZeroPad[RecordHeader::kValueAlignment] = {};
    struct iovec head_iovs[3] = {
        {hdr_buf, RecordHeader::SIZE},
        {const_cast<char*>(key.data()), static_cast<size_t>(klen)},
        {const_cast<char*>(kZeroPad), static_cast<size_t>(pad)},
    };
    struct iovec* iovp = head_iovs;
    int niov = (pad > 0) ? 3 : 2;
    off_t head_off = static_cast<off_t>(offset);
    const size_t head_total = RecordHeader::SIZE + klen + pad;
    size_t head_written = 0;
    while (niov > 0) {
        ssize_t n = ::pwritev(gds_fd_, iovp, niov, head_off);
        if (n < 0) {
            if (errno == EINTR) continue;
            {
                RecordHeader t{
                    .key_len = 0,
                    .value_len = 0,
                    .seq = 0,
                    .flags = RecordHeader::kFlagReservationPlaceholder,
                    .crc32 = 0};
                char tb[RecordHeader::SIZE];
                t.WriteTo(tb);
                ssize_t tw = ::pwrite(gds_fd_, tb, RecordHeader::SIZE,
                                      static_cast<off_t>(offset));
                (void)tw;
            }
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        if (n == 0) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        head_written += static_cast<size_t>(n);
        head_off += n;
        while (niov > 0 && static_cast<size_t>(n) >= iovp[0].iov_len) {
            n -= static_cast<ssize_t>(iovp[0].iov_len);
            ++iovp;
            --niov;
        }
        if (niov > 0 && n > 0) {
            iovp[0].iov_base = static_cast<char*>(iovp[0].iov_base) + n;
            iovp[0].iov_len -= static_cast<size_t>(n);
        }
    }
    if (head_written != head_total)
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    return {};
#else
    (void)key;
    (void)slices;
    (void)offset;
    (void)seq;
    return tl::make_unexpected(ErrorCode::GDS_NOT_AVAILABLE);
#endif
}

// GdsContext::ReadRecord()
// ===================================================================
tl::expected<void, ErrorCode> GdsContext::ReadRecord(
    const std::string& key, const std::vector<Slice>& dest_slices,
    uint64_t offset, uint32_t expected_value_size) {
#ifdef USE_GDS_BACKEND
    // Shared mode: concurrent reads/writes use explicit offsets;
    // Shutdown() takes the exclusive mode to drain us.
    SharedMutexLocker io_lock(&io_mutex_, shared_lock);

    char hdr_buf[RecordHeader::SIZE];
    if (::pread(gds_fd_, hdr_buf, RecordHeader::SIZE,
                static_cast<off_t>(offset)) != RecordHeader::SIZE)
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    RecordHeader hdr = RecordHeader::ReadFrom(hdr_buf);
    if ((hdr.flags & ~RecordHeader::kKnownFlags) != 0) {
        LOG(ERROR) << "ReadRecord: unknown flags " << hdr.flags << " at offset "
                   << offset;
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    if (hdr.value_len != expected_value_size)
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);

    if (hdr.key_len > 65536) {
        LOG(ERROR) << "ReadRecord: key_len " << hdr.key_len
                   << " exceeds limit (corrupted record at offset " << offset
                   << ")";
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    std::string sk(hdr.key_len, '\0');
    if (::pread(gds_fd_, sk.data(), hdr.key_len,
                static_cast<off_t>(offset + RecordHeader::SIZE)) !=
            static_cast<ssize_t>(hdr.key_len) ||
        sk != key)
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);

    // The destination slices must exactly cover the stored value.
    size_t total = 0;
    for (const auto& s : dest_slices) total += s.size;
    if (total != expected_value_size) {
        LOG(ERROR) << "ReadRecord: destination size " << total
                   << " != stored value size " << expected_value_size
                   << " for key " << key;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    GdsDeviceFileHandle cfh = cu_file_handle_;
    uint64_t vo = offset + RecordHeader::ValueOffsetInRecord(hdr.key_len);
    auto rd_runtime =
        device::GetAcceleratorRegistry().RuntimeAccelerators(true);

    for (const auto& dest_slice : dest_slices) {
        if (dest_slice.size == 0) continue;
        if (!dest_slice.ptr)
            return tl::make_unexpected(ErrorCode::FILE_INVALID_BUFFER);

        device::PointerInfo rd_info;
        const auto* rd_dev =
            rd_runtime.FindDeviceForPointer(dest_slice.ptr, &rd_info);

        if (rd_dev) {
            rd_dev->SetContext(rd_info.device_id);

            // Use registration cache to avoid repeated Register/Deregister.
            bool buf_ok =
                EnsureBufferRegistered(dest_slice.ptr, dest_slice.size);
            if (!buf_ok) {
                VLOG(1) << "GDS READ: buffer not registered, relying on "
                        << "cuFile bounce buffer for ptr=" << dest_slice.ptr
                        << " size=" << dest_slice.size;
            }

            ssize_t r = ops_->Read(cfh, dest_slice.ptr, dest_slice.size,
                                   static_cast<off_t>(vo));
            VLOG(1) << "[GDS READ] cuFileRead DMA: size=" << dest_slice.size
                    << " offset=" << vo << " ret=" << r;
            if (r != static_cast<ssize_t>(dest_slice.size))
                return tl::make_unexpected(ErrorCode::GDS_IO_FAIL);
        } else {
            // Safety: verify this is truly CPU memory before pread.
            auto safety_rt =
                device::GetAcceleratorRegistry().RuntimeAccelerators(true);
            if (safety_rt.FindDeviceForPointer(dest_slice.ptr) != nullptr) {
                LOG(ERROR) << "GDS READ: device pointer " << dest_slice.ptr
                           << " not matched by main lookup but found"
                           << " by safety check; refusing pread";
                return tl::make_unexpected(ErrorCode::GDS_IO_FAIL);
            }
            VLOG(1) << "[GDS READ] pread fallback: size=" << dest_slice.size
                    << " offset=" << vo;
            if (::pread(gds_fd_, dest_slice.ptr, dest_slice.size,
                        static_cast<off_t>(vo)) !=
                static_cast<ssize_t>(dest_slice.size))
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        vo += dest_slice.size;
    }
    return {};
#else
    (void)key;
    (void)dest_slices;
    (void)offset;
    (void)expected_value_size;
    return tl::make_unexpected(ErrorCode::GDS_NOT_AVAILABLE);
#endif
}

// ===================================================================
// GdsContext::Shutdown()
// ===================================================================
void GdsContext::Shutdown() {
#ifdef USE_GDS_BACKEND
    if (!ops_) {
        // Init() was never called or failed before creating ops_.
        enabled_ = false;
        return;
    }

    // Hold buf_mutex_ to prevent data race with EnsureBufferRegistered()
    // if Shutdown() is called while an I/O thread is still running.
    {
        MutexLocker lock(&buf_mutex_);
        for (auto& [ptr, size] : registered_buffers_) {
            (void)size;
            ops_->BufDeregister(ptr);
        }
        registered_buffers_.clear();
    }

    // Drain any in-flight WriteRecord/ReadRecord (they hold io_mutex_ in
    // shared mode) before deregistering the file handle and closing the
    // fd — tearing those down mid-DMA is UB in cuFile.
    {
        SharedMutexLocker io_lock(&io_mutex_);
        if (cu_file_handle_) {
            ops_->FileHandleDeregister(cu_file_handle_);
            cu_file_handle_ = nullptr;
        }

        if (gds_fd_ >= 0) {
            ::close(gds_fd_);
            gds_fd_ = -1;
        }
    }

    enabled_ = false;
    ops_.reset();
#endif
}

// ===================================================================

// GdsContext::IsRangeCovered()
// ===================================================================
// Caller must hold buf_mutex_.  Updates the extent's LRU tick on hit.
bool GdsContext::IsRangeCovered(void* ptr, size_t size) {
    auto it = registered_buffers_.upper_bound(ptr);
    if (it == registered_buffers_.begin()) return false;
    --it;
    const char* base = static_cast<const char*>(it->first);
    const char* p = static_cast<const char*>(ptr);
    if (p >= base && static_cast<size_t>(p - base) + size <= it->second.size) {
        it->second.lru_tick = ++lru_clock_;
        return true;
    }
    return false;
}

// ===================================================================
// GdsContext::RegisterAndCache()
// ===================================================================
bool GdsContext::RegisterAndCache(void* gpu_ptr, size_t size) {
#ifdef USE_GDS_BACKEND
    // Snap the registration to the whole GPU allocation containing
    // gpu_ptr.  Request-shaped registrations churn with coalescing
    // boundaries (same base, different size -> dereg + re-register every
    // few requests), which fragments the nvidia-fs driver's internal
    // BAR1 mappings over long runs.  Allocation-snapped extents are
    // disjoint and stable, so steady-state I/O is pure cache hits.
    // Only snap when the requested span lies fully inside the reported
    // allocation; a coalesced span crossing allocation boundaries keeps
    // span-shaped registration.
    void* reg_base = gpu_ptr;
    size_t reg_size = size;
    {
        void* alloc_base = nullptr;
        size_t alloc_size = 0;
        if (ops_->GetAddressRange(gpu_ptr, &alloc_base, &alloc_size) &&
            alloc_size > 0 &&
            static_cast<char*>(alloc_base) <= static_cast<char*>(gpu_ptr) &&
            static_cast<char*>(gpu_ptr) + size <=
                static_cast<char*>(alloc_base) + alloc_size) {
            reg_base = alloc_base;
            reg_size = alloc_size;
        }
    }

    // Overlap protection: deregister ALL extents intersecting the target
    // range before registering (left-crossing, fully-contained, AND
    // right-crossing — the old code skipped right-crossing extents, so
    // the subsequent BufRegister overlapped a live registration and
    // failed, silently degrading to the bounce buffer).  With
    // allocation snapping this only fires for span-shaped leftovers.
    {
        const char* p = static_cast<const char*>(reg_base);
        const char* p_end = p + reg_size;
        auto it = registered_buffers_.lower_bound(reg_base);
        if (it != registered_buffers_.begin()) {
            auto prev = std::prev(it);
            const char* prev_base = static_cast<const char*>(prev->first);
            if (prev_base + prev->second.size > p) {
                ops_->BufDeregister(prev->first);
                registered_buffers_.erase(prev);
            }
        }
        while (it != registered_buffers_.end()) {
            const char* base = static_cast<const char*>(it->first);
            if (base >= p_end) break;
            ops_->BufDeregister(it->first);
            it = registered_buffers_.erase(it);
        }
    }

    // Soft-cap LRU eviction.  With allocation snapping the live extent
    // count equals the number of live GPU allocations and should never
    // reach the cap — a firing eviction means snapping is not working
    // (e.g. GetAddressRange unsupported), so make it visible.
    while (registered_buffers_.size() >= kMaxRegisteredBuffers) {
        auto oldest = registered_buffers_.begin();
        for (auto it = registered_buffers_.begin();
             it != registered_buffers_.end(); ++it) {
            if (it->second.lru_tick < oldest->second.lru_tick) oldest = it;
        }
        LOG_EVERY_N(WARNING, 1000)
            << "GDS buffer registration cache full (" << kMaxRegisteredBuffers
            << "), evicting LRU extent base=" << oldest->first
            << " size=" << oldest->second.size
            << " — allocation snapping may be failing";
        ops_->BufDeregister(oldest->first);
        registered_buffers_.erase(oldest);
    }

    if (ops_->BufRegister(reg_base, reg_size).IsOk()) {
        registered_buffers_[reg_base] =
            RegisteredExtent{reg_size, ++lru_clock_};
        return true;
    }

    LOG_EVERY_N(WARNING, 100)
        << "GDS BufRegister failed for ptr=" << reg_base << " size=" << reg_size
        << " (registered_buffers_ size=" << registered_buffers_.size()
        << "), relying on cuFile bounce buffer";
    return false;
#else
    (void)gpu_ptr;
    (void)size;
    return false;
#endif
}

// ===================================================================
// GdsContext::EnsureBufferRegistered()
// ===================================================================
// Range-aware registration cache.  Lookup checks exact match, then range
// containment (handles per-slice lookups after coalescing).  Misses are
// registered allocation-snapped (see RegisterAndCache).
bool GdsContext::EnsureBufferRegistered(void* gpu_ptr, size_t size) {
#ifdef USE_GDS_BACKEND
    MutexLocker lock(&buf_mutex_);

    auto it = registered_buffers_.find(gpu_ptr);
    if (it != registered_buffers_.end()) {
        if (it->second.size == size) {
            it->second.lru_tick = ++lru_clock_;
            return true;
        }
        // Same base with a different size: a span-shaped leftover —
        // replace it with an allocation-snapped registration.
        ops_->BufDeregister(gpu_ptr);
        registered_buffers_.erase(it);
        return RegisterAndCache(gpu_ptr, size);
    }

    if (IsRangeCovered(gpu_ptr, size)) return true;

    return RegisterAndCache(gpu_ptr, size);
#else
    (void)gpu_ptr;
    (void)size;
    return false;
#endif
}

bool GdsContext::IsGdsAvailable() {
#ifdef USE_GDS_BACKEND
    auto probe_ops = CreateGdsDeviceOps();
    return probe_ops->ProbeDeviceNode();
#else
    return false;
#endif
}

}  // namespace mooncake
