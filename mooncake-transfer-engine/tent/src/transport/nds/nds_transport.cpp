// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#include "tent/transport/nds/nds_transport.h"

#include <ascend_nds.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <iterator>
#include <limits>
#include <map>
#include <mutex>
#include <vector>

namespace mooncake::tent {

namespace {

constexpr size_t kDefaultBatchDepth = 128;
constexpr size_t kAlignment = 4096;
constexpr size_t kSliceSize = 16 * 1024 * 1024;

Status ndsError(const char* operation, int rc) {
    return Status::InternalError(std::string(operation) + " returned " +
                                 std::to_string(rc));
}

Status getRegularFileSize(int fd, uint64_t& length) {
    struct stat st{};
    if (fstat(fd, &st)) return ndsError("fstat", -errno);
    if (S_ISREG(st.st_mode) && st.st_size >= 0) {
        length = st.st_size;
        return Status::OK();
    }
    return Status::InvalidArgument("NDS requires a regular file");
}

bool contains(uint64_t base, size_t size, uint64_t addr, size_t length) {
    return addr >= base && addr - base <= size &&
           length <= size - (addr - base);
}

// Native initialization is process-wide and not reentrant. Serialize lifecycle,
// registrations and queue access with mutex(); polling is nonblocking.
struct NdsRuntime {
    static NdsRuntime& instance() {
        static NdsRuntime driver;
        return driver;
    }

    std::mutex& mutex() { return mutex_; }

    Status init() {
        if (users_ == 0) {
            int rc = ndsFileDriverOpen();
            if (rc) return ndsError("ndsFileDriverOpen", rc);
        }
        ++users_;
        return Status::OK();
    }

    Status uninit() {
        if (users_ == 1) {
            int rc = ndsFileDriverClose();
            if (rc) return ndsError("ndsFileDriverClose", rc);
            for (const auto& [device, fd] : fds_) ::close(fd);
            fds_.clear();
        }
        --users_;
        return Status::OK();
    }

    Status registerFd(int fd, dev_t device) {
        if (fds_.count(device)) return Status::OK();
        // Keep the filesystem fd valid until the driver closes.
        int fs_fd = fcntl(fd, F_DUPFD_CLOEXEC, 0);
        if (fs_fd < 0) return ndsError("fcntl(F_DUPFD_CLOEXEC)", -errno);
        NDSfileFsDesc_t fs{&fs_fd, 1, 0};
        int rc = ndsFileRegisterFs(&fs);
        if (rc) {
            ::close(fs_fd);
            return ndsError("ndsFileRegisterFs", rc);
        }
        fds_.emplace(device, fs_fd);
        return Status::OK();
    }

    Status registerBuffer(uint64_t addr, uint64_t length) {
        auto next = buffers_.lower_bound(addr);
        bool shared = next != buffers_.end() && next->first == addr &&
                      next->second.first == length;
        if (!shared &&
            ((next != buffers_.end() && next->first < addr + length) ||
             (next != buffers_.begin() &&
              std::prev(next)->first + std::prev(next)->second.first > addr)))
            return Status::InvalidArgument("Overlapping NDS registrations");
        if (!shared) {
            int rc =
                ndsFileBufRegister(reinterpret_cast<void*>(addr), length, 0);
            if (rc) return ndsError("ndsFileBufRegister", rc);
            next =
                buffers_.emplace(addr, std::make_pair(length, size_t{0})).first;
        }
        ++next->second.second;
        return Status::OK();
    }

    Status unregisterBuffer(uint64_t addr) {
        auto shared = buffers_.find(addr);
        auto& users = shared->second.second;
        // Release the shared pin after the last user has drained its I/O.
        if (users == 1) {
            int rc = ndsFileBufDeregister(reinterpret_cast<void*>(addr));
            if (rc) return ndsError("ndsFileBufDeregister", rc);
        }
        if (--users == 0) buffers_.erase(shared);
        return Status::OK();
    }

   private:
    NdsRuntime() = default;

    std::mutex mutex_;
    size_t users_ = 0;
    // <addr, <length, users>>
    std::map<uint64_t, std::pair<size_t, size_t>> buffers_;
    std::map<dev_t, int> fds_;
};

struct NdsIoEntry {
    NDSfileIOParams_t params{};
    std::vector<NDSfileIOVec_t> iovs;
    // <task_id, bytes>
    std::vector<std::pair<size_t, size_t>> parts;
    std::shared_ptr<NdsFileContext> file_ctx_ptr;
    size_t length = 0;
    bool done = false;
};

}  // namespace

class NdsFileContext {
   public:
    NdsFileContext() = default;

    NdsFileContext(const NdsFileContext&) = delete;

    NdsFileContext& operator=(const NdsFileContext&) = delete;

    ~NdsFileContext() {
        if (handle_) ndsFileHandleDeregister(handle_);
        if (fd_ >= 0) close(fd_);
    }

    Status open(const std::string& path, int access) {
        fd_ = ::open(path.c_str(), access | O_DIRECT | O_CLOEXEC);
        if (fd_ < 0) return ndsError("open", -errno);
        if (fstat(fd_, &identity_)) return ndsError("fstat", -errno);
        if (!S_ISREG(identity_.st_mode))
            return Status::InvalidArgument("NDS requires a regular file");
        CHECK_STATUS(NdsRuntime::instance().registerFd(fd_, identity_.st_dev));
        NDSfileDescr_t desc{};
        desc.type = NDS_FILE_HANDLE_TYPE_OPAQUE_FD;
        desc.handle.fd = fd_;
        int rc = ndsFileHandleRegister(&handle_, &desc);
        if (rc) return ndsError("ndsFileHandleRegister", rc);
        return Status::OK();
    }

    int getFd() const { return fd_; }

    NDSfileHandle_t getHandle() const { return handle_; }

    bool matches(const struct stat& current) const {
        return current.st_dev == identity_.st_dev &&
               current.st_ino == identity_.st_ino &&
               current.st_rdev == identity_.st_rdev;
    }

   private:
    int fd_ = -1;
    NDSfileHandle_t handle_ = nullptr;
    struct stat identity_{};
};

class NdsSubBatch : public Transport::SubBatch {
   public:
    explicit NdsSubBatch(size_t capacity) : capacity_(capacity) {}

    size_t size() const override { return requests_.size(); }

    bool needsProgressPolling() const override { return true; }

   private:
    friend class NdsTransport;

    bool hasPendingTasksForBuffer(uint64_t base) const {
        for (size_t i = 0; i < size(); ++i) {
            if (statuses_[i].s == PENDING && buffer_bases_[i] == base)
                return true;
        }
        return false;
    }

    size_t capacity_;
    // The task ID indexes all three vectors.
    std::vector<Request> requests_;
    std::vector<uint64_t> buffer_bases_;
    std::vector<TransferStatus> statuses_;
    std::vector<std::unique_ptr<NdsIoEntry>> ios_;
    NDSfileBatchHandle_t context_ = nullptr;
    size_t submitted_ = 0;
    size_t inflight_ = 0;
    std::vector<NDSfileIOParams_t> submission_;
    std::vector<NDSfileIOEvents_t> events_;
};

// Transport lifecycle

NdsTransport::NdsTransport()
    : depth_(kDefaultBatchDepth), io_bytes_(kSliceSize) {}

NdsTransport::~NdsTransport() {
    auto status = uninstall();
    CHECK(status.ok()) << status.ToString();
    for (auto* batch : batches_) delete batch;
}

Status NdsTransport::install(std::string&,
                             std::shared_ptr<ControlService> metadata,
                             std::shared_ptr<Topology>,
                             std::shared_ptr<Config> conf) {
    auto& runtime = NdsRuntime::instance();
    std::lock_guard<std::mutex> lock(runtime.mutex());
    if (installed_) return Status::OK();
    if (!metadata) return Status::InvalidArgument("NDS requires metadata");
    int depth = conf ? conf->get("transports/nds/io_batch_depth",
                                 int(kDefaultBatchDepth))
                     : kDefaultBatchDepth;
    int bytes = conf ? conf->get("transports/nds/io_bytes", int(kSliceSize))
                     : kSliceSize;
    if (depth <= 0 || depth > int(NDS_FILE_MAX_BATCH_IO))
        return Status::InvalidArgument(
            "NDS io_batch_depth must be in [1, 1024]");
    if (bytes <= 0 || bytes % kAlignment || bytes > 64 * 1024 * 1024)
        return Status::InvalidArgument(
            "NDS io_bytes must be a multiple of 4 KiB in [4 KiB, 64 MiB]");

    CHECK_STATUS(runtime.init());
    depth_ = depth;
    io_bytes_ = bytes;
    metadata_ = std::move(metadata);
    caps.gpu_to_file = Platform::getLoader().type() == "ascend";
    installed_ = true;
    return Status::OK();
}

Status NdsTransport::quiesce() {
    std::lock_guard<std::mutex> lock(NdsRuntime::instance().mutex());
    abortAllSubBatches();
    return Status::OK();
}

Status NdsTransport::uninstall() {
    auto& runtime = NdsRuntime::instance();
    std::lock_guard<std::mutex> lock(runtime.mutex());
    if (!installed_) return Status::OK();
    abortAllSubBatches();
    while (!buffers_.empty())
        CHECK_STATUS(unregisterBuffer(buffers_.begin()->first));
    for (auto* context : idle_contexts_) {
        int rc = ndsFileBatchIODestroy(context);
        if (rc) LOG(ERROR) << "ndsFileBatchIODestroy returned " << rc;
    }
    idle_contexts_.clear();
    for (auto* batch : batches_) batch->ios_.clear();
    files_.clear();

    CHECK_STATUS(runtime.uninit());
    installed_ = false;
    metadata_.reset();
    return Status::OK();
}

// Sub-batch lifecycle

Status NdsTransport::allocateSubBatch(SubBatchRef& batch, size_t max_size) {
    std::lock_guard<std::mutex> lock(NdsRuntime::instance().mutex());
    if (!installed_ || !max_size)
        return Status::InvalidArgument(
            "NDS is unavailable or batch size is zero");
    auto* allocated = new NdsSubBatch(max_size);
    batches_.insert(allocated);
    batch = allocated;
    return Status::OK();
}

Status NdsTransport::freeSubBatch(SubBatchRef& batch) {
    std::lock_guard<std::mutex> lock(NdsRuntime::instance().mutex());
    auto* nds_batch = dynamic_cast<NdsSubBatch*>(batch);
    if (!nds_batch || !batches_.count(nds_batch))
        return Status::InvalidArgument("Invalid NDS batch");
    if (nds_batch->context_ && !nds_batch->inflight_ &&
        nds_batch->submitted_ == nds_batch->ios_.size() &&
        idle_contexts_.size() < 32) {
        idle_contexts_.push_back(nds_batch->context_);
        nds_batch->context_ = nullptr;
    } else {
        abortSubBatch(*nds_batch);
    }
    batches_.erase(nds_batch);
    delete nds_batch;
    batch = nullptr;
    return Status::OK();
}

Status NdsTransport::acquireBatchContext(NdsSubBatch& batch) {
    if (batch.context_) return Status::OK();
    if (!idle_contexts_.empty()) {
        batch.context_ = idle_contexts_.back();
        idle_contexts_.pop_back();
        return Status::OK();
    }
    int rc = ndsFileBatchIOSetUp(&batch.context_, depth_);
    if (rc || !batch.context_) return ndsError("ndsFileBatchIOSetUp", rc);
    return Status::OK();
}

// Request validation and I/O planning

Status NdsTransport::getNdsFileInfo(SegmentID target_id,
                                    SegmentCache& validated, std::string& path,
                                    uint64_t& length) {
    auto cached = validated.find(target_id);
    if (cached != validated.end()) {
        const auto& [cached_path, cached_length] = cached->second;
        path = cached_path;
        length = cached_length;
        return Status::OK();
    }
    return metadata_->segmentManager().withCachedSegment(
        target_id, [&](SegmentDesc* segment) {
            auto local = metadata_->segmentManager().getLocal();
            if (segment->type != SegmentType::File || !local ||
                local->machine_id.empty() ||
                segment->machine_id != local->machine_id ||
                !segment->name.starts_with(kLocalFileSegmentPrefix))
                return Status::InvalidArgument(
                    "NDS requires a local File Segment");
            const auto& entries =
                std::get<FileSegmentDesc>(segment->detail).buffers;
            if (entries.size() != 1 || entries[0].offset != 0)
                return Status::InvalidArgument(
                    "NDS request exceeds File Segment");
            path = entries[0].path;
            length = entries[0].length;
            validated.emplace(target_id, std::make_pair(path, length));
            return Status::OK();
        });
}

Status NdsTransport::validateRequest(const Request& request, uint64_t& base,
                                     std::string& path,
                                     SegmentCache& validated) {
    auto addr = reinterpret_cast<uint64_t>(request.source);
    if (!installed_ || !request.length || addr % kAlignment ||
        request.length % kAlignment || request.target_offset % kAlignment ||
        (request.opcode != Request::READ && request.opcode != Request::WRITE) ||
        request.target_offset > uint64_t(std::numeric_limits<off_t>::max()) ||
        request.length >
            uint64_t(std::numeric_limits<off_t>::max()) - request.target_offset)
        return Status::InvalidArgument("Invalid NDS request or alignment");
    const auto upper = buffers_.upper_bound(addr);
    const auto registration =
        upper == buffers_.begin() ? buffers_.end() : std::prev(upper);
    if (registration == buffers_.end() ||
        !contains(registration->first, registration->second, addr,
                  request.length)) {
        return Status::AddressNotRegistered("NDS source exceeds registration");
    }
    base = registration->first;

    const auto cached_segments = validated.size();
    std::string segment_path;
    uint64_t segment_length = 0;
    CHECK_STATUS(getNdsFileInfo(request.target_id, validated, segment_path,
                                segment_length));
    if (!contains(0, segment_length, request.target_offset, request.length)) {
        // Retain cache entries only after an in-bounds request.
        if (validated.size() != cached_segments)
            validated.erase(request.target_id);
        return Status::InvalidArgument("NDS request exceeds File Segment");
    }
    path = std::move(segment_path);
    return Status::OK();
}

Status NdsTransport::openFileContext(
    const std::string& path, int access,
    std::shared_ptr<NdsFileContext>& file_ctx_ptr) {
    struct stat current{};
    if (stat(path.c_str(), &current)) return ndsError("stat", -errno);
    auto key = std::make_pair(path, access);
    auto it = files_.find(key);
    if (it != files_.end() && it->second->matches(current)) {
        file_ctx_ptr = it->second;
        return Status::OK();
    }
    auto file_ctx = std::make_shared<NdsFileContext>();
    CHECK_STATUS(file_ctx->open(path, access));
    files_[key] = file_ctx;
    file_ctx_ptr = std::move(file_ctx);
    return Status::OK();
}

void NdsTransport::appendTask(
    NdsSubBatch& batch, size_t task_id,
    const std::shared_ptr<NdsFileContext>& file_ctx_ptr, size_t first_io) {
    const auto& request = batch.requests_[task_id];
    const auto buffer_base = batch.buffer_bases_[task_id];
    auto opcode =
        request.opcode == Request::READ ? NDS_FILE_READ : NDS_FILE_WRITE;
    const auto address = reinterpret_cast<uint64_t>(request.source);
    size_t consumed = 0;
    while (consumed < request.length) {
        const auto file_offset = request.target_offset + consumed;
        const auto buffer_address = address + consumed;
        NdsIoEntry* io_entry =
            batch.ios_.size() > first_io ? batch.ios_.back().get() : nullptr;
        if (io_entry &&
            (io_entry->file_ctx_ptr != file_ctx_ptr ||
             io_entry->params.opcode != opcode ||
             uint64_t(io_entry->params.file_offset) + io_entry->length !=
                 file_offset ||
             io_entry->length >= io_bytes_ ||
             task_id - io_entry->parts.back().first > 1)) {
            io_entry = nullptr;
        }
        bool extend_last_iov = false;
        if (io_entry) {
            const auto& last_iov = io_entry->iovs.back();
            const auto last_buffer_base =
                batch.buffer_bases_[io_entry->parts.back().first];
            const auto last_end =
                reinterpret_cast<uint64_t>(last_iov.buf) + last_iov.length;
            // Each iovec must fit within one registered range.
            extend_last_iov =
                last_buffer_base == buffer_base && last_end == buffer_address;
        }
        if (!io_entry ||
            (!extend_last_iov && io_entry->iovs.size() == NDS_FILE_MAX_IOV)) {
            auto added = std::make_unique<NdsIoEntry>();
            added->file_ctx_ptr = file_ctx_ptr;
            added->params.opcode = opcode;
            added->params.fh = file_ctx_ptr->getHandle();
            added->params.file_offset = file_offset;
            batch.ios_.push_back(std::move(added));
            io_entry = batch.ios_.back().get();
        }
        const size_t slice_len =
            std::min(io_bytes_ - io_entry->length, request.length - consumed);
        if (extend_last_iov) {
            io_entry->iovs.back().length += slice_len;
        } else {
            io_entry->iovs.push_back(
                {reinterpret_cast<void*>(buffer_address), slice_len});
        }
        io_entry->length += slice_len;
        io_entry->parts.emplace_back(task_id, slice_len);
        consumed += slice_len;
    }
}

Status NdsTransport::submitTransferTasks(SubBatchRef batch,
                                         const std::vector<Request>& requests) {
    std::lock_guard<std::mutex> lock(NdsRuntime::instance().mutex());
    auto* nds_batch = dynamic_cast<NdsSubBatch*>(batch);
    if (!installed_ || !nds_batch || !batches_.count(nds_batch) ||
        requests.size() > nds_batch->capacity_ - nds_batch->size())
        return Status::InvalidArgument("Invalid NDS batch or capacity");
    updateBatchStatus(*nds_batch);
    // Revalidate each distinct file once per submission. Accepted I/O retains
    // descriptor ownership if the pathname is replaced later.
    std::map<std::pair<std::string, int>,
             std::pair<std::shared_ptr<NdsFileContext>, uint64_t>>
        opened;
    const size_t first_io = nds_batch->ios_.size();
    const size_t task_count = nds_batch->size() + requests.size();
    nds_batch->requests_.reserve(task_count);
    nds_batch->buffer_bases_.reserve(task_count);
    nds_batch->statuses_.reserve(task_count);
    nds_batch->ios_.reserve(first_io + requests.size());
    SegmentCache validated;
    for (const auto& request : requests) {
        auto task_request = request;
        uint64_t buffer_base = 0;
        std::string path;
        auto status = validateRequest(request, buffer_base, path, validated);
        std::shared_ptr<NdsFileContext> file_ctx_ptr;
        if (status.ok()) {
            int access = request.opcode == Request::READ ? O_RDONLY : O_RDWR;
            auto key = std::make_pair(path, access);
            auto it = opened.find(key);
            if (it == opened.end()) {
                uint64_t length = 0;
                status = openFileContext(path, access, file_ctx_ptr);
                if (status.ok())
                    status = getRegularFileSize(file_ctx_ptr->getFd(), length);
                if (status.ok())
                    it = opened
                             .emplace(key, std::make_pair(file_ctx_ptr, length))
                             .first;
            }
            if (status.ok()) {
                const auto& [cached_file, length] = it->second;
                file_ctx_ptr = cached_file;
                if (!contains(0, length, request.target_offset, request.length))
                    status = Status::InvalidArgument(
                        "NDS request exceeds storage size");
            }
        }
        if (!status.ok()) LOG(ERROR) << status.ToString();
        nds_batch->requests_.push_back(std::move(task_request));
        nds_batch->buffer_bases_.push_back(buffer_base);
        nds_batch->statuses_.push_back({status.ok() ? PENDING : FAILED, 0});
        if (status.ok())
            appendTask(*nds_batch, nds_batch->size() - 1, file_ctx_ptr,
                       first_io);
    }
    if (nds_batch->ios_.size() > first_io) {
        auto status = acquireBatchContext(*nds_batch);
        if (status.ok())
            submitSubBatch(*nds_batch);
        else {
            LOG(ERROR) << status.ToString();
            abortSubBatch(*nds_batch);
        }
    }
    return Status::OK();
}

// Batch submission and progress

// Facade destroy drains and consumes the batch even when it returns an I/O
// error. Mark failure only after this fence; never retry the freed pointer.
void NdsTransport::abortSubBatch(NdsSubBatch& batch) {
    if (batch.context_) {
        int rc = ndsFileBatchIODestroy(batch.context_);
        batch.context_ = nullptr;
        if (rc) LOG(ERROR) << "ndsFileBatchIODestroy returned " << rc;
    }
    batch.inflight_ = 0;
    batch.submitted_ = batch.ios_.size();
    for (auto& status : batch.statuses_)
        if (status.s == PENDING) status.s = FAILED;
}

void NdsTransport::submitSubBatch(NdsSubBatch& batch) {
    if (!batch.context_ || batch.submitted_ == batch.ios_.size()) return;
    size_t count = std::min(depth_ - batch.inflight_,
                            batch.ios_.size() - batch.submitted_);
    if (!count) return;
    batch.submission_.clear();
    batch.submission_.reserve(count);
    for (size_t i = batch.submitted_; i < batch.submitted_ + count; ++i) {
        auto& io_entry = *batch.ios_[i];
        io_entry.params.bufPtr_bases = io_entry.iovs.data();
        io_entry.params.iov_count = io_entry.iovs.size();
        io_entry.params.cookie = reinterpret_cast<void*>(i + 1);
        batch.submission_.push_back(io_entry.params);
    }
    int accepted =
        ndsFileBatchIOSubmit(batch.context_, count, batch.submission_.data(),
                             NDS_FILE_IO_REGISTERED_MEM);
    // A short submission may have started DMA for the accepted prefix.
    // Fence before reporting failure to the engine's failover policy.
    if (accepted != static_cast<int>(count)) {
        LOG(ERROR) << "ndsFileBatchIOSubmit returned " << accepted << " for "
                   << count << " entries";
        abortSubBatch(batch);
        return;
    }
    batch.submitted_ += count;
    batch.inflight_ += count;
}

void NdsTransport::updateBatchStatus(NdsSubBatch& batch) {
    if (!batch.context_ || !batch.inflight_) return;
    timespec timeout{0, 0};
    batch.events_.resize(depth_);
    unsigned count = batch.inflight_;
    int rc = ndsFileBatchIOGetStatus(batch.context_, 0, &count,
                                     batch.events_.data(), &timeout);
    if (rc || count > batch.inflight_) {
        LOG(ERROR) << "ndsFileBatchIOGetStatus returned " << rc
                   << ", count=" << count;
        abortSubBatch(batch);
        return;
    }
    for (unsigned i = 0; i < count; ++i) {
        const auto& event = batch.events_[i];
        size_t cookie = reinterpret_cast<uintptr_t>(event.cookie);
        size_t index = cookie - 1;
        if (!cookie || index >= batch.submitted_ || batch.ios_[index]->done ||
            event.ret || event.status != NDS_FILE_COMPLETE) {
            LOG(ERROR) << "Invalid NDS completion: cookie=" << index
                       << ", result=" << event.ret;
            abortSubBatch(batch);
            return;
        }
        auto& io_entry = *batch.ios_[index];
        io_entry.done = true;
        for (const auto& [task_id, bytes] : io_entry.parts) {
            auto& status = batch.statuses_[task_id];
            status.transferred_bytes += bytes;
            if (status.transferred_bytes == batch.requests_[task_id].length)
                status.s = COMPLETED;
        }
    }
    batch.inflight_ -= count;
    // Refill freed slots even while other requests remain in flight.
    submitSubBatch(batch);
}

Status NdsTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                       TransferStatus& status) {
    std::lock_guard<std::mutex> lock(NdsRuntime::instance().mutex());
    auto* nds_batch = dynamic_cast<NdsSubBatch*>(batch);
    if (!nds_batch || !batches_.count(nds_batch) || task_id < 0 ||
        size_t(task_id) >= nds_batch->size())
        return Status::InvalidArgument("Invalid NDS task ID");
    if (nds_batch->statuses_[task_id].s == PENDING)
        updateBatchStatus(*nds_batch);
    status = nds_batch->statuses_[task_id];
    return Status::OK();
}

// Buffer registration and teardown

void NdsTransport::abortAllSubBatches() {
    for (auto* batch : batches_) abortSubBatch(*batch);
}

void NdsTransport::abortSubBatchesForBuffer(uint64_t base) {
    for (auto* batch : batches_) {
        if (batch->hasPendingTasksForBuffer(base)) abortSubBatch(*batch);
    }
}

Status NdsTransport::unregisterBuffer(uint64_t addr) {
    auto it = buffers_.find(addr);
    if (it == buffers_.end()) return Status::OK();
    abortSubBatchesForBuffer(addr);
    CHECK_STATUS(NdsRuntime::instance().unregisterBuffer(addr));
    buffers_.erase(it);
    return Status::OK();
}

Status NdsTransport::addMemoryBuffer(BufferDesc& desc, const MemoryOptions&) {
    auto& runtime = NdsRuntime::instance();
    std::lock_guard<std::mutex> lock(runtime.mutex());
    if (!installed_) return Status::InvalidArgument("NDS is unavailable");
    if (LocationParser(desc.location).type() != "npu") return Status::OK();
    if (!desc.addr || !desc.length || desc.addr % kAlignment ||
        desc.length % kAlignment ||
        desc.length > std::numeric_limits<uint64_t>::max() - desc.addr)
        return Status::InvalidArgument("Invalid NDS memory registration");
    if (buffers_.count(desc.addr))
        return Status::InvalidArgument("NDS buffer is already registered");
    CHECK_STATUS(runtime.registerBuffer(desc.addr, desc.length));
    buffers_.emplace(desc.addr, desc.length);
    desc.transports.push_back(NDS);
    return Status::OK();
}

Status NdsTransport::removeMemoryBuffer(BufferDesc& desc) {
    std::lock_guard<std::mutex> lock(NdsRuntime::instance().mutex());
    return unregisterBuffer(desc.addr);
}

}  // namespace mooncake::tent
