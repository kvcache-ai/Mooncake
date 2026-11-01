#include "nof/linux_initiator.h"

#include <fcntl.h>
#include <liburing.h>
#include <linux/fs.h>
#include <linux/nvme_ioctl.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <unistd.h>

#include <glog/logging.h>

#include <cerrno>
#include <chrono>
#include <climits>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

#include "environment_value_parser.h"
#include "linux_device.h"

namespace mooncake {
namespace {
constexpr unsigned kDefaultQueueDepth = 256;
constexpr unsigned kMaxQueueDepth = 32768;

unsigned GetLinuxNofQueueDepth() {
    static const unsigned depth = [] {
        const char* raw = std::getenv("MC_NOF_LINUX_QUEUE_DEPTH");
        if (!raw || !*raw) return kDefaultQueueDepth;

        const auto value = TryParseEnvironmentValue<unsigned>(raw);
        if (value && *value > 0 && *value <= kMaxQueueDepth) return *value;

        LOG(WARNING) << "Invalid value for MC_NOF_LINUX_QUEUE_DEPTH: " << raw
                     << ", expected 1.." << kMaxQueueDepth << ", using default "
                     << kDefaultQueueDepth;
        return kDefaultQueueDepth;
    }();
    return depth;
}

class IoQueue {
   public:
    int Init(unsigned depth) {
        requests_.resize(depth);
        free_.reserve(depth);
        for (auto& request : requests_) free_.push_back(&request);
        const int ret = io_uring_queue_init(depth, &ring_, 0);
        if (ret < 0) return ret;
        initialized_ = true;
        return 0;
    }

    ~IoQueue() {
        if (!initialized_) return;
        // Closing a ring is not a buffer-lifetime barrier. Drain original
        // CQEs before freeing any request or probe memory. Operators must use
        // finite kernel controller-loss/I/O timeouts for bounded shutdown.
        while (pending_ != 0) {
            Poll(0);
            if (pending_ != 0) {
                io_uring_cqe* cqe = nullptr;
                __kernel_timespec timeout{0, 1000000};
                const int ret =
                    io_uring_wait_cqe_timeout(&ring_, &cqe, &timeout);
                if (ret < 0 && ret != -EINTR && ret != -ETIME)
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
        io_uring_queue_exit(&ring_);
    }

    int Submit(int fd, void* buffer, uint64_t offset, uint32_t length,
               NofIOOp op, const NofIOAdaptor& adaptor) {
        if (free_.empty()) return -EAGAIN;
        auto* sqe = io_uring_get_sqe(&ring_);
        if (!sqe) return -EAGAIN;
        auto* request = free_.back();
        free_.pop_back();
        request->adaptor = adaptor;
        request->length = length;
        if (op == NofIOOp::kRead)
            io_uring_prep_read(sqe, fd, buffer, length, offset);
        else
            io_uring_prep_write(sqe, fd, buffer, length, offset);
        io_uring_sqe_set_data(sqe, request);
        ++pending_;
        return 0;
    }

    int64_t Poll(uint32_t maximum) {
        int submit_error = 0;
        if (io_uring_sq_ready(&ring_)) {
            const int ret = io_uring_submit(&ring_);
            if (ret < 0 && ret != -EINTR && ret != -EAGAIN && ret != -EBUSY)
                submit_error = ret;
        }
        int64_t completed = 0;
        io_uring_cqe* cqe = nullptr;
        while ((maximum == 0 || completed < maximum) &&
               io_uring_peek_cqe(&ring_, &cqe) == 0) {
            auto* request = static_cast<Request*>(io_uring_cqe_get_data(cqe));
            const int result = cqe->res;
            io_uring_cqe_seen(&ring_, cqe);
            --pending_;
            const auto adaptor = request->adaptor;
            NofIOCompletion completion;
            completion.success =
                result >= 0 && static_cast<uint32_t>(result) == request->length;
            if (!completion.success) {
                completion.error_string =
                    result < 0
                        ? std::string("Linux NoF I/O: ") +
                              std::strerror(-result)
                        : "Linux NoF short I/O: " + std::to_string(result) +
                              "/" + std::to_string(request->length);
            }
            free_.push_back(request);
            ++completed;
            adaptor.cb(adaptor.ctx, completion);
        }
        return completed != 0 ? completed : submit_error;
    }

   private:
    struct Request {
        NofIOAdaptor adaptor{};
        uint32_t length = 0;
    };
    io_uring ring_{};
    std::vector<Request> requests_;
    std::vector<Request*> free_;
    size_t pending_ = 0;
    bool initialized_ = false;
};

class LinuxSegment final : public NofSegmentHandle {
   public:
    explicit LinuxSegment(int fd, uint32_t block_size, uint64_t capacity)
        : fd(fd), block_size(block_size), capacity(capacity) {}
    ~LinuxSegment() {
        queue.reset();  // drain before closing the namespace
        close(fd);
    }
    int fd;
    uint32_t block_size;
    uint64_t capacity;
    std::unique_ptr<IoQueue> queue;
};

std::unique_ptr<LinuxSegment> OpenDevice(const detail::NofEndpoint& endpoint,
                                         bool readonly, uint32_t queue_depth,
                                         std::string& error) {
    const std::string path = detail::FindLinuxNvmeDevice(endpoint, error);
    if (path.empty()) return nullptr;
    const int fd = open(path.c_str(),
                        O_CLOEXEC | O_DIRECT | (readonly ? O_RDONLY : O_RDWR));
    if (fd < 0) {
        error = "open " + path + ": " + std::strerror(errno);
        return nullptr;
    }
    struct OpenFd {
        int value;
        ~OpenFd() {
            if (value >= 0) close(value);
        }
    } guard{fd};
    struct stat st{};
    uint32_t block_size = 0;
    uint64_t capacity = 0;
    const int nsid = ioctl(fd, NVME_IOCTL_ID);
    if (fstat(fd, &st) != 0 || !S_ISBLK(st.st_mode) || nsid <= 0 ||
        static_cast<uint32_t>(nsid) != endpoint.nsid ||
        ioctl(fd, BLKSSZGET, &block_size) != 0 ||
        ioctl(fd, BLKGETSIZE64, &capacity) != 0 || block_size == 0 ||
        capacity == 0) {
        error = "invalid or unsupported NVMe block device: " + path;
        return nullptr;
    }
    // Revalidate the opened device rather than trusting a /dev name that
    // could have been recycled between the sysfs scan and open().
    std::ifstream identity(
        "/sys/dev/block/" + std::to_string(major(st.st_rdev)) + ":" +
        std::to_string(minor(st.st_rdev)) + "/device/subsysnqn");
    std::string nqn;
    std::getline(identity, nqn);
    if (nqn != endpoint.nqn) {
        error = "NVMe namespace identity changed while opening " + path;
        return nullptr;
    }
    auto handle = std::make_unique<LinuxSegment>(fd, block_size, capacity);
    guard.value = -1;  // ownership transferred, including exception paths
    handle->queue = std::make_unique<IoQueue>();
    const int ret = handle->queue->Init(queue_depth);
    if (ret < 0) {
        error = "io_uring initialization: " + std::string(std::strerror(-ret));
        return nullptr;
    }
    return handle;
}

struct ProbeContext {
    std::mutex mutex;
    // Must be destroyed after segment, whose destructor drains pending I/O.
    std::unique_ptr<void, decltype(&free)> buffer{nullptr, free};
    bool pending = false;
    NofIOCompletion completion;
    NofIOAdaptor adaptor{Complete, this};
    std::unique_ptr<LinuxSegment> segment;

    static void Complete(void* context, const NofIOCompletion& result) {
        auto& probe = *static_cast<ProbeContext*>(context);
        probe.completion = result;
        probe.pending = false;
    }
};
}  // namespace

class LinuxNvmeInitiator::Impl {
   public:
    std::mutex mutex;
    std::map<detail::NofEndpoint, std::unique_ptr<LinuxSegment>> segments;
    std::map<detail::NofEndpoint, std::unique_ptr<ProbeContext>> probes;
};

LinuxNvmeInitiator::LinuxNvmeInitiator() : impl_(std::make_unique<Impl>()) {}
LinuxNvmeInitiator::~LinuxNvmeInitiator() = default;

NofSegmentHandle* LinuxNvmeInitiator::OpenSegment(const std::string& text) {
    detail::NofEndpoint endpoint;
    std::string error;
    if (!detail::ParseNofEndpoint(text, endpoint, error)) {
        LOG(ERROR) << error;
        return nullptr;
    }
    std::lock_guard<std::mutex> lock(impl_->mutex);
    auto it = impl_->segments.find(endpoint);
    if (it != impl_->segments.end()) return it->second.get();
    auto handle = OpenDevice(endpoint, false, GetLinuxNofQueueDepth(), error);
    if (!handle) {
        LOG(ERROR) << error;
        return nullptr;
    }
    auto* result = handle.get();
    impl_->segments.emplace(std::move(endpoint), std::move(handle));
    return result;
}

uint32_t LinuxNvmeInitiator::GetBlockSize(const NofSegmentHandle* handle) {
    return handle ? static_cast<const LinuxSegment*>(handle)->block_size
                  : kInvalidBlockSize;
}

int LinuxNvmeInitiator::SubmitIO(NofSegmentHandle* handle, void* buffer,
                                 uint64_t offset, uint64_t length, NofIOOp op,
                                 NofIOAdaptor* adaptor) {
    if (!handle || !buffer || !adaptor || !adaptor->cb || length == 0 ||
        length > INT_MAX || (op != NofIOOp::kRead && op != NofIOOp::kWrite))
        return -EINVAL;
    auto* segment = static_cast<LinuxSegment*>(handle);
    const uint32_t block = segment->block_size;
    if (offset % block || length % block ||
        reinterpret_cast<uintptr_t>(buffer) % block)
        return -EINVAL;
    if (offset > segment->capacity || length > segment->capacity - offset)
        return -ERANGE;
    return segment->queue->Submit(segment->fd, buffer, offset,
                                  static_cast<uint32_t>(length), op, *adaptor);
}

int64_t LinuxNvmeInitiator::PollCompletion(NofSegmentHandle* handle,
                                           uint32_t maximum) {
    return handle ? static_cast<LinuxSegment*>(handle)->queue->Poll(maximum)
                  : -EINVAL;
}

NofCapabilities LinuxNvmeInitiator::GetCapabilities() const {
    NofCapabilities caps;
    caps.requires_memory_registration = false;
    return caps;
}

bool LinuxNvmeInitiator::ProbeSegment(const std::string& text,
                                      uint32_t timeout_ms,
                                      std::string* error_reason) {
    const auto fail = [&](const std::string& error) {
        if (error_reason) *error_reason = error;
        return false;
    };
    detail::NofEndpoint endpoint;
    std::string error;
    if (!detail::ParseNofEndpoint(text, endpoint, error)) return fail(error);
    if (timeout_ms == 0) return fail("Linux NoF probe timeout is zero");
    ProbeContext* probe;
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        auto it = impl_->probes.find(endpoint);
        if (it == impl_->probes.end()) {
            auto created = std::make_unique<ProbeContext>();
            created->segment =
                OpenDevice(endpoint, true, kDefaultQueueDepth, error);
            if (!created->segment) return fail(error);
            void* buffer = nullptr;
            const auto block = created->segment->block_size;
            if (posix_memalign(&buffer, block, block) != 0)
                return fail("Linux NoF probe buffer allocation failed");
            created->buffer.reset(buffer);
            it = impl_->probes.emplace(std::move(endpoint), std::move(created))
                     .first;
        }
        probe = it->second.get();
    }
    std::unique_lock<std::mutex> lock(probe->mutex, std::try_to_lock);
    if (!lock.owns_lock()) return fail("Linux NoF probe already running");
    probe->segment->queue->Poll(0);
    if (probe->pending) return fail("previous Linux NoF probe has not drained");

    const auto block = probe->segment->block_size;
    const int ret = SubmitIO(probe->segment.get(), probe->buffer.get(), 0,
                             block, NofIOOp::kRead, &probe->adaptor);
    if (ret < 0)
        return fail("Linux NoF probe submit: " +
                    std::string(std::strerror(-ret)));
    probe->pending = true;
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_ms);
    while (probe->pending) {
        probe->segment->queue->Poll(0);
        if (!probe->pending) break;
        if (std::chrono::steady_clock::now() >= deadline) {
            return fail("Linux NoF probe timed out");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (!probe->completion.success) return fail(probe->completion.error_string);
    if (error_reason) error_reason->clear();
    return true;
}
}  // namespace mooncake
