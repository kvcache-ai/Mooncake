#include "nvme_kv_executor.h"

#ifdef MOONCAKE_HAVE_NVME_URING_CMD

#include "nvme_kv_executor_util.h"
#include "nvme_kv_object_layout.h"

#include <fcntl.h>
#include <liburing.h>
#include <linux/nvme_ioctl.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>

// IORING_SETUP_SQE128 was introduced in Linux 5.19.  Provide a fallback
// definition so the file compiles against older kernel headers (the feature
// is only usable at runtime on 5.19+ kernels).
#ifndef IORING_SETUP_SQE128
#define IORING_SETUP_SQE128 (1U << 10)
#endif

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

namespace mooncake {
namespace {

constexpr uint32_t kDefaultQueueDepth = 32;
constexpr uint32_t kDefaultRuntimeTransferLimit = 512 * 1024;
constexpr uint32_t kDefaultProtocolMaxValueSize = 512 * 1024;
constexpr uint32_t kDefaultTimeoutMs = 30000;
constexpr uint8_t kKvStoreOpcode = 0x01;
constexpr uint8_t kKvRetrieveOpcode = 0x02;
constexpr uint8_t kKvListOpcode = 0x06;
constexpr uint8_t kKvDeleteOpcode = 0x10;
constexpr uint32_t kNvmeKvNamespaceId = 1;
constexpr uint32_t kCdw11KeyLengthMask = 0xFFu;
constexpr uint8_t kStoreOptions = 0;
constexpr uint8_t kRetrieveOptions = 0;
constexpr uint8_t kNvmeAdminIdentifyOpcode = 0x06;
constexpr uint32_t kNvmeIdentifyControllerCns = 0x01;
constexpr size_t kNvmeDmaAlignment = 4096;

struct FreeDeleter {
    void operator()(void* ptr) const { std::free(ptr); }
};

template <typename T>
using AlignedUniquePtr = std::unique_ptr<T, FreeDeleter>;

ErrorCode MapTransportError(int err, bool is_write);

AlignedUniquePtr<char> AllocateAlignedBuffer(size_t size) {
    void* ptr = nullptr;
    if (posix_memalign(&ptr, kNvmeDmaAlignment, size) != 0) {
        return AlignedUniquePtr<char>(nullptr);
    }
    return AlignedUniquePtr<char>(static_cast<char*>(ptr));
}

uint32_t ResolveRetrievedObjectSize(const char* buffer, uint32_t returned_size,
                                    uint32_t max_size) {
    return ResolveNvmeKvObjectBlobSize(buffer, returned_size, max_size);
}

tl::expected<void, ErrorCode> IdentifyController(int fd) {
    auto identify_buffer = AllocateAlignedBuffer(4096);
    if (identify_buffer == nullptr) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    nvme_admin_cmd cmd{};
    cmd.opcode = kNvmeAdminIdentifyOpcode;
    cmd.nsid = 0;
    cmd.addr = reinterpret_cast<__u64>(identify_buffer.get());
    cmd.data_len = 4096;
    cmd.cdw10 = kNvmeIdentifyControllerCns;
    cmd.timeout_ms = kDefaultTimeoutMs;

    errno = 0;
    const int ret = ::ioctl(fd, NVME_IOCTL_ADMIN_CMD, &cmd);
    if (ret < 0) {
        LOG(WARNING) << "[NvmeKvIoUringExecutor] identify controller failed"
                     << " errno=" << errno << " strerror=" << strerror(errno);
        return tl::make_unexpected(MapTransportError(errno, false));
    }
    if (ret > 0) {
        LOG(WARNING) << "[NvmeKvIoUringExecutor] identify controller returned "
                        "NVMe status"
                     << " ret=" << ret << " result=0x" << std::hex
                     << cmd.result;
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    return {};
}

uint32_t ParseU32EnvOr(const std::string& name, uint32_t fallback) {
    const char* value = std::getenv(name.c_str());
    if (value == nullptr || value[0] == '\0') {
        return fallback;
    }
    char* end = nullptr;
    errno = 0;
    unsigned long parsed = std::strtoul(value, &end, 0);
    if (errno != 0 || end == value || *end != '\0' || parsed > UINT32_MAX) {
        return fallback;
    }
    return static_cast<uint32_t>(parsed);
}

bool ParseBoolEnvOr(const char* name, bool fallback) {
    const char* value = std::getenv(name);
    if (value == nullptr || value[0] == '\0') {
        return fallback;
    }
    const std::string parsed(value);
    if (parsed == "1" || parsed == "true" || parsed == "TRUE") {
        return true;
    }
    if (parsed == "0" || parsed == "false" || parsed == "FALSE") {
        return false;
    }
    return fallback;
}

uint8_t ResolveStoreOptions(
    const NvmeKvCommandExecutor::StoreOptions& options) {
    uint32_t flags = kStoreOptions;
    if (options.if_not_exists) {
        flags |= ParseU32EnvOr(
            "MOONCAKE_NVME_KV_STORE_IF_NOT_EXISTS_OPTION_MASK", 0x2);
    }
    return static_cast<uint8_t>(flags & 0xFFu);
}

bool ConditionalStoreEnabled() {
    return ParseBoolEnvOr("MOONCAKE_NVME_KV_ENABLE_CONDITIONAL_STORE", false);
}

bool IoUringCqe32Enabled() {
    return ParseBoolEnvOr("MOONCAKE_NVME_KV_IO_URING_ENABLE_CQE32", true);
}

uint16_t ReadLe16(const char* p) {
    return static_cast<uint16_t>(static_cast<uint8_t>(p[0])) |
           (static_cast<uint16_t>(static_cast<uint8_t>(p[1])) << 8);
}

uint32_t ReadLe32(const uint8_t* p) {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) |
           (static_cast<uint32_t>(p[3]) << 24);
}

uint32_t RoundUpToKvTransferBytes(uint32_t bytes) {
    return RoundUpToNvmeKvTransferBytes(bytes);
}

uint32_t ResolveStoreSubmissionBytes(uint32_t logical_bytes) {
    const uint32_t rounded_logical_bytes =
        RoundUpToKvTransferBytes(logical_bytes);
    const uint32_t min_store_bytes = RoundUpToKvTransferBytes(
        ParseU32EnvOr("MOONCAKE_NVME_KV_MIN_STORE_BYTES", 0));
    if (min_store_bytes == 0 || rounded_logical_bytes >= min_store_bytes) {
        if (rounded_logical_bytes != logical_bytes) {
            LOG(INFO) << "[NvmeKvIoUringExecutor] rounding buffer size "
                      << logical_bytes << " up to " << rounded_logical_bytes
                      << " bytes for 512-byte NVMe KV transfer units";
        }
        return rounded_logical_bytes;
    }
    LOG(INFO) << "[NvmeKvIoUringExecutor] expanding store submission from "
              << logical_bytes << " logical bytes (" << rounded_logical_bytes
              << " transfer bytes) to " << min_store_bytes
              << " bytes due to MOONCAKE_NVME_KV_MIN_STORE_BYTES";
    return min_store_bytes;
}

uint32_t ComputeKvBlockCountMinusOne(uint32_t bytes) {
    if (bytes == 0) {
        return 0;
    }
    const uint32_t rounded_bytes = RoundUpToKvTransferBytes(bytes);
    return (rounded_bytes / kNvmeKvTransferUnitBytes) - 1;
}

void EncodeKeyIntoCommand(const NvmeKvCommandExecutor::PhysicalKey& key,
                          struct nvme_uring_cmd& cmd) {
    const NvmeKvPackedKeyFields fields = PackNvmeKvPhysicalKey(key);
    cmd.cdw2 = fields.cdw2;
    cmd.cdw3 = fields.cdw3;
    cmd.cdw14 = fields.cdw14;
    cmd.cdw15 = fields.cdw15;
}

tl::expected<std::vector<NvmeKvCommandExecutor::PhysicalKey>, ErrorCode>
ParseListResponse(const char* buffer, uint32_t buffer_size) {
    if (buffer_size < sizeof(uint32_t)) {
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }

    const uint32_t returned_keys =
        ReadLe32(reinterpret_cast<const uint8_t*>(buffer));
    std::vector<NvmeKvCommandExecutor::PhysicalKey> keys;
    keys.reserve(returned_keys);
    uint32_t offset = sizeof(uint32_t);
    for (uint32_t i = 0; i < returned_keys; ++i) {
        if (offset + sizeof(uint16_t) > buffer_size) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        const uint16_t key_length = ReadLe16(buffer + offset);
        offset += sizeof(uint16_t);
        if (key_length == 0 ||
            key_length > NvmeKvCommandExecutor::PhysicalKey{}.size()) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        if (offset + key_length > buffer_size) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        NvmeKvCommandExecutor::PhysicalKey key{};
        std::memcpy(key.data(), buffer + offset, key_length);
        keys.push_back(key);
        offset += key_length;
        offset = (offset + 3u) & ~3u;
        if (offset > buffer_size && i + 1 != returned_keys) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
    }
    return keys;
}

uint32_t BuildKeyLengthField(size_t key_length) {
    return static_cast<uint32_t>(key_length) & kCdw11KeyLengthMask;
}

uint32_t BuildOptionAndKeyLengthField(uint8_t options, size_t key_length) {
    return (static_cast<uint32_t>(options) << 8) |
           BuildKeyLengthField(key_length);
}

ErrorCode MapTransportError(int err, bool is_write) {
    return MapNvmeKvTransportError(err, is_write);
}

class SharedNvmeUringRing {
   public:
    struct SubmitResult {
        NvmeKvCommandExecutor::CommandAuditInfo audit;
        ErrorCode mapped_error = ErrorCode::OK;
    };

    static SharedNvmeUringRing& Instance(unsigned queue_depth) {
        thread_local SharedNvmeUringRing ring(queue_depth);
        return ring;
    }

    bool IsInitialized() const { return initialized_; }

    tl::expected<SubmitResult, ErrorCode> Submit(
        int fd, struct nvme_uring_cmd& cmd, bool is_write, const char* op_name,
        const NvmeKvCommandExecutor::PhysicalKey& key) {
        NvmeKvCommandExecutor::CommandAuditInfo audit{
            .transport = "io_uring",
            .operation = op_name,
            .interpretation =
                "observed_return mirrors cqe->res and is treated as the "
                "io_uring completion result, not as NVMe CQE DW0. When the "
                "ring is initialized with CQE32, observed_result mirrors the "
                "CQE32 extra result slot, which may carry the separate NVMe "
                "result on kernels that expose it through uring_cmd.",
        };
        if (!initialized_) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (sqe == nullptr) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }

        static_assert(sizeof(cmd) <= 80,
                      "nvme_uring_cmd must fit in SQE128 cmd area");
        sqe->opcode = IORING_OP_URING_CMD;
        sqe->fd = fd;
        sqe->cmd_op = NVME_URING_CMD_IO;
        void* cmd_dest = sqe->cmd;
        __asm__ __volatile__("" : "+r"(cmd_dest));
        std::memcpy(cmd_dest, &cmd, sizeof(cmd));

        int submit_ret = io_uring_submit_and_wait(&ring_, 1);
        if (submit_ret < 0) {
            LOG(ERROR) << "[NvmeKvIoUringExecutor] io_uring_submit_and_wait "
                       << strerror(-submit_ret);
            const ErrorCode mapped = MapTransportError(-submit_ret, is_write);
            audit.observed_return = submit_ret;
            audit.success = false;
            return tl::make_unexpected(mapped);
        }

        io_uring_cqe* cqe = nullptr;
        int wait_ret = io_uring_wait_cqe(&ring_, &cqe);
        if (wait_ret < 0 || cqe == nullptr) {
            if (cqe != nullptr) {
                io_uring_cqe_seen(&ring_, cqe);
            }
            const ErrorCode mapped =
                MapTransportError(wait_ret < 0 ? -wait_ret : EIO, is_write);
            audit.observed_return = wait_ret < 0 ? wait_ret : -EIO;
            audit.success = false;
            return tl::make_unexpected(mapped);
        }

        const int res = cqe->res;
        const uint64_t cqe_extra_result =
            cqe32_enabled_ ? cqe->big_cqe[0] : static_cast<uint64_t>(0);
        io_uring_cqe_seen(&ring_, cqe);
        audit.observed_return = res;
        audit.success = res >= 0;
        if (res >= 0 && cqe32_enabled_ && cqe_extra_result <= UINT32_MAX) {
            audit.observed_result = static_cast<uint32_t>(cqe_extra_result);
            audit.has_observed_result = true;
        }

        if (res < 0) {
            const ErrorCode mapped = MapTransportError(-res, is_write);
            return tl::make_unexpected(mapped);
        }
        return SubmitResult{.audit = audit, .mapped_error = ErrorCode::OK};
    }

   private:
    explicit SharedNvmeUringRing(unsigned queue_depth) {
        io_uring_params params{};
        // IORING_SETUP_SQE128 is required for NVMe uring_cmd passthrough
        // because sizeof(struct nvme_uring_cmd) = 72 bytes, which exceeds the
        // 16 bytes available in the cmd[] area of a standard 64-byte SQE.
        params.flags = IORING_SETUP_SQE128;
        if (IoUringCqe32Enabled()) {
            params.flags |= IORING_SETUP_CQE32;
        }
        int ret = io_uring_queue_init_params(
            queue_depth == 0 ? kDefaultQueueDepth : queue_depth, &ring_,
            &params);
        if (ret < 0 && (params.flags & IORING_SETUP_CQE32)) {
            LOG(WARNING)
                << "[NvmeKvIoUringExecutor] io_uring_queue_init_params with "
                   "CQE32 failed: "
                << strerror(-ret) << ", retrying with SQE128 only";
            params.flags = IORING_SETUP_SQE128;
            ret = io_uring_queue_init_params(
                queue_depth == 0 ? kDefaultQueueDepth : queue_depth, &ring_,
                &params);
        }
        if (ret < 0) {
            LOG(ERROR) << "[NvmeKvIoUringExecutor] io_uring queue init failed: "
                       << strerror(-ret);
            return;
        }
        cqe32_enabled_ = (ring_.flags & IORING_SETUP_CQE32) != 0;
        initialized_ = true;
    }

    ~SharedNvmeUringRing() {
        if (initialized_) {
            io_uring_queue_exit(&ring_);
        }
    }

    io_uring ring_{};
    bool initialized_ = false;
    bool cqe32_enabled_ = false;
};

class NvmeKvIoUringExecutor : public NvmeKvCommandExecutor {
   public:
    NvmeKvIoUringExecutor(std::string device_path, uint32_t nsid,
                          Capabilities capabilities)
        : device_path_(std::move(device_path)),
          nsid_(nsid),
          capabilities_(capabilities) {}

    tl::expected<void, ErrorCode> Init() {
        fd_ = ::open(device_path_.c_str(), O_RDWR | O_CLOEXEC);
        if (fd_ < 0) {
            LOG(ERROR) << "[NvmeKvIoUringExecutor] open failed for "
                       << device_path_ << ": " << strerror(errno);
            return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
        }

        auto identify_res = IdentifyController(fd_);
        if (!identify_res) {
            LOG(WARNING) << "[NvmeKvIoUringExecutor] continuing after identify "
                            "controller probe failure on "
                         << device_path_ << ", error="
                         << static_cast<int>(identify_res.error());
        }

        ring_ = &SharedNvmeUringRing::Instance(capabilities_.queue_depth);
        if (!ring_->IsInitialized()) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        return {};
    }

    ~NvmeKvIoUringExecutor() override {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    tl::expected<void, ErrorCode> Store(const PhysicalKey& key,
                                        std::string value,
                                        StoreOptions options = {}) override {
        if (value.size() > capabilities_.effective_max_value_size) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        const uint32_t value_size = static_cast<uint32_t>(value.size());
        const uint32_t submission_bytes =
            ResolveStoreSubmissionBytes(value_size);
        if (submission_bytes > capabilities_.effective_max_value_size) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        auto dma_buffer = AllocateAlignedBuffer(submission_bytes);
        if (dma_buffer == nullptr) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        std::memset(dma_buffer.get(), 0, submission_bytes);
        std::memcpy(dma_buffer.get(), value.data(), value.size());
        struct nvme_uring_cmd cmd{};
        cmd.opcode = kKvStoreOpcode;
        cmd.nsid = nsid_;
        cmd.addr = reinterpret_cast<uint64_t>(dma_buffer.get());
        cmd.data_len = submission_bytes;
        cmd.cdw10 = submission_bytes;
        cmd.cdw11 = BuildOptionAndKeyLengthField(ResolveStoreOptions(options),
                                                 key.size());
        cmd.cdw12 = ComputeKvBlockCountMinusOne(submission_bytes);
        cmd.timeout_ms = kDefaultTimeoutMs;
        EncodeKeyIntoCommand(key, cmd);

        auto result = ring_->Submit(
            fd_, cmd, true,
            options.if_not_exists ? "store-if-not-exists" : "store", key);
        if (!result) {
            return tl::make_unexpected(result.error());
        }
        last_audit_info_ = result->audit;
        return {};
    }

    tl::expected<std::string, ErrorCode> Retrieve(
        const PhysicalKey& key, uint32_t size_hint = 0) const override {
        const auto submit_retrieve = [&](uint32_t request_bytes,
                                         const char* op_name)
            -> tl::expected<std::pair<AlignedUniquePtr<char>, uint32_t>,
                            ErrorCode> {
            const uint32_t transfer_bytes =
                RoundUpToKvTransferBytes(request_bytes);
            auto dma_buffer = AllocateAlignedBuffer(transfer_bytes);
            if (dma_buffer == nullptr) {
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            struct nvme_uring_cmd cmd{};
            cmd.opcode = kKvRetrieveOpcode;
            cmd.nsid = nsid_;
            cmd.addr = reinterpret_cast<uint64_t>(dma_buffer.get());
            cmd.data_len = transfer_bytes;
            cmd.cdw10 = transfer_bytes;
            cmd.cdw11 =
                BuildOptionAndKeyLengthField(kRetrieveOptions, key.size());
            cmd.cdw12 = ComputeKvBlockCountMinusOne(transfer_bytes);
            cmd.timeout_ms = kDefaultTimeoutMs;
            EncodeKeyIntoCommand(key, cmd);

            auto result = ring_->Submit(fd_, cmd, false, op_name, key);
            if (!result) {
                return tl::make_unexpected(result.error());
            }
            last_audit_info_ = result->audit;
            const uint32_t returned_size = result->audit.has_observed_result
                                               ? result->audit.observed_result
                                               : 0;
            return std::make_pair(std::move(dma_buffer), returned_size);
        };

        const uint32_t min_initial_bytes =
            size_hint > 0
                ? size_hint +
                      static_cast<uint32_t>(sizeof(NvmeKvObjectHeader)) + 512u
                : 4096u;
        const uint32_t initial_request_bytes =
            std::min(capabilities_.effective_max_value_size,
                     RoundUpToKvTransferBytes(std::max<uint32_t>(
                         sizeof(NvmeKvObjectHeader), min_initial_bytes)));
        auto retrieve_res = submit_retrieve(initial_request_bytes, "retrieve");
        bool used_fallback_request = false;
        if (!retrieve_res) {
            if (retrieve_res.error() == ErrorCode::OBJECT_NOT_FOUND ||
                initial_request_bytes ==
                    capabilities_.effective_max_value_size) {
                return tl::make_unexpected(retrieve_res.error());
            }
            retrieve_res = submit_retrieve(
                capabilities_.effective_max_value_size, "retrieve-fallback");
            if (!retrieve_res) {
                return tl::make_unexpected(retrieve_res.error());
            }
            used_fallback_request = true;
        }

        auto [dma_buffer, returned_size] = std::move(retrieve_res.value());
        const uint32_t prefix_limit =
            used_fallback_request ? capabilities_.effective_max_value_size
                                  : initial_request_bytes;
        const uint32_t actual_size = ResolveRetrievedObjectSize(
            dma_buffer.get(), returned_size, prefix_limit);
        if (actual_size != 0 && actual_size <= prefix_limit) {
            return std::string(dma_buffer.get(),
                               dma_buffer.get() + actual_size);
        }

        uint32_t required_size = returned_size;
        if (required_size <= prefix_limit) {
            required_size = ResolveNvmeKvObjectBlobSizeFromPrefix(
                dma_buffer.get(), prefix_limit);
        }
        if (required_size > prefix_limit &&
            required_size <= capabilities_.max_value_size) {
            auto retry_res = submit_retrieve(required_size, "retrieve-retry");
            if (!retry_res) {
                return tl::make_unexpected(retry_res.error());
            }
            auto [retry_buffer, retry_returned_size] =
                std::move(retry_res.value());
            const uint32_t retry_actual_size = ResolveRetrievedObjectSize(
                retry_buffer.get(), retry_returned_size, required_size);
            if (retry_actual_size != 0 && retry_actual_size <= required_size) {
                return std::string(retry_buffer.get(),
                                   retry_buffer.get() + retry_actual_size);
            }
        }

        return tl::make_unexpected(required_size > capabilities_.max_value_size
                                       ? ErrorCode::BUFFER_OVERFLOW
                                       : ErrorCode::FILE_READ_FAIL);
    }

    tl::expected<void, ErrorCode> Delete(const PhysicalKey& key) override {
        struct nvme_uring_cmd cmd{};
        cmd.opcode = kKvDeleteOpcode;
        cmd.nsid = nsid_;
        cmd.cdw10 = 0;
        cmd.cdw11 = BuildKeyLengthField(key.size());
        cmd.timeout_ms = kDefaultTimeoutMs;
        EncodeKeyIntoCommand(key, cmd);

        auto result = ring_->Submit(fd_, cmd, true, "delete", key);
        if (!result) {
            return tl::make_unexpected(result.error());
        }
        last_audit_info_ = result->audit;
        return {};
    }

    tl::expected<void, ErrorCode> Iterate(
        const std::function<tl::expected<void, ErrorCode>(
            const PhysicalKey& key)>& visitor) const override {
        constexpr uint32_t kListBufferBytes = 4096;
        constexpr uint32_t kMaxListIterations = 200;
        auto dma_buffer = AllocateAlignedBuffer(kListBufferBytes);
        if (dma_buffer == nullptr) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }

        PhysicalKey cursor_key{};
        bool has_cursor = false;
        for (uint32_t iteration = 0; iteration < kMaxListIterations;
             ++iteration) {
            std::memset(dma_buffer.get(), 0, kListBufferBytes);
            struct nvme_uring_cmd cmd{};
            cmd.opcode = kKvListOpcode;
            cmd.nsid = nsid_;
            cmd.addr = reinterpret_cast<uint64_t>(dma_buffer.get());
            cmd.data_len = kListBufferBytes;
            cmd.cdw10 = kListBufferBytes;
            cmd.cdw11 = BuildKeyLengthField(has_cursor ? cursor_key.size() : 0);
            cmd.timeout_ms = kDefaultTimeoutMs;
            if (has_cursor) {
                EncodeKeyIntoCommand(cursor_key, cmd);
            } else {
                cmd.cdw14 = 0;
            }

            auto result = ring_->Submit(fd_, cmd, false, "list", cursor_key);
            if (!result) {
                return tl::make_unexpected(result.error());
            }
            last_audit_info_ = result->audit;

            auto parsed_keys =
                ParseListResponse(dma_buffer.get(), kListBufferBytes);
            if (!parsed_keys) {
                return tl::make_unexpected(parsed_keys.error());
            }
            if (parsed_keys->empty()) {
                return {};
            }

            PhysicalKey last_key{};
            size_t visited_keys = 0;
            for (const auto& listed_key : *parsed_keys) {
                auto visit_res = visitor(listed_key);
                if (!visit_res) {
                    return visit_res;
                }
                last_key = listed_key;
                ++visited_keys;
            }
            if (visited_keys == 0 || (has_cursor && last_key == cursor_key)) {
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
            cursor_key = last_key;
            has_cursor = true;
        }
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }

    const Capabilities& GetCapabilities() const override {
        return capabilities_;
    }

    std::string GetBackendType() const override { return "real"; }

    std::optional<CommandAuditInfo> GetLastCommandAuditInfo() const override {
        return last_audit_info_;
    }

   private:
    std::string device_path_;
    uint32_t nsid_ = 1;
    Capabilities capabilities_;
    mutable std::optional<CommandAuditInfo> last_audit_info_;
    int fd_ = -1;
    SharedNvmeUringRing* ring_ = nullptr;
};

}  // namespace

std::unique_ptr<NvmeKvCommandExecutor> CreateNvmeKvIoUringExecutor(
    const std::string& /*device_id*/, std::filesystem::path /*storage_path*/,
    std::string device_path, uint32_t nsid, uint32_t queue_depth,
    uint32_t runtime_transfer_limit,
    tl::expected<NvmeKvCommandExecutor::Capabilities, ErrorCode>&
        capabilities) {
    NvmeKvCommandExecutor::Capabilities caps;
    caps.max_key_size = kNvmeKvMaxKeySizeBytes;
    caps.max_value_size =
        ParseU32EnvOr("MOONCAKE_NVME_KV_PROTOCOL_MAX_VALUE_SIZE",
                      kDefaultProtocolMaxValueSize);
    caps.runtime_transfer_limit = runtime_transfer_limit == 0
                                      ? kDefaultRuntimeTransferLimit
                                      : runtime_transfer_limit;
    caps.effective_max_value_size = RoundDownToNvmeKvTransferBytes(
        std::min(caps.max_value_size, caps.runtime_transfer_limit));
    caps.queue_depth = queue_depth == 0 ? kDefaultQueueDepth : queue_depth;
    caps.supports_iterate = true;
    caps.supports_batch_submit = true;
    caps.supports_conditional_store = ConditionalStoreEnabled();

    auto executor = std::make_unique<NvmeKvIoUringExecutor>(
        std::move(device_path), nsid, caps);
    auto init_res = executor->Init();
    if (!init_res) {
        capabilities = tl::make_unexpected(init_res.error());
        return nullptr;
    }
    capabilities = caps;
    return executor;
}

}  // namespace mooncake

#else

namespace mooncake {

std::unique_ptr<NvmeKvCommandExecutor> CreateNvmeKvIoUringExecutor(
    const std::string& /*device_id*/, std::filesystem::path /*storage_path*/,
    std::string /*device_path*/, uint32_t /*nsid*/, uint32_t /*queue_depth*/,
    uint32_t /*runtime_transfer_limit*/,
    tl::expected<NvmeKvCommandExecutor::Capabilities, ErrorCode>&
        capabilities) {
    capabilities = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    return nullptr;
}

}  // namespace mooncake

#endif  // MOONCAKE_HAVE_NVME_URING_CMD
