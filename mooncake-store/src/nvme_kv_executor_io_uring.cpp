#include "nvme_kv_executor.h"

#ifdef MOONCAKE_HAVE_NVME_URING_CMD

#include "nvme_kv_executor_util.h"
#include "nvme_kv_object_layout.h"

#include <fcntl.h>
#include <liburing.h>
#include <linux/nvme_ioctl.h>
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
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

namespace mooncake {
namespace {

bool CanSubmitCallerBufferDirectly(std::string_view value,
                                   uint32_t submission_bytes) {
    if (value.empty() || submission_bytes != value.size()) {
        return false;
    }
    const size_t required_alignment = std::max<size_t>(
        kDefaultNvmeKvTransferAlignmentBytes, NvmeKvTransferAlignmentBytes());
    return reinterpret_cast<uintptr_t>(value.data()) % required_alignment == 0;
}

bool IsCharacterDevice(const std::filesystem::path& path) {
    struct stat st{};
    return ::stat(path.c_str(), &st) == 0 && S_ISCHR(st.st_mode);
}

std::string ResolveIoUringDevicePath(const std::string& device_path) {
    const std::filesystem::path path(device_path);
    if (IsCharacterDevice(path)) {
        return device_path;
    }

    const std::string filename = path.filename().string();
    if (filename.rfind("nvme", 0) != 0) {
        return device_path;
    }
    const std::filesystem::path generic_path =
        path.parent_path() / ("ng" + filename.substr(4));
    if (IsCharacterDevice(generic_path)) {
        LOG(INFO) << "[NvmeKvIoUringExecutor] using NVMe generic char device "
                  << generic_path << " for namespace block device "
                  << device_path;
        return generic_path.string();
    }
    return device_path;
}

class SharedNvmeUringRing {
   public:
    struct BatchCommand {
        struct nvme_uring_cmd cmd{};
        bool is_write = false;
        size_t context_index = 0;
        uint32_t observed_result = 0;
        bool has_observed_result = false;
        ErrorCode mapped_error = ErrorCode::OK;
        bool submitted = false;
        bool completed = false;
    };

    static SharedNvmeUringRing& Instance(unsigned queue_depth) {
        const unsigned normalized_queue_depth =
            queue_depth == 0 ? kDefaultNvmeKvQueueDepth : queue_depth;
        thread_local std::unique_ptr<SharedNvmeUringRing> ring;
        if (ring == nullptr || ring->QueueDepth() != normalized_queue_depth) {
            ring.reset(new SharedNvmeUringRing(normalized_queue_depth));
        }
        return *ring;
    }

    bool IsInitialized() const { return initialized_; }

    tl::expected<void, ErrorCode> SubmitBatch(
        int fd, std::vector<BatchCommand>& commands) {
        if (commands.empty()) {
            return {};
        }
        if (!initialized_) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        if (commands.size() >= UINT32_MAX) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        const uint32_t completion_token = NextCompletionToken();
        size_t next_command = 0;
        size_t completed_count = 0;
        size_t inflight_ops = 0;
        ErrorCode batch_error = ErrorCode::OK;

        const auto drain_ready = [&]() -> tl::expected<size_t, ErrorCode> {
            size_t drained = 0;
            while (inflight_ops != 0) {
                io_uring_cqe* cqe = nullptr;
                const int peek_ret = io_uring_peek_cqe(&ring_, &cqe);
                if (peek_ret == -EAGAIN) {
                    break;
                }
                if (peek_ret < 0 || cqe == nullptr) {
                    return tl::make_unexpected(
                        MapNvmeKvTransportError(peek_ret < 0 ? -peek_ret : EIO,
                                                commands.front().is_write));
                }
                const uint64_t user_data = cqe->user_data;
                const int res = cqe->res;
                const uint64_t cqe_extra_result =
                    cqe32_enabled_ ? cqe->big_cqe[0] : static_cast<uint64_t>(0);
                io_uring_cqe_seen(&ring_, cqe);

                const auto complete_unknown_inflight = [&]() -> bool {
                    for (auto& command : commands) {
                        if (!command.submitted || command.completed) {
                            continue;
                        }
                        command.mapped_error = ErrorCode::INTERNAL_ERROR;
                        command.completed = true;
                        --inflight_ops;
                        ++completed_count;
                        ++drained;
                        return true;
                    }
                    return false;
                };

                size_t command_index = 0;
                if (!DecodeUserData(user_data, completion_token,
                                    command_index) ||
                    command_index >= commands.size() ||
                    !commands[command_index].submitted ||
                    commands[command_index].completed) {
                    LOG(ERROR) << "[NvmeKvIoUringExecutor] invalid or stale "
                                  "io_uring completion token";
                    batch_error = ErrorCode::INTERNAL_ERROR;
                    if (!complete_unknown_inflight()) {
                        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                    }
                    continue;
                }
                auto& command = commands[command_index];
                if (res >= 0 && cqe32_enabled_ &&
                    cqe_extra_result <= UINT32_MAX) {
                    command.observed_result =
                        static_cast<uint32_t>(cqe_extra_result);
                    command.has_observed_result = true;
                }
                if (res > 0) {
                    command.mapped_error = MapNvmeKvStatus(
                        static_cast<uint32_t>(res), command.is_write);
                } else if (res < 0) {
                    command.mapped_error =
                        MapNvmeKvTransportError(-res, command.is_write);
                } else {
                    command.mapped_error = ErrorCode::OK;
                }
                command.completed = true;
                --inflight_ops;
                ++completed_count;
                ++drained;
            }
            return drained;
        };

        const auto wait_and_drain =
            [&](unsigned wait_min) -> tl::expected<size_t, ErrorCode> {
            io_uring_cqe* first_cqe = nullptr;
            int wait_ret = 0;
            do {
                wait_ret = io_uring_wait_cqe_nr(&ring_, &first_cqe, wait_min);
            } while (wait_ret == -EINTR);
            if (wait_ret < 0 || first_cqe == nullptr) {
                return tl::make_unexpected(MapNvmeKvTransportError(
                    wait_ret < 0 ? -wait_ret : EIO, commands.front().is_write));
            }
            // wait_cqe_nr leaves first_cqe in the CQ; drain_ready consumes it
            // together with any other completions that are already available.
            return drain_ready();
        };

        while (completed_count < commands.size()) {
            const size_t prepared_start = next_command;
            size_t prepared_ops = 0;
            while (next_command < commands.size() &&
                   inflight_ops + prepared_ops < queue_depth_) {
                auto& command = commands[next_command];
                io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
                if (sqe == nullptr) {
                    break;
                }
                PrepareSqe(sqe, fd, command.cmd,
                           EncodeUserData(completion_token, next_command));
                ++next_command;
                ++prepared_ops;
            }
            if (prepared_ops == 0 && inflight_ops == 0) {
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }

            ErrorCode submit_error = ErrorCode::OK;
            size_t submitted_prepared = 0;
            while (submitted_prepared < prepared_ops) {
                int submit_ret = 0;
                do {
                    submit_ret = submitted_prepared == 0
                                     ? io_uring_submit_and_wait(&ring_, 1)
                                     : io_uring_submit(&ring_);
                } while (submit_ret == -EINTR);
                const size_t remaining = prepared_ops - submitted_prepared;
                if (submit_ret <= 0 ||
                    static_cast<size_t>(submit_ret) > remaining) {
                    submit_error = MapNvmeKvTransportError(
                        submit_ret < 0 ? -submit_ret : EIO,
                        commands.front().is_write);
                    break;
                }
                const size_t accepted = static_cast<size_t>(submit_ret);
                for (size_t index = 0; index < accepted; ++index) {
                    auto& command =
                        commands[prepared_start + submitted_prepared + index];
                    command.submitted = true;
                    ++inflight_ops;
                }
                submitted_prepared += accepted;
            }

            if (submit_error != ErrorCode::OK) {
                while (inflight_ops != 0) {
                    auto drain_result = wait_and_drain(1);
                    if (!drain_result || drain_result.value() == 0) {
                        const ErrorCode drain_error =
                            drain_result ? ErrorCode::INTERNAL_ERROR
                                         : drain_result.error();
                        ResetRing();
                        return tl::make_unexpected(drain_error);
                    }
                }
                ResetRing();
                return tl::make_unexpected(submit_error);
            }

            auto drain_result = wait_and_drain(1);
            if (!drain_result || drain_result.value() == 0) {
                const ErrorCode drain_error = drain_result
                                                  ? ErrorCode::INTERNAL_ERROR
                                                  : drain_result.error();
                ResetRing();
                return tl::make_unexpected(drain_error);
            }
        }
        if (batch_error != ErrorCode::OK) {
            ResetRing();
            return tl::make_unexpected(batch_error);
        }
        return {};
    }

   private:
    explicit SharedNvmeUringRing(unsigned queue_depth)
        : queue_depth_(queue_depth == 0 ? kDefaultNvmeKvQueueDepth
                                        : queue_depth) {
        InitializeRing();
    }

    void InitializeRing() {
        io_uring_params params{};
        // IORING_SETUP_SQE128 is required for NVMe uring_cmd passthrough
        // because sizeof(struct nvme_uring_cmd) = 72 bytes, which exceeds the
        // 16 bytes available in the cmd[] area of a standard 64-byte SQE.
        params.flags = IORING_SETUP_SQE128 | IORING_SETUP_CQE32;
        int ret = io_uring_queue_init_params(queue_depth_, &ring_, &params);
        if (ret < 0 && (params.flags & IORING_SETUP_CQE32)) {
            LOG(WARNING)
                << "[NvmeKvIoUringExecutor] io_uring_queue_init_params with "
                   "CQE32 failed: "
                << strerror(-ret) << ", retrying with SQE128 only";
            params.flags = IORING_SETUP_SQE128;
            ret = io_uring_queue_init_params(queue_depth_, &ring_, &params);
        }
        if (ret < 0) {
            LOG(ERROR) << "[NvmeKvIoUringExecutor] io_uring queue init failed: "
                       << strerror(-ret);
            return;
        }
        cqe32_enabled_ = (ring_.flags & IORING_SETUP_CQE32) != 0;
        initialized_ = true;
    }

    void ResetRing() {
        if (initialized_) {
            io_uring_queue_exit(&ring_);
        }
        ring_ = {};
        initialized_ = false;
        cqe32_enabled_ = false;
        InitializeRing();
    }

    unsigned QueueDepth() const { return queue_depth_; }

    static void PrepareSqe(io_uring_sqe* sqe, int fd,
                           const struct nvme_uring_cmd& cmd,
                           uint64_t user_data) {
        static_assert(sizeof(cmd) <= 80,
                      "nvme_uring_cmd must fit in SQE128 cmd area");
        constexpr size_t kSqe128Bytes = sizeof(io_uring_sqe) * 2;
        std::memset(sqe, 0, kSqe128Bytes);
        sqe->opcode = IORING_OP_URING_CMD;
        sqe->fd = fd;
        sqe->cmd_op = NVME_URING_CMD_IO;
        sqe->len = sizeof(struct nvme_uring_cmd);
        sqe->user_data = user_data;
        std::memcpy(sqe->cmd, &cmd, sizeof(cmd));
    }

    uint32_t NextCompletionToken() {
        ++completion_token_;
        if (completion_token_ == 0) {
            ++completion_token_;
        }
        return completion_token_;
    }

    static uint64_t EncodeUserData(uint32_t token, size_t command_index) {
        return (static_cast<uint64_t>(token) << 32) |
               (static_cast<uint32_t>(command_index) + 1u);
    }

    static bool DecodeUserData(uint64_t user_data, uint32_t expected_token,
                               size_t& command_index) {
        const uint32_t token = static_cast<uint32_t>(user_data >> 32);
        const uint32_t encoded_index = static_cast<uint32_t>(user_data);
        if (token != expected_token || encoded_index == 0) {
            return false;
        }
        command_index = static_cast<size_t>(encoded_index - 1u);
        return true;
    }

   public:
    ~SharedNvmeUringRing() {
        if (initialized_) {
            io_uring_queue_exit(&ring_);
        }
    }

   private:
    io_uring ring_{};
    unsigned queue_depth_ = kDefaultNvmeKvQueueDepth;
    uint32_t completion_token_ = 0;
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
        const std::string io_uring_device_path =
            ResolveIoUringDevicePath(device_path_);
        if (!IsCharacterDevice(io_uring_device_path)) {
            LOG(WARNING) << "[NvmeKvIoUringExecutor] io_uring requires an NVMe "
                            "generic character device, configured path="
                         << device_path_
                         << " resolved path=" << io_uring_device_path;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        fd_ = ::open(io_uring_device_path.c_str(), O_RDWR | O_CLOEXEC);
        if (fd_ < 0) {
            LOG(ERROR) << "[NvmeKvIoUringExecutor] open failed for "
                       << io_uring_device_path << " (configured "
                       << device_path_ << "): " << strerror(errno);
            return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
        }

        auto& ring = CurrentThreadRing();
        if (!ring.IsInitialized()) {
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
                                        std::string value) override {
        StoreRequest request;
        request.key = key;
        request.value = value;
        std::vector<StoreRequest> requests;
        requests.push_back(std::move(request));
        StoreBatch(requests);
        return std::move(requests.front().result);
    }

    void StoreBatch(std::vector<StoreRequest>& requests) override {
        std::vector<NvmeKvAlignedBuffer> dma_buffers;
        dma_buffers.reserve(requests.size());
        std::vector<SharedNvmeUringRing::BatchCommand> commands;
        commands.reserve(requests.size());

        for (size_t index = 0; index < requests.size(); ++index) {
            auto& request = requests[index];
            if (request.value.size() > capabilities_.effective_max_value_size) {
                request.result = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                continue;
            }
            const uint32_t value_size =
                static_cast<uint32_t>(request.value.size());
            const uint32_t submission_bytes =
                ResolveNvmeKvStoreSubmissionBytes(value_size);
            if (submission_bytes > capabilities_.effective_max_value_size) {
                request.result = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                continue;
            }
            const char* submission_data = request.value.data();
            if (!CanSubmitCallerBufferDirectly(request.value,
                                               submission_bytes)) {
                auto dma_buffer = AllocateNvmeKvAlignedBuffer(submission_bytes);
                if (dma_buffer == nullptr) {
                    request.result =
                        tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                    continue;
                }
                std::memset(dma_buffer.get(), 0, submission_bytes);
                if (!request.value.empty()) {
                    std::memcpy(dma_buffer.get(), request.value.data(),
                                request.value.size());
                }
                submission_data = dma_buffer.get();
                dma_buffers.push_back(std::move(dma_buffer));
            }

            SharedNvmeUringRing::BatchCommand command;
            command.is_write = true;
            command.context_index = index;
            BuildNvmeKvStoreCommand(command.cmd, nsid_, request.key,
                                    submission_data, submission_bytes);

            commands.push_back(std::move(command));
        }

        auto submit_result = CurrentThreadRing().SubmitBatch(fd_, commands);
        const ErrorCode uncompleted_error =
            submit_result ? ErrorCode::INTERNAL_ERROR : submit_result.error();
        for (const auto& command : commands) {
            if (command.completed && command.mapped_error == ErrorCode::OK) {
                requests[command.context_index].result = {};
            } else {
                requests[command.context_index].result =
                    tl::make_unexpected(command.completed ? command.mapped_error
                                                          : uncompleted_error);
            }
        }
    }

    tl::expected<std::string, ErrorCode> Retrieve(
        const PhysicalKey& key, uint32_t size_hint = 0) const override {
        RetrieveBufferRequest request;
        request.key = key;
        request.size_hint = size_hint;
        std::vector<RetrieveBufferRequest> requests;
        requests.push_back(std::move(request));
        RetrieveBufferBatch(requests);
        if (!requests.front().result) {
            return tl::make_unexpected(requests.front().result.error());
        }
        return requests.front().result->ToString();
    }

    void RetrieveBufferBatch(
        std::vector<RetrieveBufferRequest>& requests) const override {
        struct RetrieveAttempt {
            size_t request_index = 0;
            uint32_t request_bytes = 0;
            NvmeKvAlignedBuffer dma_buffer;
            uint32_t returned_size = 0;
            ErrorCode error = ErrorCode::OK;
            bool completed = false;
        };
        const auto make_attempt = [](size_t request_index,
                                     uint32_t request_bytes) {
            RetrieveAttempt attempt;
            attempt.request_index = request_index;
            attempt.request_bytes = request_bytes;
            return attempt;
        };

        const auto submit_attempts = [&](std::vector<RetrieveAttempt>& attempts)
            -> tl::expected<void, ErrorCode> {
            std::vector<SharedNvmeUringRing::BatchCommand> commands;
            commands.reserve(attempts.size());
            for (size_t attempt_index = 0; attempt_index < attempts.size();
                 ++attempt_index) {
                auto& attempt = attempts[attempt_index];
                const uint32_t transfer_bytes =
                    RoundUpToNvmeKvTransferBytes(attempt.request_bytes);
                if (transfer_bytes > capabilities_.effective_max_value_size) {
                    attempt.error = ErrorCode::BUFFER_OVERFLOW;
                    attempt.completed = true;
                    continue;
                }
                attempt.dma_buffer =
                    AllocateNvmeKvAlignedBuffer(transfer_bytes);
                if (attempt.dma_buffer == nullptr) {
                    attempt.error = ErrorCode::INTERNAL_ERROR;
                    attempt.completed = true;
                    continue;
                }

                const auto& request = requests[attempt.request_index];
                SharedNvmeUringRing::BatchCommand command;
                command.is_write = false;
                command.context_index = attempt_index;
                BuildNvmeKvRetrieveCommand(command.cmd, nsid_, request.key,
                                           attempt.dma_buffer.get(),
                                           transfer_bytes);
                commands.push_back(std::move(command));
            }

            auto submit_result = CurrentThreadRing().SubmitBatch(fd_, commands);
            const ErrorCode uncompleted_error = submit_result
                                                    ? ErrorCode::INTERNAL_ERROR
                                                    : submit_result.error();
            for (const auto& command : commands) {
                auto& attempt = attempts[command.context_index];
                attempt.completed = command.completed;
                attempt.error = command.completed ? command.mapped_error
                                                  : uncompleted_error;
                if (command.completed &&
                    command.mapped_error == ErrorCode::OK) {
                    attempt.returned_size = command.has_observed_result
                                                ? command.observed_result
                                                : 0;
                }
            }
            if (!submit_result) {
                return tl::make_unexpected(submit_result.error());
            }
            return {};
        };

        const auto finish_successful_attempt =
            [&](RetrieveAttempt& attempt, uint32_t prefix_limit,
                bool allow_retry,
                std::vector<RetrieveAttempt>& retry_attempts) {
                auto& request = requests[attempt.request_index];
                if (attempt.error != ErrorCode::OK ||
                    attempt.dma_buffer == nullptr) {
                    request.result = tl::make_unexpected(attempt.error);
                    return;
                }

                const uint32_t actual_size = ResolveNvmeKvRetrievedValueSize(
                    attempt.dma_buffer.get(), attempt.returned_size,
                    prefix_limit, request.size_hint);
                if (actual_size != 0 && actual_size <= prefix_limit) {
                    char* data = attempt.dma_buffer.get();
                    std::shared_ptr<void> owner(attempt.dma_buffer.release(),
                                                NvmeKvFreeDeleter());
                    request.result = RetrievedBuffer{
                        .owner = std::move(owner),
                        .data = data,
                        .size = actual_size,
                    };
                    return;
                }

                uint32_t required_size = attempt.returned_size;
                if (required_size <= prefix_limit) {
                    required_size = ResolveNvmeKvObjectBlobSizeFromPrefix(
                        attempt.dma_buffer.get(), prefix_limit);
                }
                if (allow_retry && required_size > prefix_limit &&
                    required_size <= capabilities_.effective_max_value_size) {
                    retry_attempts.push_back(
                        make_attempt(attempt.request_index, required_size));
                    return;
                }
                request.result = tl::make_unexpected(
                    required_size > capabilities_.effective_max_value_size
                        ? ErrorCode::BUFFER_OVERFLOW
                        : ErrorCode::FILE_READ_FAIL);
            };

        std::vector<RetrieveAttempt> initial_attempts;
        initial_attempts.reserve(requests.size());
        std::vector<uint32_t> initial_request_bytes(requests.size(), 0);
        for (size_t index = 0; index < requests.size(); ++index) {
            const uint32_t request_bytes = ResolveNvmeKvInitialRetrieveBytes(
                requests[index].size_hint,
                capabilities_.effective_max_value_size);
            initial_request_bytes[index] = request_bytes;
            initial_attempts.push_back(make_attempt(index, request_bytes));
        }

        (void)submit_attempts(initial_attempts);

        std::vector<RetrieveAttempt> fallback_attempts;
        std::vector<RetrieveAttempt> retry_attempts;
        for (auto& attempt : initial_attempts) {
            const auto& request = requests[attempt.request_index];
            const uint32_t initial_bytes =
                initial_request_bytes[attempt.request_index];
            if (attempt.error != ErrorCode::OK) {
                if (ShouldRetryNvmeKvRetrieveWithMaxBuffer(
                        attempt.error, request.size_hint, initial_bytes,
                        capabilities_.effective_max_value_size)) {
                    fallback_attempts.push_back(
                        make_attempt(attempt.request_index,
                                     capabilities_.effective_max_value_size));
                } else {
                    requests[attempt.request_index].result =
                        tl::make_unexpected(attempt.error);
                }
                continue;
            }
            finish_successful_attempt(attempt, initial_bytes, true,
                                      retry_attempts);
        }

        if (!fallback_attempts.empty()) {
            (void)submit_attempts(fallback_attempts);
            for (auto& attempt : fallback_attempts) {
                finish_successful_attempt(
                    attempt, capabilities_.effective_max_value_size, false,
                    retry_attempts);
            }
        }

        if (!retry_attempts.empty()) {
            (void)submit_attempts(retry_attempts);
            std::vector<RetrieveAttempt> ignored_retries;
            for (auto& attempt : retry_attempts) {
                finish_successful_attempt(attempt, attempt.request_bytes, false,
                                          ignored_retries);
            }
        }
    }

    void RetrieveIntoBatch(
        std::vector<RetrieveIntoRequest>& requests) const override {
        std::vector<SharedNvmeUringRing::BatchCommand> commands;
        commands.reserve(requests.size());
        for (size_t index = 0; index < requests.size(); ++index) {
            auto& request = requests[index];
            const uint32_t transfer_bytes =
                RoundUpToNvmeKvTransferBytes(request.size);
            const auto address = reinterpret_cast<uintptr_t>(request.data);
            if (request.data == nullptr || request.size == 0 ||
                transfer_bytes != request.size ||
                transfer_bytes > capabilities_.effective_max_value_size ||
                address % NvmeKvTransferAlignmentBytes() != 0) {
                request.result = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                continue;
            }

            SharedNvmeUringRing::BatchCommand command;
            command.is_write = false;
            command.context_index = index;
            BuildNvmeKvRetrieveCommand(command.cmd, nsid_, request.key,
                                       request.data, transfer_bytes);
            commands.push_back(std::move(command));
        }

        auto submit_result = CurrentThreadRing().SubmitBatch(fd_, commands);
        for (const auto& command : commands) {
            auto& request = requests[command.context_index];
            if (!command.completed) {
                request.result = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                continue;
            }
            if (command.mapped_error != ErrorCode::OK) {
                request.result = tl::make_unexpected(command.mapped_error);
                continue;
            }
            const uint32_t actual_size = command.has_observed_result
                                             ? command.observed_result
                                             : request.size;
            if (actual_size != request.size) {
                request.result = tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
                continue;
            }
            request.result = actual_size;
        }
        if (!submit_result) {
            for (auto& request : requests) {
                if (!request.result &&
                    request.result.error() == ErrorCode::INTERNAL_ERROR) {
                    request.result = tl::make_unexpected(submit_result.error());
                }
            }
        }
    }

    tl::expected<void, ErrorCode> Delete(const PhysicalKey& key) override {
        SharedNvmeUringRing::BatchCommand command;
        command.is_write = true;
        BuildNvmeKvDeleteCommand(command.cmd, nsid_, key);
        std::vector<SharedNvmeUringRing::BatchCommand> commands;
        commands.push_back(std::move(command));
        auto submit_result = CurrentThreadRing().SubmitBatch(fd_, commands);
        const auto& completed = commands.front();
        if (!completed.completed) {
            return tl::make_unexpected(submit_result ? ErrorCode::INTERNAL_ERROR
                                                     : submit_result.error());
        }
        if (completed.mapped_error != ErrorCode::OK) {
            return tl::make_unexpected(completed.mapped_error);
        }
        return {};
    }

    const Capabilities& GetCapabilities() const override {
        return capabilities_;
    }

   private:
    SharedNvmeUringRing& CurrentThreadRing() const {
        return SharedNvmeUringRing::Instance(capabilities_.queue_depth);
    }

    std::string device_path_;
    uint32_t nsid_ = 1;
    Capabilities capabilities_;
    int fd_ = -1;
};

}  // namespace

NvmeKvExecutorResult CreateNvmeKvIoUringExecutor(
    std::string device_path, uint32_t nsid, uint32_t queue_depth,
    uint32_t runtime_transfer_limit) {
    auto caps = BuildNvmeKvCapabilities(kDefaultNvmeKvQueueDepth, queue_depth,
                                        runtime_transfer_limit);

    auto executor = std::make_unique<NvmeKvIoUringExecutor>(
        std::move(device_path), nsid, caps);
    auto init_res = executor->Init();
    if (!init_res) {
        return tl::make_unexpected(init_res.error());
    }
    return std::unique_ptr<NvmeKvCommandExecutor>(std::move(executor));
}

}  // namespace mooncake

#else

namespace mooncake {

NvmeKvExecutorResult CreateNvmeKvIoUringExecutor(
    std::string /*device_path*/, uint32_t /*nsid*/, uint32_t /*queue_depth*/,
    uint32_t /*runtime_transfer_limit*/) {
    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
}

}  // namespace mooncake

#endif  // MOONCAKE_HAVE_NVME_URING_CMD
