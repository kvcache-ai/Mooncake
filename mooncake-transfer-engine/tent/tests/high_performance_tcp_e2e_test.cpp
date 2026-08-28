// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include <gtest/gtest.h>
#include <poll.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/transfer_engine.h"

namespace mooncake::tent {
namespace {

constexpr size_t kDataLength = 256 * 1024;
constexpr auto kChildReadyTimeout = std::chrono::seconds(10);
constexpr auto kChildExitTimeout = std::chrono::seconds(10);

bool ReadExactlyWithTimeout(int fd, void* buffer, size_t length,
                            std::string* failure) {
    auto* cursor = static_cast<uint8_t*>(buffer);
    size_t received = 0;
    const auto deadline = std::chrono::steady_clock::now() + kChildReadyTimeout;

    while (received < length) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            *failure = "timed out waiting for the HP TCP server";
            return false;
        }
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline -
                                                                  now);
        pollfd descriptor{fd, POLLIN, 0};
        const int poll_result =
            poll(&descriptor, 1, static_cast<int>(remaining.count() + 1));
        if (poll_result < 0 && errno == EINTR) continue;
        if (poll_result == 0) {
            *failure = "timed out waiting for the HP TCP server";
            return false;
        }
        if (poll_result < 0) {
            *failure = std::string(
                           "poll failed while waiting for the HP TCP "
                           "server: ") +
                       std::strerror(errno);
            return false;
        }

        ssize_t read_result;
        do {
            read_result = read(fd, cursor + received, length - received);
        } while (read_result < 0 && errno == EINTR);
        if (read_result == 0) {
            *failure = "HP TCP server exited before reporting readiness";
            return false;
        }
        if (read_result < 0) {
            *failure = std::string(
                           "read failed while waiting for the HP TCP "
                           "server: ") +
                       std::strerror(errno);
            return false;
        }
        received += static_cast<size_t>(read_result);
    }
    return true;
}

class ChildProcessGuard {
   public:
    enum class FinishResult { kExited, kTimedOutAndKilled, kWaitError };

    ChildProcessGuard(pid_t pid, int stop_fd) : pid_(pid), stop_fd_(stop_fd) {}

    ~ChildProcessGuard() {
        if (pid_ <= 0) return;
        int status = 0;
        (void)finish(&status);
    }

    FinishResult finish(int* status) {
        signalStop();
        const WaitResult wait_result = waitForExit(status);
        FinishResult finish_result = FinishResult::kWaitError;
        if (wait_result == WaitResult::kExited) {
            finish_result = FinishResult::kExited;
        } else if (wait_result == WaitResult::kTimedOut &&
                   killAndReap(status)) {
            finish_result = FinishResult::kTimedOutAndKilled;
        }
        pid_ = -1;
        return finish_result;
    }

   private:
    enum class WaitResult { kExited, kTimedOut, kError };

    void signalStop() {
        if (stop_fd_ < 0) return;
        close(stop_fd_);
        stop_fd_ = -1;
    }

    WaitResult waitForExit(int* status) const {
        const auto deadline =
            std::chrono::steady_clock::now() + kChildExitTimeout;
        for (;;) {
            const pid_t result = waitpid(pid_, status, WNOHANG);
            if (result == pid_) return WaitResult::kExited;
            if (result < 0) {
                if (errno == EINTR) {
                    if (std::chrono::steady_clock::now() >= deadline)
                        return WaitResult::kTimedOut;
                    continue;
                }
                return WaitResult::kError;
            }
            if (std::chrono::steady_clock::now() >= deadline)
                return WaitResult::kTimedOut;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    bool killAndReap(int* status) const {
        if (kill(pid_, SIGKILL) != 0 && errno != ESRCH) return false;
        pid_t result;
        do {
            result = waitpid(pid_, status, 0);
        } while (result < 0 && errno == EINTR);
        return result == pid_;
    }

    pid_t pid_;
    int stop_fd_;
};

std::shared_ptr<Config> MakeHpConfig() {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("metadata_servers", "P2PHANDSHAKE");
    config->set("transports/tcp/enable", false);
    config->set("transports/hp_tcp/enable", true);
    config->set("transports/hp_tcp/bind_address", "127.0.0.1");
    config->set("transports/hp_tcp/advertise_address", "127.0.0.1");
    config->set("transports/hp_tcp/port", 0);
    config->set("transports/hp_tcp/worker_count", 4);
    config->set("transports/hp_tcp/connections_per_peer", 4);
    config->set("transports/hp_tcp/max_outstanding_tasks", 128);
    config->set("transports/hp_tcp/max_outstanding_bytes", 64ULL << 20);
    config->set("transports/hp_tcp/max_transfer_bytes", 8ULL << 20);
    config->set("transports/hp_tcp/chunk_size", 64ULL << 10);
    config->set("transports/hp_tcp/connect_timeout_ms", 2000);
    config->set("transports/hp_tcp/progress_timeout_ms", 5000);
    config->set("transports/rdma/enable", false);
    config->set("transports/shm/enable", false);
    config->set("rpc_server_threads", 1);
    return config;
}

bool WaitBatchDone(TransferEngine& engine, BatchID batch) {
    TransferStatus status;
    for (int i = 0; i < 10000; ++i) {
        const Status result = engine.getTransferStatus(batch, status);
        if (!result.ok() || status.s == FAILED || status.s == CANCELED ||
            status.s == TIMEOUT || status.s == INVALID) {
            return false;
        }
        if (status.s == COMPLETED) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
}

void RunWriteThenReadAcrossProcesses(size_t task_count) {
    const size_t remote_buffer_length = task_count * kDataLength;
    const size_t local_buffer_length = 2 * remote_buffer_length;

    int ready_pipe[2];
    int stop_pipe[2];
    ASSERT_EQ(pipe(ready_pipe), 0);
    ASSERT_EQ(pipe(stop_pipe), 0);

    const pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        close(ready_pipe[0]);
        close(stop_pipe[1]);

        int exit_code = 0;
        {
            TransferEngine server(MakeHpConfig());
            if (!server.available()) {
                exit_code = 2;
            } else {
                std::vector<uint8_t> remote(remote_buffer_length, 0);
                const Status registered = server.registerLocalMemory(
                    remote.data(), remote.size(), kGlobalReadWrite);
                if (!registered.ok()) {
                    exit_code = 3;
                } else {
                    const std::string segment = server.getSegmentName();
                    const uint32_t length =
                        static_cast<uint32_t>(segment.size());
                    if (write(ready_pipe[1], &length, sizeof(length)) !=
                        static_cast<ssize_t>(sizeof(length))) {
                        exit_code = 4;
                    } else if (write(ready_pipe[1], segment.data(), length) !=
                               static_cast<ssize_t>(length)) {
                        exit_code = 5;
                    } else {
                        char stop = 0;
                        ssize_t stop_result = 0;
                        do {
                            stop_result = read(stop_pipe[0], &stop, 1);
                        } while (stop_result < 0 && errno == EINTR);
                        if (stop_result < 0) exit_code = 7;
                    }
                    if (!server
                             .unregisterLocalMemory(remote.data(),
                                                    remote.size())
                             .ok() &&
                        exit_code == 0) {
                        exit_code = 6;
                    }
                }
            }
        }
        close(ready_pipe[1]);
        close(stop_pipe[0]);
        _exit(exit_code);
    }

    close(ready_pipe[1]);
    close(stop_pipe[0]);
    ChildProcessGuard child_guard(child, stop_pipe[1]);

    uint32_t segment_length = 0;
    std::string ready_failure;
    bool ready = ReadExactlyWithTimeout(ready_pipe[0], &segment_length,
                                        sizeof(segment_length), &ready_failure);
    std::string server_segment;
    if (ready) {
        server_segment.resize(segment_length);
        ready = ReadExactlyWithTimeout(ready_pipe[0], server_segment.data(),
                                       segment_length, &ready_failure);
    }
    if (!ready) {
        close(ready_pipe[0]);
        int status = 0;
        const auto finish_result = child_guard.finish(&status);
        if (finish_result ==
            ChildProcessGuard::FinishResult::kTimedOutAndKilled) {
            FAIL() << ready_failure
                   << "; child did not exit before timeout and was killed";
        }
        if (finish_result == ChildProcessGuard::FinishResult::kWaitError) {
            FAIL() << ready_failure << "; failed to wait for child safely";
        }
        if (WIFEXITED(status)) {
            FAIL() << ready_failure << "; child exit code "
                   << WEXITSTATUS(status);
        }
        if (WIFSIGNALED(status)) {
            FAIL() << ready_failure << "; child signal " << WTERMSIG(status);
        }
        FAIL() << ready_failure << "; unexpected child status " << status;
    }
    close(ready_pipe[0]);

    TransferEngine client(MakeHpConfig());
    ASSERT_TRUE(client.available());
    std::vector<uint8_t> local(local_buffer_length, 0);
    for (size_t task = 0; task < task_count; ++task) {
        uint8_t* source = local.data() + task * kDataLength;
        for (size_t i = 0; i < kDataLength; ++i) {
            source[i] = static_cast<uint8_t>((task * 131 + i * 7) & 0xff);
        }
    }
    ASSERT_TRUE(
        client.registerLocalMemory(local.data(), local.size(), kGlobalReadWrite)
            .ok());

    SegmentID segment = 0;
    Status result;
    for (int i = 0; i < 100; ++i) {
        result = client.openSegment(segment, server_segment);
        if (result.ok()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(result.ok()) << result.ToString();

    SegmentInfo info;
    ASSERT_TRUE(client.getSegmentInfo(segment, info).ok());
    ASSERT_FALSE(info.buffers.empty());

    std::vector<Request> writes;
    std::vector<Request> reads;
    writes.reserve(task_count);
    reads.reserve(task_count);
    for (size_t task = 0; task < task_count; ++task) {
        Request write_request{};
        write_request.opcode = Request::WRITE;
        write_request.source = local.data() + task * kDataLength;
        write_request.target_id = segment;
        write_request.target_offset = info.buffers[0].base + task * kDataLength;
        write_request.length = kDataLength;
        write_request.transport_hint = HP_TCP;
        writes.push_back(write_request);

        Request read_request{};
        read_request.opcode = Request::READ;
        read_request.source =
            local.data() + remote_buffer_length + task * kDataLength;
        read_request.target_id = segment;
        read_request.target_offset = info.buffers[0].base + task * kDataLength;
        read_request.length = kDataLength;
        read_request.transport_hint = HP_TCP;
        reads.push_back(read_request);
    }

    BatchID batch = client.allocateBatch(task_count);
    ASSERT_TRUE(client.submitTransfer(batch, writes).ok());
    ASSERT_TRUE(WaitBatchDone(client, batch));
    ASSERT_TRUE(client.freeBatch(batch).ok());

    batch = client.allocateBatch(task_count);
    ASSERT_TRUE(client.submitTransfer(batch, reads).ok());
    ASSERT_TRUE(WaitBatchDone(client, batch));
    ASSERT_TRUE(client.freeBatch(batch).ok());

    for (size_t task = 0; task < task_count; ++task) {
        const uint8_t* written = local.data() + task * kDataLength;
        const uint8_t* read_back =
            local.data() + remote_buffer_length + task * kDataLength;
        EXPECT_EQ(std::memcmp(written, read_back, kDataLength), 0)
            << "task " << task << " mismatch";
    }

    EXPECT_TRUE(client.closeSegment(segment).ok());
    EXPECT_TRUE(client.unregisterLocalMemory(local.data(), local.size()).ok());

    int status = 0;
    const auto finish_result = child_guard.finish(&status);
    ASSERT_TRUE(finish_result == ChildProcessGuard::FinishResult::kExited)
        << (finish_result == ChildProcessGuard::FinishResult::kTimedOutAndKilled
                ? "HP TCP server did not shut down before timeout and was "
                  "killed"
                : "failed to wait for HP TCP server safely");
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST(HighPerformanceTcpE2eTest, WriteThenReadConcurrency16) {
    RunWriteThenReadAcrossProcesses(16);
}

}  // namespace
}  // namespace mooncake::tent
