// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

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

class ChildProcessGuard {
   public:
    ChildProcessGuard(pid_t pid, int stop_fd) : pid_(pid), stop_fd_(stop_fd) {}

    ~ChildProcessGuard() {
        if (pid_ <= 0) return;
        close(stop_fd_);
        (void)waitpid(pid_, nullptr, 0);
    }

    int finish() {
        close(stop_fd_);
        int status = 0;
        (void)waitpid(pid_, &status, 0);
        pid_ = -1;
        stop_fd_ = -1;
        return status;
    }

    int reap() {
        int status = 0;
        (void)waitpid(pid_, &status, 0);
        close(stop_fd_);
        pid_ = -1;
        stop_fd_ = -1;
        return status;
    }

   private:
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
    config->set("transports/hp_tcp/queue_capacity_per_worker", 64);
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
                        (void)read(stop_pipe[0], &stop, 1);
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
    const ssize_t received =
        read(ready_pipe[0], &segment_length, sizeof(segment_length));
    if (received != static_cast<ssize_t>(sizeof(segment_length))) {
        const int status = child_guard.reap();
        GTEST_SKIP() << "HP TCP server initialization failed, child status "
                     << status;
    }
    std::string server_segment(segment_length, '\0');
    ASSERT_EQ(read(ready_pipe[0], server_segment.data(), segment_length),
              static_cast<ssize_t>(segment_length));
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

    const int status = child_guard.finish();
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}

TEST(HighPerformanceTcpE2eTest, WriteThenReadConcurrency1) {
    RunWriteThenReadAcrossProcesses(1);
}

TEST(HighPerformanceTcpE2eTest, WriteThenReadConcurrency4) {
    RunWriteThenReadAcrossProcesses(4);
}

TEST(HighPerformanceTcpE2eTest, WriteThenReadConcurrency16) {
    RunWriteThenReadAcrossProcesses(16);
}

}  // namespace
}  // namespace mooncake::tent
