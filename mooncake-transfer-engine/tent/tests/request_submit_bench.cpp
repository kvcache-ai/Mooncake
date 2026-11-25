// Copyright 2026 KVCache.AI
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

// Public submit-path benchmark over TCP loopback. Compare the same command
// before/after a change; submit_us excludes allocation and completion polling,
// while completed_us includes submission and waiting for the payload write.
// Usage: request_submit_bench [requests=1024] [iterations=100] [merge=1]
//                            [bytes_per_request=64]
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/transfer_engine.h"

using namespace mooncake::tent;
using Clock = std::chrono::steady_clock;

namespace {
void check(const Status& status) {
    if (!status.ok()) throw std::runtime_error(status.ToString());
}

std::shared_ptr<Config> config(bool merge) {
    auto result = std::make_shared<Config>();
    result->set("metadata_type", "p2p");
    result->set("metadata_servers", "P2PHANDSHAKE");
    result->set("rpc_server_hostname", "127.0.0.1");
    result->set("rpc_server_port", "0");
    result->set("rpc_server_threads", 2);
    result->set("log_level", "error");
    result->set("merge_requests", merge);
    result->set("transports/tcp/enable", true);
    result->set("transports/rdma/enable", false);
    result->set("transports/shm/enable", false);
    result->set("transports/io_uring/enable", false);
    return result;
}

double median(std::vector<double>& samples) {
    std::sort(samples.begin(), samples.end());
    return samples[samples.size() / 2];
}
}  // namespace

int main(int argc, char** argv) {
    try {
        const size_t count = argc > 1 ? std::stoul(argv[1]) : 1024;
        const size_t iterations = argc > 2 ? std::stoul(argv[2]) : 100;
        const bool merge = argc > 3 ? std::stoul(argv[3]) != 0 : true;
        const size_t bytes = argc > 4 ? std::stoul(argv[4]) : 64;
        if (!count || count > 65536 || !iterations || iterations > 1000000 ||
            !bytes || bytes > 4096)
            throw std::invalid_argument("invalid benchmark dimensions");

        std::vector<uint8_t> source(count * bytes);
        std::vector<uint8_t> destination(source.size());
        for (size_t i = 0; i < source.size(); ++i) source[i] = i * 131 + 7;
        TransferEngine server(config(merge)), client(config(merge));
        if (!server.available() || !client.available())
            throw std::runtime_error("TCP engine initialization failed");
        check(
            server.registerLocalMemory(destination.data(), destination.size()));
        check(client.registerLocalMemory(source.data(), source.size()));
        SegmentID segment{};
        check(client.openSegment(segment, server.getSegmentName()));

        std::vector<Request> requests;
        for (size_t i = 0; i < count; ++i) {
            Request request{};
            request.opcode = Request::WRITE;
            request.source = source.data() + i * bytes;
            request.target_id = segment;
            request.target_offset =
                reinterpret_cast<uint64_t>(destination.data() + i * bytes);
            request.length = bytes;
            request.transport_hint = TCP;
            requests.push_back(request);
        }
        std::mt19937 random(91827);
        std::shuffle(requests.begin(), requests.end(), random);
        std::vector<double> submit_samples, completed_samples;
        for (size_t iteration = 0; iteration < iterations + 10; ++iteration) {
            std::fill(destination.begin(), destination.end(), 0);
            BatchID batch = client.allocateBatch(count);
            if (!batch) throw std::runtime_error("batch allocation failed");
            const auto start = Clock::now();
            check(client.submitTransfer(batch, requests));
            const auto submitted = Clock::now();
            for (;;) {
                TransferStatus status{};
                check(client.getTransferStatus(batch, status));
                if (status.s == COMPLETED) break;
                if (status.s != PENDING && status.s != INITIAL)
                    throw std::runtime_error("transfer failed");
                if (Clock::now() - start > std::chrono::seconds(30))
                    throw std::runtime_error("transfer timed out");
                std::this_thread::yield();
            }
            const auto completed = Clock::now();
            if (source != destination)
                throw std::runtime_error("payload mismatch");
            check(client.freeBatch(batch));
            if (iteration >= 10) {
                submit_samples.push_back(
                    std::chrono::duration<double, std::micro>(submitted - start)
                        .count());
                completed_samples.push_back(
                    std::chrono::duration<double, std::micro>(completed - start)
                        .count());
            }
        }
        std::cout << "requests=" << count << " bytes=" << bytes
                  << " merge=" << merge << " iterations=" << iterations
                  << " submit_us=" << median(submit_samples)
                  << " completed_us=" << median(completed_samples) << '\n';
        check(client.closeSegment(segment));
        check(client.unregisterLocalMemory(source.data(), source.size()));
        check(server.unregisterLocalMemory(destination.data(),
                                           destination.size()));
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
