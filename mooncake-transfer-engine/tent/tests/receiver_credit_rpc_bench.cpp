// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/runtime/control_plane.h"

#include <sys/resource.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace mooncake::tent {
namespace {

using Clock = std::chrono::steady_clock;

constexpr size_t resourceIndex(CreditResource resource) {
    return static_cast<size_t>(resource) - 1;
}

double processCpuSeconds() {
    rusage usage{};
    if (getrusage(RUSAGE_SELF, &usage) != 0) return 0;
    return usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1e6 +
           usage.ru_stime.tv_sec + usage.ru_stime.tv_usec / 1e6;
}

uint64_t percentile(const std::vector<uint64_t>& sorted, double fraction) {
    if (sorted.empty()) return 0;
    const auto index =
        static_cast<size_t>(fraction * static_cast<double>(sorted.size() - 1));
    return sorted[index];
}

std::shared_ptr<ReceiverCreditAllocator> makeAllocator() {
    ReceiverCreditAllocatorConfig config;
    config.capacity[resourceIndex(CreditResource::DataBytes)] = 1ULL << 50;
    config.capacity[resourceIndex(CreditResource::RequestSlots)] = 1ULL << 30;
    config.max_grant_per_pull[resourceIndex(CreditResource::DataBytes)] = 64ULL
                                                                          << 20;
    config.max_grant_per_pull[resourceIndex(CreditResource::RequestSlots)] = 64;
    config.max_entries = 4096;
    config.ttl_ms = 60'000;
    config.retry_after_us = 100;
    config.receiver_session_id = {0x1234, 0x5678};
    config.epoch = 1;

    std::unique_ptr<ReceiverCreditAllocator> allocator;
    auto status = ReceiverCreditAllocator::create(config, allocator);
    if (!status.ok()) return nullptr;
    return std::shared_ptr<ReceiverCreditAllocator>(std::move(allocator));
}

ReceiverCreditPullRequestV1 makeRequest(uint64_t sender_peer) {
    ReceiverCreditPullRequestV1 request;
    request.sender_peer = sender_peer;
    request.qos_class = 0;
    request.request_sequence = 1;
    request.resources = {
        {CreditResource::DataBytes, 0, 0, 4ULL << 20, 64ULL << 20},
        {CreditResource::RequestSlots, 0, 0, 1, 64},
    };
    return request;
}

int runServer(uint16_t port, uint64_t duration_seconds) {
    auto allocator = makeAllocator();
    if (!allocator) return 2;
    ControlService service("p2p", "", nullptr);
    service.setReceiverCreditAllocator(allocator);
    auto status = service.start(port);
    if (!status.ok()) {
        std::cerr << status.ToString() << '\n';
        return 3;
    }

    const double cpu_start = processCpuSeconds();
    const auto wall_start = Clock::now();
    std::cout << "READY port=" << port << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(duration_seconds));
    const double wall_seconds =
        std::chrono::duration<double>(Clock::now() - wall_start).count();
    const double cpu_seconds = processCpuSeconds() - cpu_start;

    ReceiverCreditAllocatorSnapshot snapshot;
    status = allocator->snapshot(snapshot);
    if (!status.ok()) return 4;
    std::cout << "{\"role\":\"server\",\"wall_s\":" << wall_seconds
              << ",\"cpu_s\":" << cpu_seconds
              << ",\"cpu_cores\":" << cpu_seconds / wall_seconds
              << ",\"peer_entries\":" << snapshot.entries << "}" << std::endl;
    return 0;
}

int runClient(const std::string& address, uint64_t sender_peer,
              uint64_t measured_calls, uint64_t warmup_calls) {
    if (sender_peer == 0 || measured_calls == 0) return 2;
    auto request = makeRequest(sender_peer);
    const uint64_t total_calls = measured_calls + warmup_calls;
    std::vector<uint64_t> latency_ns;
    latency_ns.reserve(measured_calls);
    uint64_t granted = 0;
    uint64_t retries = 0;
    size_t request_wire_bytes = 0;

    const double cpu_start = processCpuSeconds();
    auto wall_start = Clock::now();
    for (uint64_t call = 0; call < total_calls; ++call) {
        if (call == warmup_calls) wall_start = Clock::now();
        const auto start = Clock::now();
        ReceiverCreditPullResponseV1 response;
        auto status =
            ControlClient::pullReceiverCredit(address, request, response);
        const auto elapsed =
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() -
                                                                 start)
                .count();
        if (!status.ok()) {
            std::cerr << status.ToString() << '\n';
            return 3;
        }
        if (response.status == ReceiverCreditPullStatus::Retry) {
            ++retries;
        } else if (response.status != ReceiverCreditPullStatus::Granted) {
            std::cerr << "unexpected response status "
                      << static_cast<uint16_t>(response.status) << '\n';
            return 4;
        }
        if (call >= warmup_calls) latency_ns.push_back(elapsed);

        request.expected_receiver_session_id =
            response.activation.receiver_session_id;
        request.expected_epoch = response.activation.epoch;
        request.last_update_sequence = response.update.sequence;
        ++request.request_sequence;
        for (const auto& grant : response.update.grants)
            if (grant.resource == CreditResource::DataBytes)
                granted = grant.grant_total;
    }
    const double wall_seconds =
        std::chrono::duration<double>(Clock::now() - wall_start).count();
    const double cpu_seconds = processCpuSeconds() - cpu_start;

    std::string request_wire;
    if (!ReceiverCreditPullRequestCodecV1::encode(request, request_wire).ok())
        return 5;
    request_wire_bytes = request_wire.size();
    std::sort(latency_ns.begin(), latency_ns.end());
    const uint64_t response_wire_bytes =
        ReceiverCreditPullResponseCodecV1::kWireBytes;
    const uint64_t wire_payload_bytes =
        measured_calls * (request_wire_bytes + response_wire_bytes);
    std::cout << "{\"role\":\"client\",\"sender_peer\":" << sender_peer
              << ",\"calls\":" << measured_calls
              << ",\"warmup_calls\":" << warmup_calls
              << ",\"request_bytes\":" << request_wire_bytes
              << ",\"response_bytes\":" << response_wire_bytes
              << ",\"wire_payload_bytes\":" << wire_payload_bytes
              << ",\"wall_s\":" << wall_seconds << ",\"cpu_s\":" << cpu_seconds
              << ",\"qps\":" << measured_calls / wall_seconds
              << ",\"rtt_p50_us\":" << percentile(latency_ns, 0.50) / 1000.0
              << ",\"rtt_p95_us\":" << percentile(latency_ns, 0.95) / 1000.0
              << ",\"rtt_p99_us\":" << percentile(latency_ns, 0.99) / 1000.0
              << ",\"rtt_max_us\":" << latency_ns.back() / 1000.0
              << ",\"retries\":" << retries << ",\"granted_bytes\":" << granted
              << "}" << std::endl;
    return 0;
}

}  // namespace
}  // namespace mooncake::tent

int main(int argc, char** argv) {
    using namespace mooncake::tent;
    if (argc == 4 && std::string(argv[1]) == "server")
        return runServer(static_cast<uint16_t>(std::stoul(argv[2])),
                         std::stoull(argv[3]));
    if (argc == 6 && std::string(argv[1]) == "client")
        return runClient(argv[2], std::stoull(argv[3]), std::stoull(argv[4]),
                         std::stoull(argv[5]));
    std::cerr << "server <port> <duration_s> | client <ip:port> <sender_peer> "
                 "<calls> <warmup_calls>\n";
    return 1;
}
