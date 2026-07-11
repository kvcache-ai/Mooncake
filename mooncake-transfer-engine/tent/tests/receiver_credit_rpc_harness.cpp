// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/rpc/rpc.h"
#include "tent/runtime/receiver_credit_control.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <sstream>
#include <thread>

namespace mooncake::tent {
namespace {
constexpr int kCreditTestRpc = 1001;
constexpr int kCapabilityTestRpc = 1002;
CreditKey key() { return {{1, 2}, 3, 4}; }
ReceiverCreditUpdateV1 update(uint64_t sequence, uint64_t epoch) {
    ReceiverCreditUpdateV1 u;
    u.receiver_session_id = key().receiver_session;
    u.qos_class = key().qos_class;
    u.epoch = epoch;
    u.sequence = sequence;
    u.grants = {{CreditResource::DataBytes, sequence}};
    return u;
}

int runServer(uint16_t port, uint64_t expected_calls, uint64_t drain_delay_us,
              uint64_t epoch) {
    BoundedCreditUpdateInbox inbox(64);
    ReceiverCreditIngress ingress(inbox, key(), epoch);
    SenderCreditLedger ledger;
    if (!ledger.activate(key(), epoch).ok()) return 2;
    std::atomic<uint64_t> accepted{0};
    std::atomic<uint64_t> queue_full{0};
    std::atomic<uint64_t> invalid{0};
    CoroRpcAgent server;
    server.registerFunction(
        kCreditTestRpc, [&](std::string_view wire, std::string& response) {
            auto status = ingress.tryAccept(wire);
            if (status.IsTooManyRequests()) {
                ++queue_full;
                response = "FULL";
                return;
            }
            if (!status.ok()) {
                ++invalid;
                response = "INVALID";
                return;
            }
            ++accepted;
            response = "OK";
        });
    auto status = server.start(port);
    if (!status.ok()) return 3;
    uint64_t applied = 0, duplicate = 0, gaps = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while ((accepted < expected_calls || inbox.size() != 0) &&
           std::chrono::steady_clock::now() < deadline) {
        std::vector<CreditControlEnvelope> batch;
        inbox.drain(batch, 31);
        for (auto& envelope : batch) {
            CreditUpdateDisposition disposition;
            status =
                ledger.applyUpdate(envelope.key, envelope.update, disposition);
            if (!status.ok()) return 4;
            if (disposition == CreditUpdateDisposition::DuplicateOrOld)
                ++duplicate;
            else if (disposition == CreditUpdateDisposition::SequenceGap)
                ++gaps;
            else
                ++applied;
        }
        if (drain_delay_us)
            std::this_thread::sleep_for(
                std::chrono::microseconds(drain_delay_us));
        std::this_thread::yield();
    }
    uint64_t available = 0;
    status = ledger.available(key(), CreditResource::DataBytes, available);
    std::cout << "accepted=" << accepted << " applied=" << applied
              << " duplicate=" << duplicate << " gaps=" << gaps
              << " queue_full=" << queue_full << " invalid=" << invalid
              << " epoch=" << epoch << " available=" << available << std::endl;
    return !status.ok() || accepted != expected_calls ? 5 : 0;
}

int runClient(const std::string& address, uint64_t first, uint64_t count,
              uint64_t epoch, bool expect_invalid) {
    CoroRpcAgent client;
    for (uint64_t sequence = first; sequence < first + count; ++sequence) {
        std::string wire, response;
        auto status =
            ReceiverCreditCodecV1::encode(update(sequence, epoch), wire);
        if (!status.ok()) return 2;
        do {
            status = client.call(address, kCreditTestRpc, wire, response);
            if (!status.ok()) return 3;
            if (response == "FULL") std::this_thread::yield();
        } while (response == "FULL");
        if (expect_invalid) {
            if (response != "INVALID") return 4;
            continue;
        }
        if (response != "OK") return 4;
    }
    return 0;
}

std::vector<uint16_t> parseVersions(const std::string& text) {
    std::vector<uint16_t> versions;
    std::stringstream stream(text);
    std::string item;
    while (std::getline(stream, item, ','))
        if (!item.empty()) versions.push_back(std::stoul(item));
    return versions;
}

int runCapabilityServer(uint16_t port, uint64_t expected_calls,
                        const std::vector<uint16_t>& advertised_versions) {
    std::string advertised_wire;
    if (!CreditCapabilityCodecV1::encode(advertised_versions, advertised_wire)
             .ok())
        return 2;
    std::atomic<uint64_t> calls{0};
    std::atomic<uint64_t> invalid{0};
    CoroRpcAgent server;
    server.registerFunction(
        kCapabilityTestRpc,
        [&](std::string_view request, std::string& response) {
            std::vector<uint16_t> offered;
            if (!CreditCapabilityCodecV1::decode(request, offered).ok()) {
                ++invalid;
                response = "INVALID";
                return;
            }
            ++calls;
            response = advertised_wire;
        });
    auto status = server.start(port);
    if (!status.ok()) return 3;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (calls < expected_calls && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    std::cout << "calls=" << calls << " invalid=" << invalid << std::endl;
    return calls == expected_calls && invalid == 0 ? 0 : 4;
}

int runCapabilityClient(const std::string& address, CreditRolloutMode mode,
                        CreditPeerState expected) {
    std::string request, response;
    if (!CreditCapabilityCodecV1::encode({1}, request).ok()) return 2;
    CoroRpcAgent client;
    auto status = client.call(address, kCapabilityTestRpc, request, response);
    if (!status.ok()) return 3;
    std::vector<uint16_t> peer_versions;
    if (!CreditCapabilityCodecV1::decode(response, peer_versions).ok()) return 4;
    CreditCapabilityState state(mode);
    status = state.completeNegotiation(peer_versions);
    if (expected == CreditPeerState::Failed) {
        if (!status.IsNotImplemented()) return 5;
    } else if (!status.ok()) {
        return 5;
    }
    std::cout << "state=" << static_cast<int>(state.state())
              << " version=" << state.version() << std::endl;
    return state.state() == expected ? 0 : 6;
}
}  // namespace
}  // namespace mooncake::tent

int main(int argc, char** argv) {
    using namespace mooncake::tent;
    if (argc >= 4 && argc <= 6 && std::string(argv[1]) == "server")
        return runServer(static_cast<uint16_t>(std::stoul(argv[2])),
                         std::stoull(argv[3]),
                         argc >= 5 ? std::stoull(argv[4]) : 0,
                         argc == 6 ? std::stoull(argv[5]) : 7);
    if (argc >= 5 && argc <= 7 && std::string(argv[1]) == "client")
        return runClient(argv[2], std::stoull(argv[3]), std::stoull(argv[4]),
                         argc >= 6 ? std::stoull(argv[5]) : 7,
                         argc == 7 && std::string(argv[6]) == "expect-invalid");
    if (argc == 5 && std::string(argv[1]) == "cap-server")
        return runCapabilityServer(static_cast<uint16_t>(std::stoul(argv[2])),
                                   std::stoull(argv[3]),
                                   parseVersions(argv[4]));
    if (argc == 5 && std::string(argv[1]) == "cap-client") {
        CreditRolloutMode mode = std::string(argv[3]) == "required"
                                     ? CreditRolloutMode::Required
                                     : CreditRolloutMode::Optional;
        CreditPeerState expected = CreditPeerState::Active;
        if (std::string(argv[4]) == "legacy")
            expected = CreditPeerState::Legacy;
        else if (std::string(argv[4]) == "failed")
            expected = CreditPeerState::Failed;
        return runCapabilityClient(argv[2], mode, expected);
    }
    std::cerr << "server <port> <calls> [delay_us] [epoch] | client <ip:port> "
                 "<first> <count> [epoch] [expect-invalid] | cap-server "
                 "<port> <calls> <versions> | cap-client <ip:port> "
                 "<optional|required> <active|legacy|failed>\n";
    return 1;
}
