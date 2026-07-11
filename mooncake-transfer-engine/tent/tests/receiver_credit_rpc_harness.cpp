// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/rpc/rpc.h"
#include "tent/runtime/receiver_credit_control.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

namespace mooncake::tent {
namespace {
constexpr int kCreditTestRpc = 1001;
CreditKey key() { return {{1, 2}, 3, 4}; }
ReceiverCreditUpdateV1 update(uint64_t sequence) {
    ReceiverCreditUpdateV1 u;
    u.receiver_session_id = key().receiver_session;
    u.qos_class = key().qos_class;
    u.epoch = 7;
    u.sequence = sequence;
    u.grants = {{CreditResource::DataBytes, sequence}};
    return u;
}

int runServer(uint16_t port, uint64_t expected_calls) {
    BoundedCreditUpdateInbox inbox(64);
    SenderCreditLedger ledger;
    if (!ledger.activate(key(), 7).ok()) return 2;
    std::atomic<uint64_t> accepted{0};
    CoroRpcAgent server;
    server.registerFunction(
        kCreditTestRpc, [&](std::string_view wire, std::string& response) {
            ReceiverCreditUpdateV1 decoded;
            auto status = ReceiverCreditCodecV1::decode(wire, decoded);
            if (!status.ok()) {
                response = "INVALID";
                return;
            }
            status = inbox.tryPublish({key(), decoded});
            if (!status.ok()) {
                response = "FULL";
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
        std::this_thread::yield();
    }
    uint64_t available = 0;
    status = ledger.available(key(), CreditResource::DataBytes, available);
    std::cout << "accepted=" << accepted << " applied=" << applied
              << " duplicate=" << duplicate << " gaps=" << gaps
              << " available=" << available << std::endl;
    return !status.ok() || accepted != expected_calls ? 5 : 0;
}

int runClient(const std::string& address, uint64_t first, uint64_t count) {
    CoroRpcAgent client;
    for (uint64_t sequence = first; sequence < first + count; ++sequence) {
        std::string wire, response;
        auto status = ReceiverCreditCodecV1::encode(update(sequence), wire);
        if (!status.ok()) return 2;
        do {
            status = client.call(address, kCreditTestRpc, wire, response);
            if (!status.ok()) return 3;
            if (response == "FULL") std::this_thread::yield();
        } while (response == "FULL");
        if (response != "OK") return 4;
    }
    return 0;
}
}  // namespace
}  // namespace mooncake::tent

int main(int argc, char** argv) {
    using namespace mooncake::tent;
    if (argc == 4 && std::string(argv[1]) == "server")
        return runServer(static_cast<uint16_t>(std::stoul(argv[2])),
                         std::stoull(argv[3]));
    if (argc == 5 && std::string(argv[1]) == "client")
        return runClient(argv[2], std::stoull(argv[3]), std::stoull(argv[4]));
    std::cerr << "server <port> <calls> | client <ip:port> <first> <count>\n";
    return 1;
}
