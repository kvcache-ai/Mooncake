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

// Hardware-independent selection contract for classic MultiTransport.
// No real transports are installed and no data transfers are submitted.
// The fixture uses inert Transport subclasses to compare selected pointer
// identities. These do not create GPU/TCP resources and are safe to destroy.
// Build this only under USE_MACA && ENABLE_MULTI_PROTOCOL.

#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "multi_transport.h"
#include "transfer_metadata.h"

#if !defined(USE_MACA) || !defined(ENABLE_MULTI_PROTOCOL)
#error "Requires USE_MACA and ENABLE_MULTI_PROTOCOL"
#endif

namespace mooncake {
// Friend declared by the a9983dcd MultiTransport header. This peer exists
// only in the test binary. No production visibility change is necessary.
class MultiTransportTestPeer {
   public:
    static void AddTransport(MultiTransport& multi, const std::string& name,
                             const std::shared_ptr<Transport>& transport) {
        multi.transport_map_[name] = transport;
    }

    static Status SelectAuto(MultiTransport& multi,
                             const Transport::TransferRequest& request,
                             Transport*& selected) {
        return multi.selectTransport(request, selected);
    }

    static Status SelectExplicit(MultiTransport& multi,
                                 const Transport::TransferRequest& request,
                                 Transport*& selected,
                                 std::string& preferred) {
        return multi.mp_selectTransport(request, selected, preferred);
    }
};
}  // namespace mooncake

namespace {

// A selection-only fake. The production router is real; no real transport is
// instantiated, installed or used for data movement. Unexpected calls fail
// loudly so the test cannot accidentally exercise an uninitialized backend.
class SelectionOnlyTransport final : public mooncake::Transport {
   public:
    explicit SelectionOnlyTransport(std::string name) : name_(std::move(name)) {}

    mooncake::Status submitTransfer(
        BatchID, const std::vector<TransferRequest>&) override {
        throw std::logic_error("SelectionOnlyTransport: unexpected submitTransfer");
    }

    mooncake::Status getTransferStatus(
        BatchID, size_t, TransferStatus&) override {
        throw std::logic_error("SelectionOnlyTransport: unexpected getTransferStatus");
    }

   private:
    int registerLocalMemory(void*, size_t, const std::string&, bool,
                            bool) override {
        throw std::logic_error("SelectionOnlyTransport: unexpected registration");
    }

    int unregisterLocalMemory(void*, bool) override {
        throw std::logic_error("SelectionOnlyTransport: unexpected unregistration");
    }

    int registerLocalMemoryBatch(const std::vector<BufferEntry>&,
                                 const std::string&) override {
        throw std::logic_error("SelectionOnlyTransport: unexpected batch registration");
    }

    int unregisterLocalMemoryBatch(const std::vector<void*>&) override {
        throw std::logic_error("SelectionOnlyTransport: unexpected batch unregistration");
    }

    const char* getName() const override { return name_.c_str(); }

    std::string name_;
};

constexpr uint64_t kAddr = 0x20000000;
constexpr size_t kLength = 1ULL << 20;
const std::string kLocal = "127.0.0.1:29990";
const std::string kSameHost = "127.0.0.1:29991";
const std::string kOtherHost = "203.0.113.42:29991";

void Require(bool condition, const std::string& detail) {
    if (!condition) throw std::runtime_error(detail);
}

void Exercise(const char* label, const std::string& peer, bool reverse_order,
              const std::string& initial_preference,
              const std::string& expected_automatic,
              const std::string& expected_explicit,
              bool install_tcp = true) {
    auto metadata = std::make_shared<mooncake::TransferMetadata>(P2PHANDSHAKE);
    auto desc = std::make_shared<mooncake::TransferMetadata::SegmentDesc>();
    desc->name = peer;
    desc->protocol = "maca,tcp";
    const std::vector<std::string> order =
        reverse_order ? std::vector<std::string>{"tcp", "maca"}
                      : std::vector<std::string>{"maca", "tcp"};
    for (const auto& proto : order) {
        mooncake::TransferMetadata::BufferDesc buffer{};
        buffer.protocol = proto;
        buffer.name = "cuda:0";
        buffer.addr = kAddr;
        buffer.length = kLength;
        if (proto == "maca") buffer.shm_name = "synthetic-handle-only";
        desc->buffers.push_back(std::move(buffer));
    }
    // LOCAL_SEGMENT_ID keeps the metadata fixture in memory. The segment
    // descriptor name is intentionally different for same/cross host cases.
    metadata->addLocalSegment(mooncake::LOCAL_SEGMENT_ID, peer, std::move(desc));
    std::string local_name = kLocal;
    mooncake::MultiTransport router(metadata, local_name);

    // Compare real router decisions against inert, lifetime-safe fakes.
    auto maca = std::make_shared<SelectionOnlyTransport>("maca");
    auto tcp = std::make_shared<SelectionOnlyTransport>("tcp");
    mooncake::MultiTransportTestPeer::AddTransport(router, "maca", maca);
    if (install_tcp) {
        mooncake::MultiTransportTestPeer::AddTransport(router, "tcp", tcp);
    }

    mooncake::Transport::TransferRequest request{};
    request.opcode = mooncake::Transport::TransferRequest::READ;
    request.source = nullptr;
    request.target_id = mooncake::LOCAL_SEGMENT_ID;
    request.target_offset = kAddr + 4096;
    request.length = 4096;

    auto expected_ptr = [&](const std::string& protocol) -> mooncake::Transport* {
        if (protocol == "maca") return maca.get();
        if (protocol == "tcp" && install_tcp) return tcp.get();
        return nullptr;
    };

    mooncake::Transport* selected = nullptr;
    const auto auto_status = mooncake::MultiTransportTestPeer::SelectAuto(
        router, request, selected);
    Require(auto_status.ok() == (expected_ptr(expected_automatic) != nullptr),
            std::string(label) + " automatic path status mismatch: " +
                auto_status.ToString());
    Require(selected == expected_ptr(expected_automatic),
            std::string(label) + " automatic path chose wrong pointer; status=" +
                auto_status.ToString());

    selected = nullptr;
    std::string preferred = initial_preference;
    const auto explicit_status = mooncake::MultiTransportTestPeer::SelectExplicit(
        router, request, selected, preferred);
    Require(preferred == expected_explicit,
            std::string(label) + " explicit preference mismatch, got=" +
                preferred + " status=" + explicit_status.ToString());
    Require(explicit_status.ok() == (expected_ptr(expected_explicit) != nullptr),
            std::string(label) + " explicit path status mismatch: " +
                explicit_status.ToString());
    Require(selected == expected_ptr(expected_explicit),
            std::string(label) + " explicit path chose wrong pointer; status=" +
                explicit_status.ToString());
    std::cout << "PASS " << label << " auto=" << expected_automatic
              << " explicit=" << preferred << "\n";
}

}  // namespace

int main() {
    try {
        // Cross-host candidates: GPU IPC is invalid, TCP must be selected.
        Exercise("remote_maca_tcp", kOtherHost, false, "maca", "tcp", "tcp");
        Exercise("remote_order_reversed", kOtherHost, true, "maca", "tcp", "tcp");
        // Same-host candidates: keep MACA IPC (and honor explicit TCP).
        Exercise("local_maca_tcp", kSameHost, false, "maca", "maca", "maca");
        Exercise("local_order_reversed", kSameHost, true, "maca", "maca", "maca");
        Exercise("local_explicit_tcp", kSameHost, false, "tcp", "maca", "tcp");
        Exercise("remote_explicit_tcp", kOtherHost, false, "tcp", "tcp", "tcp");
        // No installed fallback: never take the cross-host MACA path.
        Exercise("remote_no_tcp_installed", kOtherHost, false, "maca", "none", "tcp", false);
    } catch (const std::exception& error) {
        std::cerr << "FAIL " << error.what() << '\n';
        return 1;
    }
    std::cout << "ALL MACA IPC LOCALITY CASES PASS\n";
    return 0;
}
