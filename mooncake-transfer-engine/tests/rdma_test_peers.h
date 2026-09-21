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

// Shared test-only friend accessors for exercising RdmaTransport/RdmaContext
// without a real RDMA device. Include this instead of redeclaring
// RdmaTransportTestPeer/RdmaContextTestPeer in individual test files.

#ifndef MOONCAKE_TESTS_RDMA_TEST_PEERS_H
#define MOONCAKE_TESTS_RDMA_TEST_PEERS_H

#include <memory>
#include <string>

#include "transfer_metadata.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_transport.h"

namespace mooncake {

class RdmaTransportTestPeer {
   public:
    static void bindMetadata(RdmaTransport &transport,
                             std::shared_ptr<TransferMetadata> metadata,
                             std::string local_server_name) {
        transport.metadata_ = std::move(metadata);
        transport.local_server_name_ = std::move(local_server_name);
    }

    // Registers a (possibly bare, unconstructed) RdmaContext as one of the
    // transport's local devices, bypassing initializeRdmaResources().
    static void addContext(RdmaTransport &transport,
                           std::shared_ptr<RdmaContext> context) {
        transport.context_list_.push_back(std::move(context));
    }

    static void bindTopology(RdmaTransport &transport,
                             std::shared_ptr<Topology> topology) {
        transport.local_topology_ = std::move(topology);
    }

    // Drives the device-initialization loop directly so tests can assert on
    // the resulting context_list_ layout without a full install().
    static int initializeResources(RdmaTransport &transport) {
        return transport.initializeRdmaResources();
    }
};

class RdmaContextTestPeer {
   public:
    static bool hasEndpointStore(const RdmaContext &context) {
        return context.endpoint_store_ != nullptr;
    }

    static void seedAutoGidState(RdmaContext &context, ibv_context *verbs_ctx,
                                 uint8_t port, uint32_t lid, const ibv_gid &gid,
                                 int gid_index) {
        context.context_ = verbs_ctx;
        context.port_ = port;
        context.lid_ = lid;
        context.gid_ = gid;
        context.gid_index_ = gid_index;
        context.auto_gid_selection_enabled_ = true;
    }

    static void disableContextForTeardown(RdmaContext &context) {
        context.context_ = nullptr;
    }
};

}  // namespace mooncake

#endif  // MOONCAKE_TESTS_RDMA_TEST_PEERS_H
