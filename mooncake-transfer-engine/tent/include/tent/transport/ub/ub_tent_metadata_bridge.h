// Copyright 2025 KVCache.AI
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

#ifndef UB_TENT_METADATA_BRIDGE_H
#define UB_TENT_METADATA_BRIDGE_H

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

// Old-TE headers must come first so that common.h declares
// LOCAL_SEGMENT_ID as a static const BEFORE tent/common/types.h
// redefines it as a macro.
#include "transfer_metadata.h"

// TENT headers (tent/common/types.h defines #define LOCAL_SEGMENT_ID)
#include "tent/common/types.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/segment.h"

namespace mooncake {
namespace tent {

// UbTentMetadataBridge bridges the legacy mooncake::TransferMetadata interface
// (used by UbTransport / UbWorkerPool internally) to TENT's ControlService and
// SegmentManager.
//
// Key responsibilities:
//   - Remote segment lookups (getSegmentDescByID, getSegmentDescByName,
//     getSegmentID) are redirected to the TENT SegmentManager so that the
//     UbWorkerPool obtains the correct tseg / eid fields from TENT segment
//     descriptors.
//   - Local memory operations (addLocalMemoryBuffer, addLocalSegment, etc.)
//     flow through to the base-class in-memory cache so that
//     UbTransport::selectDevice() and UbContext::buildLocalBufferDesc() work
//     correctly.
//   - startHandshakeDaemon becomes a no-op: the UB handshake is handled by
//     TENT's BootstrapUb RPC (registered via ControlService).
//   - sendHandshake is redirected to ControlClient::bootstrapUb() so that
//     the URMA connection setup uses TENT's RPC channel.
class UbTentMetadataBridge : public mooncake::TransferMetadata {
   public:
    // conn_string is used to initialize the base-class local cache in P2P mode
    // (no external metadata store).  Pass "p2p" for TENT-integrated operation.
    explicit UbTentMetadataBridge(
        std::shared_ptr<tent::ControlService> control_service,
        const std::string& conn_string = "p2p");

    ~UbTentMetadataBridge() override = default;

    // Remote segment lookup: queries TENT SegmentManager and converts the
    // TENT SegmentDesc to the old-TE format (with tseg / eid fields).
    // LOCAL_SEGMENT_ID falls through to the base-class in-memory cache.
    std::shared_ptr<SegmentDesc> getSegmentDescByID(
        SegmentID segment_id, bool force_update = false) override;

    std::shared_ptr<SegmentDesc> getSegmentDescByName(
        const std::string& segment_name, bool force_update = false) override;

    // Maps a segment name to the corresponding TENT SegmentID (used as the
    // old-TE SegmentID for remote segments).
    SegmentID getSegmentID(const std::string& segment_name) override;

    // No-op: the UB handshake daemon is replaced by TENT's BootstrapUb RPC.
    // Stores the callback so that onBootstrapUb can dispatch to it.
    int startHandshakeDaemon(OnReceiveHandShake callback, uint16_t listen_port,
                             int sockfd) override;

    // Routes the UB handshake through TENT's ControlClient::bootstrapUb RPC
    // instead of the old-TE TCP handshake plugin.
    int sendHandshake(const std::string& peer_server_name,
                      const HandShakeDesc& local_desc,
                      HandShakeDesc& peer_desc) override;

    // Accessor for the stored handshake callback (invoked by onBootstrapUb).
    OnReceiveHandShake handshakeCallback() const { return ub_handshake_cb_; }

   private:
    // Converts a TENT SegmentDesc to the old-TE SegmentDesc format, extracting
    // device EIDs and buffer tseg handles from transport_attrs[UB].
    std::shared_ptr<SegmentDesc> convertFromTent(
        const tent::SegmentDesc* tent_seg) const;

    std::shared_ptr<tent::ControlService> control_service_;
    OnReceiveHandShake ub_handshake_cb_;

    // Cache: old-TE SegmentID (= TENT SegmentID for remotes) → converted desc.
    mutable std::mutex cache_mutex_;
    mutable std::unordered_map<SegmentID, std::shared_ptr<SegmentDesc>>
        remote_desc_cache_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // UB_TENT_METADATA_BRIDGE_H
