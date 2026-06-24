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

#include "tent/transport/ub/ub_tent_metadata_bridge.h"

#include <glog/logging.h>

#include "tent/runtime/segment_manager.h"
#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {

using json = nlohmann::json;

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

UbTentMetadataBridge::UbTentMetadataBridge(
    std::shared_ptr<tent::ControlService> control_service,
    const std::string& conn_string)
    : mooncake::TransferMetadata(conn_string),
      control_service_(std::move(control_service)) {}

// ---------------------------------------------------------------------------
// Remote segment conversion
// ---------------------------------------------------------------------------

std::shared_ptr<TransferMetadata::SegmentDesc>
UbTentMetadataBridge::convertFromTent(const tent::SegmentDesc* tent_seg) const {
    if (!tent_seg) return nullptr;

    if (tent_seg->type != tent::SegmentType::Memory) {
        LOG(WARNING)
            << "UbTentMetadataBridge: segment '" << tent_seg->name
            << "' is not a memory segment; cannot extract UB attributes";
        return nullptr;
    }

    auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
    desc->name = tent_seg->name;
    desc->protocol = "ub";

    const auto& mem = std::get<tent::MemorySegmentDesc>(tent_seg->detail);

    // Extract per-device EIDs stored in transport_attrs[UB].
    for (const auto& dev : mem.devices) {
        TransferMetadata::DeviceDesc d;
        d.name = dev.name;
        auto it = dev.transport_attrs.find(TransportType::UB);
        if (it != dev.transport_attrs.end()) {
            d.eid = it->second;
        }
        desc->devices.push_back(d);
    }

    // Extract per-buffer tseg handles stored in transport_attrs[UB] as a
    // JSON array of strings.
    for (const auto& buf : mem.buffers) {
        TransferMetadata::BufferDesc b;
        b.addr = buf.addr;
        b.length = buf.length;
        auto it = buf.transport_attrs.find(TransportType::UB);
        if (it != buf.transport_attrs.end()) {
            try {
                auto j = json::parse(it->second);
                for (auto& t : j) b.tseg.push_back(t.get<std::string>());
            } catch (const std::exception& e) {
                LOG(WARNING) << "UbTentMetadataBridge: failed to parse tseg "
                                "for buffer "
                             << buf.addr << ": " << e.what();
            }
        }
        desc->buffers.push_back(std::move(b));
    }

    return desc;
}

// ---------------------------------------------------------------------------
// Remote segment lookup
// ---------------------------------------------------------------------------

std::shared_ptr<TransferMetadata::SegmentDesc>
UbTentMetadataBridge::getSegmentDescByID(SegmentID segment_id,
                                         bool force_update) {
    // Local segment: served by the base-class in-memory cache.
    if (segment_id == LOCAL_SEGMENT_ID) {
        return TransferMetadata::getSegmentDescByID(segment_id, force_update);
    }

    if (!control_service_) {
        LOG(ERROR) << "UbTentMetadataBridge: no ControlService for ID lookup "
                   << segment_id;
        return nullptr;
    }

    if (!force_update) {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        auto it = remote_desc_cache_.find(segment_id);
        if (it != remote_desc_cache_.end()) return it->second;
    }

    tent::SegmentDesc* tent_desc = nullptr;
    auto status = control_service_->segmentManager().getRemoteCached(
        tent_desc, segment_id);
    if (!status.ok() || !tent_desc) {
        LOG(WARNING) << "UbTentMetadataBridge: TENT segment ID " << segment_id
                     << " not found: " << status.message();
        return nullptr;
    }

    auto converted = convertFromTent(tent_desc);
    if (converted) {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        remote_desc_cache_[segment_id] = converted;
    }
    return converted;
}

std::shared_ptr<TransferMetadata::SegmentDesc>
UbTentMetadataBridge::getSegmentDescByName(const std::string& segment_name,
                                           bool force_update) {
    if (!control_service_) {
        return TransferMetadata::getSegmentDescByName(segment_name,
                                                      force_update);
    }

    tent::SegmentDescRef tent_desc_ref;
    auto status = control_service_->segmentManager().getRemote(tent_desc_ref,
                                                               segment_name);
    if (!status.ok() || !tent_desc_ref) {
        LOG(WARNING) << "UbTentMetadataBridge: segment '" << segment_name
                     << "' not found in TENT: " << status.message();
        return nullptr;
    }

    return convertFromTent(tent_desc_ref.get());
}

// ---------------------------------------------------------------------------
// Segment ID lookup
// ---------------------------------------------------------------------------

TransferMetadata::SegmentID UbTentMetadataBridge::getSegmentID(
    const std::string& segment_name) {
    if (!control_service_) {
        return TransferMetadata::getSegmentID(segment_name);
    }

    SegmentID handle = 0;
    auto status =
        control_service_->segmentManager().openRemote(handle, segment_name);
    if (!status.ok()) {
        LOG(ERROR) << "UbTentMetadataBridge: cannot open remote segment '"
                   << segment_name << "': " << status.message();
        return static_cast<TransferMetadata::SegmentID>(-1);
    }
    return static_cast<TransferMetadata::SegmentID>(handle);
}

// ---------------------------------------------------------------------------
// Handshake daemon: no-op (replaced by TENT BootstrapUb RPC)
// ---------------------------------------------------------------------------

int UbTentMetadataBridge::startHandshakeDaemon(OnReceiveHandShake callback,
                                               uint16_t /*listen_port*/,
                                               int /*sockfd*/) {
    // Store the callback so that the ControlService onBootstrapUb handler can
    // dispatch incoming UB bootstrap requests to it.
    ub_handshake_cb_ = std::move(callback);
    return 0;
}

// ---------------------------------------------------------------------------
// sendHandshake: routes through TENT BootstrapUb RPC
// ---------------------------------------------------------------------------

int UbTentMetadataBridge::sendHandshake(const std::string& peer_server_name,
                                        const HandShakeDesc& local_desc,
                                        HandShakeDesc& peer_desc) {
    if (!control_service_) {
        return TransferMetadata::sendHandshake(peer_server_name, local_desc,
                                               peer_desc);
    }

    // Resolve the TENT RPC address for this peer.  The peer_server_name
    // matches the TENT segment name (= the remote local_segment_name_).
    std::string rpc_addr;
    {
        tent::SegmentDescRef tent_desc_ref;
        auto status = control_service_->segmentManager().getRemote(
            tent_desc_ref, peer_server_name);
        if (!status.ok() || !tent_desc_ref) {
            LOG(ERROR) << "UbTentMetadataBridge: cannot resolve peer '"
                       << peer_server_name
                       << "' to a TENT segment: " << status.message();
            return -1;
        }
        rpc_addr = tent_desc_ref->rpc_server_addr;
    }

    UbBootstrapDesc request;
    request.local_nic_path = local_desc.local_nic_path;
    request.peer_nic_path = local_desc.peer_nic_path;
#ifdef USE_UB
    request.jetty_num = local_desc.jetty_num;
#endif

    UbBootstrapDesc response;
    auto status = ControlClient::bootstrapUb(rpc_addr, request, response);
    if (!status.ok()) {
        LOG(ERROR) << "UbTentMetadataBridge: BootstrapUb RPC to " << rpc_addr
                   << " failed: " << status.message();
        return -1;
    }

    peer_desc.local_nic_path = response.local_nic_path;
    peer_desc.peer_nic_path = response.peer_nic_path;
    peer_desc.reply_msg = response.reply_msg;
#ifdef USE_UB
    peer_desc.jetty_num = response.jetty_num;
#endif

    return 0;
}

}  // namespace tent
}  // namespace mooncake
