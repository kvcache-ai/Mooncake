// Copyright 2024 KVCache.AI
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

#include "show_links.h"

#include <json/json.h>

#include <sstream>

#include "transfer_engine_impl.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_transport.h"

namespace mooncake {

static std::vector<NicDiagInfo> collectLocalNics(TransferEngineImpl* impl) {
    std::vector<NicDiagInfo> nics;
    auto* transport = impl->getTransport("rdma");
    if (!transport) {
        auto topo = impl->getLocalTopology();
        if (!topo) return nics;

        for (const auto& hca : topo->getHcaList()) {
            NicDiagInfo info;
            info.device_name = hca;
            info.numa_node = -1;
            info.gid_index = -1;
            info.port = 0;
            info.active_speed = 0;
            info.active_width = 0;
            nics.push_back(info);
        }
        return nics;
    }

    auto* rdma = static_cast<RdmaTransport*>(transport);
    for (auto& ctx : rdma->getContextList()) {
        NicDiagInfo info;
        info.device_name = ctx->deviceName();
        info.numa_node = ctx->socketId();
        info.gid = ctx->gid();
        info.gid_index = ctx->gidIndex();
        info.port = ctx->portNum();
        info.active_speed = ctx->activeSpeed();
        info.active_width = ctx->activeWidth();
        nics.push_back(info);
    }
    return nics;
}

static bool hasTransportDetails(const NicDiagInfo& nic) {
    return nic.gid_index >= 0;
}

static int computeSpeedGbps(int active_speed, int active_width) {
    int lane_gbps = 0;
    switch (active_speed) {
        case 1:
            lane_gbps = 2;
            break;
        case 2:
            lane_gbps = 5;
            break;
        case 4:
            lane_gbps = 10;
            break;
        case 8:
            lane_gbps = 10;
            break;
        case 16:
            lane_gbps = 14;
            break;
        case 32:
            lane_gbps = 25;
            break;
        case 64:
            lane_gbps = 50;
            break;
        case 128:
            lane_gbps = 100;
            break;
        case 256:
            lane_gbps = 200;
            break;
        default:
            lane_gbps = active_speed;
            break;
    }
    int lanes = 1;
    switch (active_width) {
        case 1:
            lanes = 1;
            break;
        case 2:
            lanes = 4;
            break;
        case 4:
            lanes = 8;
            break;
        case 8:
            lanes = 12;
            break;
        case 16:
            lanes = 2;
            break;
        default:
            lanes = 1;
            break;
    }
    return lane_gbps * lanes;
}

std::string buildShowLinksJson(TransferEngineImpl* impl) {
    Json::Value root;

    auto nics = collectLocalNics(impl);
    Json::Value nic_arr(Json::arrayValue);
    for (auto& nic : nics) {
        Json::Value n;
        n["device"] = nic.device_name;
        n["numa"] = nic.numa_node;
        n["gid"] = nic.gid;
        n["gid_index"] = nic.gid_index;
        n["port"] = nic.port;
        n["speed_gbps"] = computeSpeedGbps(nic.active_speed, nic.active_width);
        n["source"] = hasTransportDetails(nic) ? "rdma_transport" : "topology";
        nic_arr.append(n);
    }
    root["local_nics"] = nic_arr;

    auto topo = impl->getLocalTopology();
    if (topo) {
        root["topology"] = topo->toJson();
    }

    Json::StreamWriterBuilder builder;
    builder["indentation"] = "  ";
    return Json::writeString(builder, root);
}

std::string buildShowLinksReadable(TransferEngineImpl* impl) {
    std::ostringstream os;

    auto nics = collectLocalNics(impl);
    os << "=== Local NICs ===\n";
    if (nics.empty()) {
        os << "  (no RDMA devices found)\n";
    }
    for (auto& nic : nics) {
        os << "  " << nic.device_name;
        if (!hasTransportDetails(nic)) {
            os << "  (topology only; RDMA transport not initialized)\n";
            continue;
        }
        os << "  NUMA=" << nic.numa_node << "  GID=" << nic.gid
           << " (idx=" << nic.gid_index << ")"
           << "  Port=" << (int)nic.port << "  "
           << computeSpeedGbps(nic.active_speed, nic.active_width) << "Gbps\n";
    }

    auto topo = impl->getLocalTopology();
    if (topo && !topo->empty()) {
        os << "\n=== Topology (NIC Selection) ===\n";
        auto matrix = topo->getMatrix();
        for (auto& [location, entry] : matrix) {
            os << "  " << location << " -> preferred: [";
            for (size_t i = 0; i < entry.preferred_hca.size(); i++) {
                if (i > 0) os << ", ";
                os << entry.preferred_hca[i];
            }
            os << "]  available: [";
            for (size_t i = 0; i < entry.avail_hca.size(); i++) {
                if (i > 0) os << ", ";
                os << entry.avail_hca[i];
            }
            os << "]\n";
        }
    }

    return os.str();
}

}  // namespace mooncake
