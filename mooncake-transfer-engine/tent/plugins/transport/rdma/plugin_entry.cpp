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

// The built-in RDMA transport as a plugin (TENT_XPORT_RDMA=plugin or both). It
// claims the "rdma" slot and, like the built-in, is enabled by default; a core
// that already has RDMA rejects it as a conflict.

#ifndef TENT_TRANSPORT_PLUGIN_VERSION
#define TENT_TRANSPORT_PLUGIN_VERSION "1"
#endif

#include <infiniband/verbs.h>

#include <stdexcept>

#include "tent/runtime/topology.h"
#include "tent/transport/rdma/rdma_transport.h"
#include "tent/transport_plugin.hpp"

namespace {

bool hasActivePort(ibv_device* device) {
    ibv_context* context = ibv_open_device(device);
    if (!context) return false;
    bool active = false;
    ibv_device_attr device_attr{};
    if (ibv_query_device(context, &device_attr) == 0) {
        for (uint8_t port = 1; port <= device_attr.phys_port_cnt && !active;
             ++port) {
            ibv_port_attr port_attr{};
            active = ibv_query_port(context, port, &port_attr) == 0 &&
                     port_attr.gid_tbl_len > 0 &&
                     port_attr.state == IBV_PORT_ACTIVE;
        }
    }
    ibv_close_device(context);
    return active;
}

// probe() runs once per process, so it asks whether this host has an RDMA
// device with an active port at all, the devices topology discovery keeps;
// which NICs an engine may use depends on that engine's configuration and is
// checked in create().
int rdmaProbe(const tent_transport_plugin_host_t*) {
    int count = 0;
    ibv_device** devices = ibv_get_device_list(&count);
    bool active = false;
    for (int i = 0; i < count && !active; ++i)
        active = hasActivePort(devices[i]);
    if (devices) ibv_free_device_list(devices);
    if (active) return 0;
    mooncake::tent::transport_plugin_detail::setLastError(
        "no RDMA device with an active port on this host");
    return 1;
}

// Like the built-in loader, an engine gets RDMA only when its topology has an
// RDMA NIC; otherwise this engine reports rejected_create_failed. A NULL
// topology does not rule it out.
mooncake::tent::Transport* rdmaCreate(
    const tent_transport_plugin_host_t* host) {
    const auto* topology =
        static_cast<const mooncake::tent::Topology*>(host->topology);
    if (topology && !topology->getNicCount(mooncake::tent::Topology::NIC_RDMA))
        throw std::runtime_error("no RDMA NIC in this engine's topology");
    return new mooncake::tent::RdmaTransport();
}

}  // namespace

// RdmaTransport reports notification and cancellation support and, when GPU
// Direct RDMA is available, the GPU capabilities as well.
TENT_DEFINE_TRANSPORT_PLUGIN_EX(
    rdma, TENT_TRANSPORT_PLUGIN_VERSION, mooncake::tent::RDMA,
    TENT_XPORT_FLAG_ENABLED_BY_DEFAULT | TENT_XPORT_FLAG_NOTIFICATION |
        TENT_XPORT_FLAG_CANCELLATION,
    TENT_XPORT_CAP_DRAM_TO_DRAM | TENT_XPORT_CAP_DRAM_TO_GPU |
        TENT_XPORT_CAP_GPU_TO_DRAM | TENT_XPORT_CAP_GPU_TO_GPU,
    rdmaProbe, rdmaCreate(host))
