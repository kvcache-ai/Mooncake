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

#include "tent/runtime/transfer_engine_impl.h"
#include "tent/transport/shm/shm_transport.h"
#include "tent/transport/tcp/tcp_transport.h"

#ifdef USE_RDMA
#include "tent/transport/rdma/rdma_transport.h"
#endif

#ifdef USE_CUDA
#include "tent/transport/nvlink/nvlink_transport.h"
#include "tent/transport/mnnvl/mnnvl_transport.h"
#endif

#ifdef USE_GDS
#include "tent/transport/gds/gds_transport.h"
#endif

#ifdef USE_URING
#include "tent/transport/io_uring/io_uring_transport.h"
#endif

#if defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT)
#include "tent/transport/ascend/ascend_direct_transport.h"
#endif

namespace mooncake {
namespace tent {

Status TransferEngineImpl::loadTransports() {
    if (conf_->get("transports/tcp/enable", true))
        transport_list_[TCP] = std::make_shared<TcpTransport>();

    // TODO affect the end-to-end performance because it is not numa aware
    if (conf_->get("transports/shm/enable", false))
        transport_list_[SHM] = std::make_shared<ShmTransport>();

#ifdef USE_RDMA
    if (conf_->get("transports/rdma/enable", true) &&
        topology_->getNicCount(Topology::NIC_RDMA)) {
        transport_list_[RDMA] = std::make_shared<RdmaTransport>();
    }
#endif

#ifdef USE_URING
    if (conf_->get("transports/io_uring/enable", true))
        transport_list_[IOURING] = std::make_shared<IOUringTransport>();
#endif

#ifdef USE_CUDA
    bool enable_mnnvl = getenv("MC_ENABLE_MNNVL") != nullptr;
    if (enable_mnnvl) {
        if (conf_->get("transports/mnnvl/enable", true))
            transport_list_[MNNVL] = std::make_shared<MnnvlTransport>();
    } else {
        if (conf_->get("transports/nvlink/enable", true))
            transport_list_[NVLINK] = std::make_shared<NVLinkTransport>();
    }
#endif

#ifdef USE_GDS
    if (conf_->get("transports/gds/enable", false))
        transport_list_[GDS] = std::make_shared<GdsTransport>();
#endif

#if defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT)
    if (conf_->get("transports/ascend_direct/enable", true)) {
        transport_list_[AscendDirect] =
            std::make_shared<AscendDirectTransport>();
    }
#endif

    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
