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

#include "v1/runtime/transfer_engine_impl.h"

#include "v1/transport/shm/shm_transport.h"
#include "v1/transport/tcp/tcp_transport.h"

#ifdef USE_RDMA
#include "v1/transport/rdma/rdma_transport.h"
#endif

#ifdef USE_CUDA
#include "v1/transport/nvlink/nvlink_transport.h"
#include "v1/transport/mnnvl/mnnvl_transport.h"
#endif

#ifdef USE_GDS
#include "v1/transport/gds/gds_transport.h"
#endif

#ifdef USE_URING
#include "v1/transport/io_uring/io_uring_transport.h"
#endif

namespace mooncake {
namespace v1 {

Status TransferEngineImpl::loadTransports() {
    if (conf_->get("transports/tcp/enable", true))
        transport_list_[TCP] = std::make_unique<TcpTransport>();

    if (conf_->get("transports/shm/enable", true))
        transport_list_[SHM] = std::make_unique<ShmTransport>();

#ifdef USE_RDMA
    if (conf_->get("transports/rdma/enable", true) &&
        topology_->getNicCount(Topology::NIC_RDMA)) {
        transport_list_[RDMA] = std::make_unique<RdmaTransport>();
    }
#endif

#ifdef USE_URING
    if (conf_->get("transports/io_uring/enable", true))
        transport_list_[IOURING] = std::make_unique<IOUringTransport>();
#endif

#ifdef USE_CUDA
    if (conf_->get("transports/nvlink/enable", true))
        transport_list_[NVLINK] = std::make_unique<NVLinkTransport>();

    if (conf_->get("transports/mnnvl/enable", true))
        transport_list_[MNNVL] = std::make_unique<MnnvlTransport>();
#endif

#ifdef USE_GDS
    if (conf_->get("transports/gds/enable", true))
        transport_list_[GDS] = std::make_unique<GdsTransport>();
#endif

    return Status::OK();
}

}  // namespace v1
}  // namespace mooncake