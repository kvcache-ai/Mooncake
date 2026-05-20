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

#include "tent/transport/ibgda/ibgda_transport.h"

#include <cuda_runtime.h>

#include <sstream>

#include "tent/common/status.h"

namespace mooncake {
namespace tent {

namespace {
Status notImplemented(const char* method) {
    std::stringstream ss;
    ss << "IbGdaTransport::" << method
       << " is not implemented in Stage C scaffold" << LOC_MARK;
    return Status::NotImplemented(ss.str());
}
}  // namespace

IbGdaTransport::IbGdaTransport() {
    caps.gpu_to_gpu = true;
}

IbGdaTransport::~IbGdaTransport() { (void)uninstall(); }

Status IbGdaTransport::install(std::string& local_segment_name,
                               std::shared_ptr<ControlService> metadata,
                               std::shared_ptr<Topology> local_topology,
                               std::shared_ptr<Config> conf) {
    local_segment_name_ = local_segment_name;
    metadata_ = std::move(metadata);
    local_topology_ = std::move(local_topology);
    conf_ = std::move(conf);
    installed_ = true;
    return Status::OK();
}

Status IbGdaTransport::uninstall() {
    installed_ = false;
    network_ctx_ = nullptr;
    num_channels_ = 0;
    peer_info_.clear();
    return Status::OK();
}

Status IbGdaTransport::allocateSubBatch(SubBatchRef&, size_t) {
    return notImplemented("allocateSubBatch");
}

Status IbGdaTransport::freeSubBatch(SubBatchRef&) {
    return notImplemented("freeSubBatch");
}

Status IbGdaTransport::submitTransferTasks(
    SubBatchRef, const std::vector<Request>&) {
    return notImplemented("submitTransferTasks");
}

Status IbGdaTransport::getTransferStatus(SubBatchRef, int,
                                         TransferStatus& status) {
    status = {TransferStatusEnum::FAILED, 0};
    return notImplemented("getTransferStatus");
}

Status IbGdaTransport::addMemoryBuffer(BufferDesc&, const MemoryOptions&) {
    return notImplemented("addMemoryBuffer");
}

Status IbGdaTransport::removeMemoryBuffer(BufferDesc&) {
    return notImplemented("removeMemoryBuffer");
}

Status IbGdaTransport::symAlloc(void** ptr, size_t size) {
    if (!ptr) return Status::InvalidArgument("ptr is null" LOC_MARK);
    auto err = cudaMalloc(ptr, size);
    if (err != cudaSuccess) {
        return Status::CudaError(std::string("cudaMalloc failed: ") +
                                 cudaGetErrorString(err) + LOC_MARK);
    }
    return Status::OK();
}

Status IbGdaTransport::symFree(void* ptr) {
    if (!ptr) return Status::OK();
    auto err = cudaFree(ptr);
    if (err != cudaSuccess) {
        return Status::CudaError(std::string("cudaFree failed: ") +
                                 cudaGetErrorString(err) + LOC_MARK);
    }
    return Status::OK();
}

void* IbGdaTransport::getRemotePtr(void* local_ptr, int) { return local_ptr; }

Status IbGdaTransport::registerMemory(void*, size_t, uint32_t&, uint32_t&) {
    return notImplemented("registerMemory");
}

Status IbGdaTransport::unregisterMemory(void*) {
    return notImplemented("unregisterMemory");
}

Status IbGdaTransport::getChannelResources(
    int channel_id, DeviceChannelResources& resources) {
    if (channel_id < 0 || channel_id >= num_channels_) {
        return Status::InvalidArgument("invalid IBGDA channel id" LOC_MARK);
    }
    if (!network_ctx_) {
        return Status::InvalidArgument(
            "IBGDA device context has not been adopted" LOC_MARK);
    }
    resources.channel_id = channel_id;
    resources.num_channels = num_channels_;
    resources.network_ctx = network_ctx_;
    return Status::OK();
}

Status IbGdaTransport::getPeerInfo(int rank, DevicePeerInfo& peer) {
    auto it = peer_info_.find(rank);
    if (it == peer_info_.end()) {
        return Status::InvalidArgument("IBGDA peer info not found" LOC_MARK);
    }
    peer = it->second;
    return Status::OK();
}

Status IbGdaTransport::connect(int) { return notImplemented("connect"); }

Status IbGdaTransport::barrier() { return notImplemented("barrier"); }

const DeviceCommCapabilities IbGdaTransport::deviceCapabilities() const {
    DeviceCommCapabilities caps;
    caps.gpu_initiated_rdma = true;
    caps.symmetric_memory = true;
    caps.signal = true;
    caps.atomic = true;
    caps.latency_tier_ns = 1000;
    return caps;
}

Status IbGdaTransport::adoptDeviceContext(void* network_ctx,
                                          int num_channels) {
    if (!network_ctx) {
        return Status::InvalidArgument("network_ctx is null" LOC_MARK);
    }
    if (num_channels <= 0) {
        return Status::InvalidArgument("num_channels must be positive" LOC_MARK);
    }
    network_ctx_ = network_ctx;
    num_channels_ = num_channels;
    return Status::OK();
}

Status IbGdaTransport::adoptDeviceContext(IbGdaDeviceContext* network_ctx,
                                          int num_channels) {
    if (!network_ctx) {
        return Status::InvalidArgument("network_ctx is null" LOC_MARK);
    }
    if (network_ctx->abi_version != kIbGdaDeviceContextAbiVersion) {
        return Status::InvalidArgument("unsupported IBGDA device context ABI" LOC_MARK);
    }
    if (!network_ctx->raddrs || !network_ctx->rkeys || !network_ctx->qp_devctxs) {
        return Status::InvalidArgument("incomplete IBGDA device context" LOC_MARK);
    }
    if (network_ctx->num_qps <= 0 || network_ctx->num_ranks <= 0) {
        return Status::InvalidArgument("invalid IBGDA device context shape" LOC_MARK);
    }
    return adoptDeviceContext(static_cast<void*>(network_ctx), num_channels);
}

Status IbGdaTransport::setPeerInfo(int rank, const DevicePeerInfo& peer) {
    if (rank < 0) return Status::InvalidArgument("rank must be non-negative" LOC_MARK);
    peer_info_[rank] = peer;
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
