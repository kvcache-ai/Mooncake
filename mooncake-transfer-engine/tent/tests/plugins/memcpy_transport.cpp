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

// The plugin the loader tests load: copies DRAM to DRAM within the local
// segment with memcpy. It takes the SHM slot because the core leaves that slot
// empty by default (transports/shm/enable is false) and the selector already
// routes same-machine DRAM transfers to SHM.

#include <cstring>
#include <vector>

#include "tent/runtime/transport.h"
#include "tent/transport_plugin.h"

namespace mooncake {
namespace tent {
namespace {

struct MemcpySubBatch : public Transport::SubBatch {
    std::vector<TransferStatus> tasks;
    size_t size() const override { return tasks.size(); }
};

class MemcpyTransport : public Transport {
   public:
    MemcpyTransport() { caps.dram_to_dram = true; }

    // The loader tests rely on this: one object installed by two engines
    // would mean the loader shared it.
    Status install(std::string&, std::shared_ptr<ControlService>,
                   std::shared_ptr<Topology>,
                   std::shared_ptr<Config>) override {
        if (installed_) return Status::InvalidArgument("installed twice");
        installed_ = true;
        return Status::OK();
    }

    Status allocateSubBatch(SubBatchRef& batch, size_t) override {
        batch = new MemcpySubBatch();
        return Status::OK();
    }

    Status freeSubBatch(SubBatchRef& batch) override {
        delete static_cast<MemcpySubBatch*>(batch);
        batch = nullptr;
        return Status::OK();
    }

    Status submitTransferTasks(SubBatchRef batch,
                               const std::vector<Request>& requests) override {
        auto* sub_batch = static_cast<MemcpySubBatch*>(batch);
        for (const auto& request : requests)
            if (request.target_id != LOCAL_SEGMENT_ID)
                return Status::InvalidArgument(
                    "memcpy serves the local segment only" LOC_MARK);
        for (const auto& request : requests) {
            auto* target = reinterpret_cast<void*>(request.target_offset);
            if (request.opcode == Request::WRITE)
                std::memcpy(target, request.source, request.length);
            else
                std::memcpy(request.source, target, request.length);
            sub_batch->tasks.push_back({COMPLETED, request.length});
        }
        batch->notifyProgress();
        return Status::OK();
    }

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override {
        auto* sub_batch = static_cast<MemcpySubBatch*>(batch);
        if (task_id < 0 || task_id >= (int)sub_batch->tasks.size())
            return Status::InvalidArgument("invalid task id" LOC_MARK);
        status = sub_batch->tasks[task_id];
        return Status::OK();
    }

    Status addMemoryBuffer(BufferDesc& desc, const MemoryOptions&) override {
        desc.transports.push_back(SHM);
        return Status::OK();
    }

    Status removeMemoryBuffer(BufferDesc&) override { return Status::OK(); }

    const char* getName() const override { return "memcpy"; }

   private:
    bool installed_ = false;
};

std::shared_ptr<Transport> create(const TransportPluginContext&) {
    return std::make_shared<MemcpyTransport>();
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

extern "C" const char* tent_transport_plugin_build_id() {
    return TENT_BUILD_ID;
}

extern "C" const mooncake::tent::TransportPluginDesc*
tent_register_transport_plugin() {
    static const mooncake::tent::TransportPluginDesc desc{
        mooncake::tent::SHM, &mooncake::tent::create};
    return &desc;
}
