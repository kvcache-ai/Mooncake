// Copyright 2025 Huawei Technologies Co., Ltd
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

#include "transport/ascend_transport/heterogeneous_rdma_transport.h"

namespace mooncake {
HeterogeneousRdmaTransport::HeterogeneousRdmaTransport() {
    transport_ = new RdmaTransport();
}

HeterogeneousRdmaTransport::~HeterogeneousRdmaTransport() { delete transport_; }

int HeterogeneousRdmaTransport::install(std::string &local_server_name,
                                        std::shared_ptr<TransferMetadata> meta,
                                        std::shared_ptr<Topology> topo) {
    int ret = 0;
    local_server_name_ = local_server_name;

    ret = aclrtGetDevice(&deviceLogicId_);
    if (ret) {
        LOG(ERROR) << "HeterogeneousRdmaTransport: aclrtGetDevice failed, ret: "
                   << ret;
        return ret;
    }

    LOG(INFO) << "HeterogeneousRdmaTransport: begin to install transport,  "
                 "local_server_name: "
              << local_server_name << "deviceLogicId_: " << deviceLogicId_;

    hostAddr_ = (void *)malloc(HUGE_HOST_SIZE);
    memset(hostAddr_, 0, HUGE_HOST_SIZE);
    if (transport_ == nullptr) {
        LOG(ERROR) << "HeterogeneousRdmaTransport:transport is null";
        return ret;
    }

    ret = transport_->install(local_server_name_, meta, topo);
    if (ret) {
        LOG(ERROR) << "RdmaTransport install error, ret: " << ret;
        return ret;
    }
    ret = transport_->registerLocalMemory(hostAddr_, HUGE_HOST_SIZE, "cpu",
                                          true, true);
    if (ret) {
        LOG(ERROR)
            << "HeterogeneousRdmaTransport: registerLocalMemory error, ret: "
            << ret;
        return ret;
    }
    return ret;
}

int HeterogeneousRdmaTransport::registerLocalMemory(void *addr, size_t length,
                                                    const std::string &name,
                                                    bool remote_accessible,
                                                    bool update_metadata) {
    aclrtPtrAttributes attributes;
    int ret = aclrtPointerGetAttributes(addr, &attributes);
    if (ret) {
        LOG(ERROR) << "aclrtPointrtGetAttributes error, ret: " << ret;
        return ret;
    }

    if (attributes.location.type == 0) {
        ret = transport_->registerLocalMemory(addr, length, "cpu", true, true);
        if (ret) {
            LOG(ERROR) << "rdma transport registerLocalMemory error, ret: "
                       << ret;
            return ret;
        }
    }
    return 0;
}

int HeterogeneousRdmaTransport::unregisterLocalMemory(void *addr,
                                                      bool update_metadata) {
    aclrtPtrAttributes attributes;
    int ret = aclrtPointerGetAttributes(addr, &attributes);
    if (ret) {
        LOG(ERROR) << "aclrtPointrtGetAttributes error, ret: " << ret;
        return ret;
    }

    if (attributes.location.type == 0) {
        ret = transport_->unregisterLocalMemory(addr, true);
        if (ret) {
            LOG(ERROR) << "rdma transport unregisterLocalMemory error, ret: "
                       << ret;
            return ret;
        }
    }
    return 0;
}

int HeterogeneousRdmaTransport::registerLocalMemoryBatch(
    const std::vector<HeterogeneousRdmaTransport::BufferEntry> &buffer_list,
    const std::string &location) {
    int ret;
    for (auto &buffer : buffer_list) {
        ret = registerLocalMemory(buffer.addr, buffer.length, location, true,
                                  false);
        if (ret) {
            LOG(ERROR) << "HeterogeneousRdmaTransport registerLocalMemoryBatch "
                          "error, ret: "
                       << ret;
            return ret;
        }
    }
    return metadata_->updateLocalSegmentDesc();
}

int HeterogeneousRdmaTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    int ret;
    for (auto &addr : addr_list) {
        ret = unregisterLocalMemory(addr, false);
        if (ret) {
            LOG(ERROR) << "HeterogeneousRdmaTransport "
                          "unregisterLocalMemoryBatch error, ret: "
                       << ret;
            return ret;
        }
    }
    return metadata_->updateLocalSegmentDesc();
}

Status HeterogeneousRdmaTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    std::vector<TransferRequest> new_entries;
    new_entries.resize(entries.size());
    int index = 0;
    aclError ret;
    if (firstSubmit_) {
        ret = aclrtSetDevice(deviceLogicId_);
        if (ret) {
            LOG(ERROR)
                << "HeterogeneousRdmaTransport: aclrtSetDevice failed ret: "
                << ret;
        }
        ret = aclrtCreateStream(&stream_);
        if (ret) {
            LOG(ERROR)
                << "HeterogeneousRdmaTransport: aclrtCreateStream error, ret: "
                << ret;
        }
        firstSubmit_ = false;
    }

    {
        std::lock_guard<std::mutex> lock(memcpy_mutex_);
        for (auto &request : entries) {
            aclrtPtrAttributes attributes;
            ret = aclrtPointerGetAttributes(request.source, &attributes);
            if (ret) {
                LOG(ERROR) << "aclrtPointrtGetAttributes error, ret: " << ret;
                return Status::InvalidArgument(
                    "HeterogeneousRdmaTransport: Exceed the limitation of "
                    "capacity, batch id: ");
            }

            if (attributes.location.type == 0) {
                continue;
            }
            ret =
                aclrtMemcpyAsync(static_cast<char *>(hostAddr_) + offset_,
                                 request.length, request.source, request.length,
                                 ACL_MEMCPY_DEVICE_TO_HOST, stream_);
            if (ret) {
                LOG(ERROR) << "HeterogeneousRdmaTransport: aclrtMemcpyAsync "
                              "error, ret: "
                           << ret << ", hostAddr: " << hostAddr_
                           << ", offset_: " << offset_
                           << ", deviceAddr: " << request.source
                           << "len: " << request.length;
                return Status::InvalidArgument(
                    "HeterogeneousRdmaTransport: Exceed the limitation of "
                    "capacity, batch id: ");
            }
            new_entries[index] = request;
            new_entries[index].source =
                static_cast<char *>(hostAddr_) + offset_;
            offset_ += request.length;
            if (offset_ >= HUGE_HOST_SIZE) {
                offset_ = 0;
            }
        }

        ret = aclrtSynchronizeStream(stream_);
        if (ret) {
            LOG(ERROR) << "HeterogeneousRdmaTransport: aclrtSynchronizeStream "
                          "error, ret: "
                       << ret;
            return Status::InvalidArgument(
                "HeterogeneousRdmaTransport: Exceed the limitation of "
                "capacity, "
                "batch id: ");
        }
    }

    return transport_->submitTransfer(batch_id, new_entries);
}

Status HeterogeneousRdmaTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    aclError ret;
    if (firstSubmit_) {
        ret = aclrtSetDevice(deviceLogicId_);
        if (ret) {
            LOG(ERROR)
                << "HeterogeneousRdmaTransport: aclrtSetDevice failed ret: "
                << ret;
        }

        ret = aclrtCreateStream(&stream_);
        if (ret) {
            LOG(ERROR)
                << "HeterogeneousRdmaTransport: aclrtCreateStream error, ret: "
                << ret;
        }
        firstSubmit_ = false;
    }

    {
        std::lock_guard<std::mutex> lock(memcpy_mutex_);
        for (size_t index = 0; index < task_list.size(); ++index) {
            auto &task = *task_list[index];
            auto &request = *task.request;

            aclrtPtrAttributes attributes;
            ret = aclrtPointerGetAttributes(request.source, &attributes);
            if (ret) {
                LOG(ERROR) << "aclrtPointrtGetAttributes error, ret: " << ret;
                return Status::InvalidArgument(
                    "HeterogeneousRdmaTransport: Exceed the limitation of "
                    "capacity, batch id: ");
            }

            if (attributes.location.type == 0) {
                continue;
            }

            ret =
                aclrtMemcpyAsync(static_cast<char *>(hostAddr_) + offset_,
                                 request.length, request.source, request.length,
                                 ACL_MEMCPY_DEVICE_TO_HOST, stream_);
            if (ret) {
                LOG(ERROR) << "HeterogeneousRdmaTransport: aclrtMemcpyAsync "
                              "error, ret: "
                           << ret << ", hostAddr: " << hostAddr_
                           << ", offset_: " << offset_
                           << ", deviceAddr: " << request.source
                           << ", len: " << request.length;
                return Status::InvalidArgument(
                    "HeterogeneousRdmaTransport: Exceed the limitation of "
                    "capacity, batch id: ");
            }
            request.source = static_cast<char *>(hostAddr_) + offset_;
            offset_ += request.length;
            if (offset_ >= HUGE_HOST_SIZE) {
                offset_ = 0;
            }
        }

        ret = aclrtSynchronizeStream(stream_);
        if (ret) {
            LOG(ERROR) << "HeterogeneousRdmaTransport: aclrtSynchronizeStream "
                          "error, ret: "
                       << ret;
            return Status::InvalidArgument(
                "HeterogeneousRdmaTransport: Exceed the limitation of "
                "capacity, "
                "batch id: ");
        }
    }

    return transport_->submitTransferTask(task_list);
}

Status HeterogeneousRdmaTransport::getTransferStatus(
    BatchID batch_id, std::vector<TransferStatus> &status) {
    return transport_->getTransferStatus(batch_id, status);
}

Status HeterogeneousRdmaTransport::getTransferStatus(BatchID batch_id,
                                                     size_t task_id,
                                                     TransferStatus &status) {
    return transport_->getTransferStatus(batch_id, task_id, status);
}

}  // namespace mooncake
