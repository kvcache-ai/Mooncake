// Copyright 2025 Alibaba Cloud and its affiliates
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

#include "transport/nvmeof_generic_transport/nvmeof_transport.h"

#include <glog/logging.h>

#include "common.h"
#include "config.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
NVMeoFGenericTransport::NVMeoFGenericTransport()
    : initiator(nullptr), worker_pool(nullptr), target(nullptr) {}

NVMeoFGenericTransport::~NVMeoFGenericTransport() {
    for (auto &it : segment_to_controller_) {
        worker_pool->removeController(it.second.get());
        initiator->detachController(it.second);
    }

    segment_to_controller_.clear();
    worker_pool.reset();
    initiator.reset();
    target.reset();
}

BatchID NVMeoFGenericTransport::allocateBatchID(size_t batch_size) {
    auto batch_id = Transport::allocateBatchID(batch_size);
    return batch_id;
}

Status NVMeoFGenericTransport::freeBatchID(BatchID batch_id) {
    Status rc = Transport::freeBatchID(batch_id);
    return rc;
}

Status NVMeoFGenericTransport::getTransferStatus(BatchID batch_id,
                                                 size_t task_id,
                                                 TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument("Task ID " + std::to_string(task_id) +
                                       " out of range " +
                                       std::to_string(task_count));
    }

    auto &task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;

    auto success_slice_count = task.success_slice_count;
    auto failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count < task.slice_count) {
        status.s = Transport::TransferStatusEnum::WAITING;
    } else {
        task.is_finished = true;
        if (failed_slice_count) {
            status.s = Transport::TransferStatusEnum::FAILED;
        } else {
            status.s = Transport::TransferStatusEnum::COMPLETED;
        }
    }

    return Status::OK();
}

Status NVMeoFGenericTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    std::unordered_map<SegmentHandle, std::vector<Slice *>> slice_to_submit;

    for (auto task : task_list) {
        auto request = task->request;
        Slice *slice = getSliceCache().allocate();
        slice->task = task;
        slice->source_addr = request->source;
        slice->length = request->length;
        slice->opcode = request->opcode;
        slice->target_id = request->target_id;
        slice->file_id = request->file_id;
        slice->nvmeof_generic.offset = request->target_offset;
        slice->status = Slice::PENDING;
        slice->ts = 0;

        task->slice_list.push_back(slice);
        task->total_bytes += request->length;
        __sync_fetch_and_add(&task->slice_count, 1);

        slice_to_submit[request->target_id].push_back(slice);
    }

    for (auto it : slice_to_submit) {
        auto ctrlr = getOrCreateController(it.first);
        if (ctrlr != nullptr) {
            for (auto &slice : it.second) {
                worker_pool->submitTask(ctrlr.get(), slice);
            }
        } else {
            for (auto slice : it.second) {
                slice->markFailed();
            }
        }
        it.second.clear();
    }

    return Status::OK();
}

Status NVMeoFGenericTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "NVMeoFGenericTransport: Exceed the limitation of "
                      "current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "NVMeoFGenericTransport: Exceed the limitation of capacity, batch "
            "id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    std::vector<TransferTask *> task_list;
    for (auto &request : entries) {
        auto &task = batch_desc.task_list[task_id++];
        task.batch_id = batch_id;
        task.request = &request;
        task_list.push_back(&task);
    }

    return this->submitTransferTask(task_list);
}

int NVMeoFGenericTransport::install(std::string &local_server_name,
                                    std::shared_ptr<TransferMetadata> meta,
                                    void **args) {
    int rc = Transport::install(local_server_name, meta, args);
    if (rc != 0) {
        LOG(ERROR) << "Transport::install failed, rc=" << rc;
        return rc;
    }

    if (args != nullptr && args[0] != nullptr) {
        std::string trStr = static_cast<char *>(args[0]);
        rc = parseTrid(trStr);
        if (rc != 0) {
            LOG(ERROR) << "Failed to parse nvmeof trid \"" << trStr
                       << "\", rc=" << rc;
            return rc;
        }
    }

    return 0;
}

int NVMeoFGenericTransport::setupLocalSegment() {
    if (this->target != nullptr) {
        return 0;
    }

    if (!validateTrid(local_trid)) {
        LOG(ERROR) << "NVMeoF trid not specified";
        return ERR_INVALID_ARGUMENT;
    }

    this->target = std::make_unique<NVMeoFTarget>(local_server_name_);
    int rc = this->target->setup(local_trid.trtype, local_trid.adrfam,
                                 local_trid.traddr, local_trid.trsvcid);
    if (rc != 0) {
        LOG(ERROR) << "Failed to create nvmeof target, rc=" << rc;
        return ERR_INVALID_ARGUMENT;
    }

    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) {
        LOG(ERROR) << "Failed to create local segment";
        this->target.reset();
        return ERR_MEMORY;
    }

    desc->name = local_server_name_;
    desc->protocol = "nvmeof_generic";
    desc->nvmeof_generic_trid = local_trid;
    desc->nvmeof_generic_trid.subnqn = this->target->getSubNQN();

    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

int NVMeoFGenericTransport::registerLocalMemory(void *addr, size_t length,
                                                const std::string &location,
                                                bool remote_accessible,
                                                bool update_metadata) {
    return 0;
}

int NVMeoFGenericTransport::unregisterLocalMemory(void *addr,
                                                  bool update_metadata) {
    return 0;
}

int NVMeoFGenericTransport::registerLocalFile(FileBufferID id,
                                              const std::string &path,
                                              size_t size) {
    int rc = setupLocalSegment();
    if (rc != 0) {
        LOG(ERROR) << "Failed to allocate local segment, rc=" << rc;
        return ERR_MEMORY;
    }

    rc = this->target->addFile(id, path);
    if (rc != 0) {
        LOG(ERROR) << "Failed to add file " << path << ", rc=" << rc;
        return rc;
    }

    FileBufferDesc buffer_desc;
    buffer_desc.id = id;
    buffer_desc.path = path;
    buffer_desc.size = size;
    /// TODO: Set align according to file type.
    buffer_desc.align = 0;

    rc = this->metadata_->addFileBuffer(buffer_desc, true);
    if (rc != 0) {
        LOG(ERROR) << "Failed to add file buffer " << path << ", rc=" << rc;
        this->target->removeFile(id);
        return rc;
    }

    return 0;
}

int NVMeoFGenericTransport::unregisterLocalFile(FileBufferID id) {
    if (this->target == nullptr) {
        LOG(ERROR) << "NVMeoFGenericTransport::target has not been initialized";
        return ERR_ADDRESS_NOT_REGISTERED;
    }

    int rc = this->metadata_->removeFileBuffer(id, true);
    if (rc != 0) {
        LOG(ERROR) << "Failed to remove file buffer " << id << ", rc=" << rc;
        return rc;
    }

    this->target->removeFile(id);
    return 0;
}

int NVMeoFGenericTransport::parseTrid(const std::string &trStr) {
    std::istringstream stream(trStr);
    std::string option;

    while (stream >> option) {
        auto sep = option.find('=');
        if (sep == option.npos) {
            sep = option.find(':');
            if (sep == option.npos) {
                LOG(ERROR) << "No separator '=' or ':' found in trid string \""
                           << trStr << "\"";
                return ERR_INVALID_ARGUMENT;
            }
        }

        auto key = option.substr(0, sep);
        auto value = option.substr(sep + 1);
        if (key.empty() || value.empty()) {
            LOG(ERROR) << "Invalid trid option: key=" << key
                       << " value=" << value;
            return ERR_INVALID_ARGUMENT;
        }

        if (key == "trtype") {
            local_trid.trtype = value;
        } else if (key == "adrfam") {
            local_trid.adrfam = value;
        } else if (key == "traddr") {
            local_trid.traddr = value;
        } else if (key == "trsvcid") {
            local_trid.trsvcid = value;
        } else {
            LOG(ERROR) << "Invalid trid string operation: key=" << key
                       << ", value=" << value;
            return ERR_INVALID_ARGUMENT;
        }
    }

    if (!validateTrid(local_trid)) {
        LOG(ERROR) << "Invalid trid: trtype=" << local_trid.trtype
                   << ", adrfam=" << local_trid.adrfam
                   << ", traddr=" << local_trid.traddr
                   << ", trsvcid=" << local_trid.trsvcid;
        return ERR_INVALID_ARGUMENT;
    }

    return 0;
}

bool NVMeoFGenericTransport::validateTrid(const NVMeoFTrid &local_trid) {
    return !(local_trid.trtype.empty() || local_trid.adrfam.empty() ||
             local_trid.traddr.empty() || local_trid.trsvcid.empty());
}

int NVMeoFGenericTransport::setupInitiator() {
    if (this->initiator == nullptr) {
        this->initiator =
            NVMeoFInitiator::create(globalConfig().nvmeof_generic_direct_io);
        if (this->initiator == nullptr) {
            LOG(ERROR) << "Failed to create nvmeof initiator";
            return ERR_MEMORY;
        }
    }

    if (this->worker_pool == nullptr) {
        this->worker_pool = std::make_unique<NVMeoFWorkerPool>(
            globalConfig().nvmeof_generic_num_workers);
        if (this->worker_pool == nullptr) {
            LOG(ERROR) << "Failed to create nvmeof worker pool";
            return ERR_MEMORY;
        }
    }

    return 0;
}

std::shared_ptr<NVMeoFController> NVMeoFGenericTransport::getOrCreateController(
    SegmentHandle handle) {
    {
        RWSpinlock::ReadGuard guard(controller_lock_);
        auto it = segment_to_controller_.find(handle);
        if (it != segment_to_controller_.end()) {
            return it->second;
        }
    }

    auto desc = metadata_->getSegmentDescByID(handle);
    if (desc == nullptr || desc->protocol != "nvmeof_generic" ||
        desc->file_buffers.size() <= 0) {
        LOG(ERROR) << "Invalid segment " << desc;
        return nullptr;
    }

    RWSpinlock::WriteGuard guard(controller_lock_);
    auto it = segment_to_controller_.find(handle);
    if (it != segment_to_controller_.end()) {
        // Someone else attached the controller.
        return it->second;
    }

    int rc = setupInitiator();
    if (rc != 0) {
        LOG(ERROR) << "Failed to setup initiator, rc=" << rc;
        return nullptr;
    }

    auto &trid = desc->nvmeof_generic_trid;
    auto controller = initiator->attachController(
        trid.trtype, trid.adrfam, trid.traddr, trid.trsvcid, trid.subnqn);
    if (controller == nullptr) {
        LOG(ERROR) << "Failed to attach controller trtype=" << trid.trtype
                   << " adrfam=" << trid.adrfam << " traddr=" << trid.traddr
                   << " trsvcid=" << trid.trsvcid << " subnqn=" << trid.subnqn;
        return nullptr;
    }

    rc = this->worker_pool->addController(controller);
    if (rc != 0) {
        LOG(ERROR) << "Failed to add controller to worker pool, rc=" << rc;
        initiator->detachController(controller);
        return nullptr;
    }

    segment_to_controller_[handle] = controller;
    return controller;
}

}  // namespace mooncake
