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

#include "tent/transfer_engine.h"
#include "tent/common/config.h"
#include "tent/runtime/transfer_engine_impl.h"
#include <glog/logging.h>

namespace mooncake {
namespace tent {

TransferEngine::TransferEngine()
    : impl_(std::make_unique<TransferEngineImpl>()) {}

TransferEngine::TransferEngine(const std::string config_path) {
    auto conf = std::make_shared<Config>();
    auto status = conf->load(config_path);
    if (!status.ok()) {
        LOG(WARNING) << "Failed to read config file " << config_path;
    }
    impl_ = std::make_unique<TransferEngineImpl>(conf);
}

TransferEngine::TransferEngine(std::shared_ptr<Config> conf)
    : impl_(std::make_unique<TransferEngineImpl>(conf)) {}

TransferEngine::~TransferEngine() {}

bool TransferEngine::available() const { return impl_->available(); }

const std::string TransferEngine::getSegmentName() const {
    return impl_->getSegmentName();
}

const std::string TransferEngine::getRpcServerAddress() const {
    return impl_->getRpcServerAddress();
}

uint16_t TransferEngine::getRpcServerPort() const {
    return impl_->getRpcServerPort();
}

Status TransferEngine::exportLocalSegment(std::string& shared_handle) {
    return impl_->exportLocalSegment(shared_handle);
}

Status TransferEngine::importRemoteSegment(SegmentID& handle,
                                           const std::string& shared_handle) {
    return impl_->importRemoteSegment(handle, shared_handle);
}

Status TransferEngine::openSegment(SegmentID& handle,
                                   const std::string& segment_name) {
    return impl_->openSegment(handle, segment_name);
}

Status TransferEngine::closeSegment(SegmentID handle) {
    return impl_->closeSegment(handle);
}

Status TransferEngine::getSegmentInfo(SegmentID handle, SegmentInfo& info) {
    return impl_->getSegmentInfo(handle, info);
}

Status TransferEngine::allocateLocalMemory(void** addr, size_t size,
                                           Location location) {
    return impl_->allocateLocalMemory(addr, size, location);
}

Status TransferEngine::allocateLocalMemory(void** addr, size_t size,
                                           MemoryOptions& options) {
    return impl_->allocateLocalMemory(addr, size, options);
}

Status TransferEngine::freeLocalMemory(void* addr) {
    return impl_->freeLocalMemory(addr);
}

Status TransferEngine::registerLocalMemory(void* addr, size_t size,
                                           Permission permission) {
    return impl_->registerLocalMemory({addr}, {size}, permission);
}

Status TransferEngine::registerLocalMemory(void* addr, size_t size,
                                           MemoryOptions& options) {
    return impl_->registerLocalMemory({addr}, {size}, options);
}

Status TransferEngine::registerLocalMemory(std::vector<void*> addr_list,
                                           std::vector<size_t> size_list,
                                           Permission permission) {
    return impl_->registerLocalMemory(addr_list, size_list, permission);
}

Status TransferEngine::registerLocalMemory(std::vector<void*> addr_list,
                                           std::vector<size_t> size_list,
                                           MemoryOptions& options) {
    return impl_->registerLocalMemory(addr_list, size_list, options);
}

Status TransferEngine::unregisterLocalMemory(void* addr, size_t size) {
    if (size == 0)
        return impl_->unregisterLocalMemory({addr});
    else
        return impl_->unregisterLocalMemory({addr}, {size});
}

Status TransferEngine::unregisterLocalMemory(std::vector<void*> addr_list,
                                             std::vector<size_t> size_list) {
    return impl_->unregisterLocalMemory(addr_list, size_list);
}

BatchID TransferEngine::allocateBatch(size_t batch_size) {
    return impl_->allocateBatch(batch_size);
}

Status TransferEngine::freeBatch(BatchID batch_id) {
    return impl_->freeBatch(batch_id);
}

Status TransferEngine::submitTransfer(
    BatchID batch_id, const std::vector<Request>& request_list) {
    return impl_->submitTransfer(batch_id, request_list);
}

Status TransferEngine::submitTransfer(BatchID batch_id,
                                      const std::vector<Request>& request_list,
                                      const Notification& notifi) {
    return impl_->submitTransfer(batch_id, request_list, notifi);
}

Status TransferEngine::sendNotification(SegmentID target_id,
                                        const Notification& notifi) {
    return impl_->sendNotification(target_id, notifi);
}

Status TransferEngine::receiveNotification(
    std::vector<Notification>& notifi_list) {
    return impl_->receiveNotification(notifi_list);
}

Status TransferEngine::getTransferStatus(BatchID batch_id, size_t task_id,
                                         TransferStatus& task_status) {
    return impl_->getTransferStatus(batch_id, task_id, task_status);
}

Status TransferEngine::getTransferStatus(
    BatchID batch_id, std::vector<TransferStatus>& status_list) {
    return impl_->getTransferStatus(batch_id, status_list);
}

Status TransferEngine::getTransferStatus(BatchID batch_id,
                                         TransferStatus& overall_status) {
    return impl_->getTransferStatus(batch_id, overall_status);
}

}  // namespace tent
}  // namespace mooncake
