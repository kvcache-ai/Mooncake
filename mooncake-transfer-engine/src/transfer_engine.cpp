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

#include "transfer_engine.h"
#include "transfer_engine_impl.h"
#include <utility>

namespace mooncake {

TransferEngine::TransferEngine(bool auto_discover)
    : impl_(std::make_shared<TransferEngineImpl>(auto_discover)) {}

TransferEngine::TransferEngine(bool auto_discover,
                               const std::vector<std::string>& filter)
    : impl_(std::make_shared<TransferEngineImpl>(auto_discover, filter)) {}

TransferEngine::~TransferEngine() = default;

int TransferEngine::init(const std::string& metadata_conn_string,
                         const std::string& local_server_name,
                         const std::string& ip_or_host_name,
                         uint64_t rpc_port) {
    return impl_->init(metadata_conn_string, local_server_name, ip_or_host_name,
                       rpc_port);
}

int TransferEngine::freeEngine() { return impl_->freeEngine(); }

Transport* TransferEngine::installTransport(const std::string& proto,
                                            void** args) {
    return impl_->installTransport(proto, args);
}

int TransferEngine::uninstallTransport(const std::string& proto) {
    return impl_->uninstallTransport(proto);
}

std::string TransferEngine::getLocalIpAndPort() {
    return impl_->getLocalIpAndPort();
}

int TransferEngine::getRpcPort() { return impl_->getRpcPort(); }

SegmentHandle TransferEngine::openSegment(const std::string& segment_name) {
    return impl_->openSegment(segment_name);
}

Status TransferEngine::CheckSegmentStatus(SegmentID sid) {
    return impl_->CheckSegmentStatus(sid);
}

int TransferEngine::closeSegment(SegmentHandle handle) {
    return impl_->closeSegment(handle);
}

int TransferEngine::removeLocalSegment(const std::string& segment_name) {
    return impl_->removeLocalSegment(segment_name);
}

int TransferEngine::registerLocalMemory(void* addr, size_t length,
                                        const std::string& location,
                                        bool remote_accessible,
                                        bool update_metadata) {
    return impl_->registerLocalMemory(addr, length, location, remote_accessible,
                                      update_metadata);
}

int TransferEngine::unregisterLocalMemory(void* addr, bool update_metadata) {
    return impl_->unregisterLocalMemory(addr, update_metadata);
}

int TransferEngine::registerLocalMemoryBatch(
    const std::vector<BufferEntry>& buffer_list, const std::string& location) {
    return impl_->registerLocalMemoryBatch(buffer_list, location);
}

int TransferEngine::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    return impl_->unregisterLocalMemoryBatch(addr_list);
}

BatchID TransferEngine::allocateBatchID(size_t batch_size) {
    return impl_->allocateBatchID(batch_size);
}

Status TransferEngine::freeBatchID(BatchID batch_id) {
    return impl_->freeBatchID(batch_id);
}

Status TransferEngine::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    return impl_->submitTransfer(batch_id, entries);
}

Status TransferEngine::submitTransferWithNotify(
    BatchID batch_id, const std::vector<TransferRequest>& entries,
    TransferMetadata::NotifyDesc notify_msg) {
    return impl_->submitTransferWithNotify(batch_id, entries, notify_msg);
}

int TransferEngine::getNotifies(
    std::vector<TransferMetadata::NotifyDesc>& notifies) {
    return impl_->getNotifies(notifies);
}

int TransferEngine::sendNotifyByID(SegmentID target_id,
                                   TransferMetadata::NotifyDesc notify_msg) {
    return impl_->sendNotifyByID(target_id, notify_msg);
}

int TransferEngine::sendNotifyByName(std::string remote_agent,
                                     TransferMetadata::NotifyDesc notify_msg) {
    return impl_->sendNotifyByName(std::move(remote_agent), notify_msg);
}

Status TransferEngine::getTransferStatus(BatchID batch_id, size_t task_id,
                                         TransferStatus& status) {
    return impl_->getTransferStatus(batch_id, task_id, status);
}

Status TransferEngine::getBatchTransferStatus(BatchID batch_id,
                                              TransferStatus& status) {
    return impl_->getBatchTransferStatus(batch_id, status);
}

Transport* TransferEngine::getTransport(const std::string& proto) {
    return impl_->getTransport(proto);
}

int TransferEngine::syncSegmentCache(const std::string& segment_name) {
    return impl_->syncSegmentCache(segment_name);
}

std::shared_ptr<TransferMetadata> TransferEngine::getMetadata() {
    return impl_->getMetadata();
}

bool TransferEngine::checkOverlap(void* addr, uint64_t length) {
    return impl_->checkOverlap(addr, length);
}

void TransferEngine::setAutoDiscover(bool auto_discover) {
    impl_->setAutoDiscover(auto_discover);
}

void TransferEngine::setWhitelistFilters(std::vector<std::string>&& filters) {
    impl_->setWhitelistFilters(std::move(filters));
}

int TransferEngine::numContexts() const { return impl_->numContexts(); }

std::shared_ptr<Topology> TransferEngine::getLocalTopology() {
    return impl_->getLocalTopology();
}
}  // namespace mooncake
