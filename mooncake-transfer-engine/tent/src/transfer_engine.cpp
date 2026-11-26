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
#include "tent/runtime/topology.h"
#include "batch_submit.h"
#ifdef USE_CUDA
#include <cuda_runtime_api.h>
#endif
#include <glog/logging.h>

namespace mooncake {
namespace tent {

TransferEngine::TransferEngine()
    : impl_(std::make_unique<TransferEngineImpl>()),
      batch_submit_executor_(std::make_shared<BatchSubmitExecutor>()) {}

TransferEngine::TransferEngine(const std::string config_path)
    : batch_submit_executor_(std::make_shared<BatchSubmitExecutor>()) {
    auto conf = std::make_shared<Config>();
    auto status = conf->loadFile(config_path);
    if (!status.ok()) {
        LOG(ERROR) << "Failed to read config file " << config_path << ": "
                   << status.ToString();
    }
    impl_ = std::make_unique<TransferEngineImpl>(conf, status.ok());
}

TransferEngine::TransferEngine(std::shared_ptr<Config> conf)
    : impl_(std::make_unique<TransferEngineImpl>(conf)),
      batch_submit_executor_(std::make_shared<BatchSubmitExecutor>()) {}

TransferEngine::~TransferEngine() { batch_submit_executor_->shutdown(); }

bool TransferEngine::supportsParallelBatchSubmit(SegmentID peer) const {
    return impl_->supportsParallelBatchSubmit(peer);
}

bool TransferEngine::isBatchSubmitEligible(size_t count) const {
    return impl_ && batch_submit_executor_ &&
           batch_submit_executor_->eligible(count);
}

struct TransferEngine::TentSubBatchAdapter {
    using Executor = BatchSubmitExecutor;
    TransferEngine& engine;
    const std::vector<Request>& requests;
    BatchID sub_batch_id = 0;

    Executor::Result allocate();
    Executor::Result submit();
    SubBatchProgress poll();
    Executor::Result release();
};

BatchSubmitExecutor::Result TransferEngine::TentSubBatchAdapter::allocate() {
    sub_batch_id = engine.allocateBatch(requests.size());
    return sub_batch_id
               ? Executor::Result::OK()
               : Executor::Result::Error("Could not allocate sub-batch");
}

BatchSubmitExecutor::Result TransferEngine::TentSubBatchAdapter::submit() {
    auto status = engine.submitTransfer(sub_batch_id, requests);
    return status.ok() ? Executor::Result::OK()
                       : Executor::Result::SubmitError(status.ToString());
}

SubBatchProgress TransferEngine::TentSubBatchAdapter::poll() {
    TransferStatus status{};
    auto polled = engine.getTransferStatus(sub_batch_id, status);
    if (!polled.ok()) return {true, false, polled.ToString()};
    const bool terminal = status.s != PENDING && status.s != INITIAL;
    return {terminal,
            status.s == COMPLETED,
            {},
            status.s == FAILED || status.s == TIMEOUT};
}

BatchSubmitExecutor::Result TransferEngine::TentSubBatchAdapter::release() {
    auto status = engine.freeBatch(sub_batch_id);
    sub_batch_id = 0;
    return status.ok() ? Executor::Result::OK()
                       : Executor::Result::Error(status.ToString());
}

std::optional<Status> TransferEngine::tryParallelBatchTransfer(
    SegmentID peer, size_t count, const SubBatchRequestBuilder& builder,
    std::chrono::nanoseconds timeout,
    ParallelBatchTransferFailureInfo* failure_info) {
    using Executor = ::mooncake::BatchSubmitExecutor;
    if (failure_info) *failure_info = {};
    if (!isBatchSubmitEligible(count)) return std::nullopt;
    auto& executor = *batch_submit_executor_;
    const auto deadline = Executor::Clock::now() + timeout;
    if (!supportsParallelBatchSubmit(peer)) {
        VLOG(1) << "TENT parallel batch admission declined: peer=" << peer
                << " descriptors=" << count;
        return std::nullopt;
    }
    VLOG(1) << "TENT parallel batch admitted: peer=" << peer
            << " descriptors=" << count;
    int device = -1;
#ifdef USE_CUDA
    // Host-only transfers also work without an initialized CUDA runtime.
    // A missing GPU, an uninitialized driver, and the CI driver stub all
    // fail this query with different codes. There is no caller device to
    // propagate, so the workers skip cudaSetDevice.
    if (cudaGetDevice(&device) != cudaSuccess) {
        device = -1;
        (void)cudaGetLastError();
    }
#endif
    std::vector<std::vector<Request>> request_storage(
        std::min(executor.config().threads, count));
    auto result = executor.execute(
        count, deadline,
        [&](Executor::Range range, const Executor::Cancellation& cancellation) {
#ifdef USE_CUDA
            if (device >= 0 && cudaSetDevice(device) != cudaSuccess)
                return Executor::Result::Error(
                    "Could not bind batch submit CUDA device");
#endif
            auto& requests = request_storage[range.index];
            requests = builder(range.begin, range.end);
            if (requests.size() != range.end - range.begin)
                return Executor::Result::Error(
                    "Invalid sub-batch request count");
            TentSubBatchAdapter sub_batch{*this, requests};
            return ::mooncake::runSubBatch(sub_batch, cancellation);
        });
    if (failure_info) *failure_info = {result.submit_failed, result.retryable};
    return result.ok() ? Status::OK() : Status::InternalError(result.error);
}

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

std::shared_ptr<Topology> TransferEngine::getLocalTopology() const {
    return impl_->getLocalTopology();
}

std::string TransferEngine::getLocalTopologyString() const {
    auto topo = impl_->getLocalTopology();
    return topo ? topo->toString() : "{}";
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

Status TransferEngine::mapRemoteMemory(SegmentID peer, void* gva, size_t size,
                                       TransportType type) {
    return impl_->mapRemoteMemory(peer, gva, size, type);
}

Status TransferEngine::unmapRemoteMemory(SegmentID peer, void* gva,
                                         TransportType type) {
    return impl_->unmapRemoteMemory(peer, gva, type);
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

Status TransferEngine::submitTransferRequiringPostSubmitCancellation(
    BatchID batch_id, const std::vector<Request>& request_list) {
    return impl_->submitTransferRequiringPostSubmitCancellation(batch_id,
                                                                request_list);
}

Status TransferEngine::submitTransfer(BatchID batch_id,
                                      const std::vector<Request>& request_list,
                                      const Notification& notifi) {
    return impl_->submitTransfer(batch_id, request_list, notifi);
}

Status TransferEngine::cancelTransfer(BatchID batch_id, size_t task_id) {
    return impl_->cancelTransfer(batch_id, task_id);
}

Status TransferEngine::sendNotification(SegmentID target_id,
                                        const Notification& notifi) {
    return impl_->sendNotification(target_id, notifi);
}

Status TransferEngine::receiveNotification(
    std::vector<Notification>& notifi_list) {
    return impl_->receiveNotification(notifi_list);
}

Status TransferEngine::probePeerAliveByID(SegmentID target_id) {
    return impl_->probePeerAliveByID(target_id);
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

Status TransferEngine::waitTransfer(BatchID batch_id,
                                    std::vector<TransferStatus>* status_list,
                                    uint64_t timeout_ms) {
    return impl_->waitTransfer(batch_id, status_list, timeout_ms);
}

Status TransferEngine::progressBatch(BatchID batch_id,
                                     TransferStatus& overall_status) {
    return impl_->progressBatch(batch_id, overall_status);
}

Status TransferEngine::getNicLoadStats(std::vector<NicLoadStats>& stats) const {
    return impl_->getNicLoadStats(stats);
}

}  // namespace tent
}  // namespace mooncake
