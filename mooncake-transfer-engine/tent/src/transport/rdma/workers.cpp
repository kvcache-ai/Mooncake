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

#include "tent/transport/rdma/workers.h"

#include <sys/epoll.h>

#include <cassert>
#include <glog/logging.h>

#include "tent/transport/rdma/endpoint_store.h"
#include "tent/common/utils/ip.h"
#include "tent/common/utils/string_builder.h"
#include "tent/common/utils/os.h"
#include "tent/common/utils/random.h"
#include "tent/common/log_rate_limiter.h"

namespace mooncake {
namespace tent {

thread_local int tl_wid = -1;

// Helper function to get diagnostic message for WC errors
static const char* getWcErrorDiagnostic(ibv_wc_status status) {
    switch (status) {
        case IBV_WC_LOC_LEN_ERR:
            return "Local length error - buffer size too small. Check memory "
                   "registration size.";
        case IBV_WC_LOC_PROT_ERR:
            return "Local protection error - check memory access permissions "
                   "(LOCAL_WRITE).";
        case IBV_WC_LOC_QP_OP_ERR:
            return "Local QP operation error - QP not in correct state for "
                   "this operation.";
        case IBV_WC_WR_FLUSH_ERR:
            return "WR flushed error - connection closed or QP reset. Peer may "
                   "have disconnected.";
        case IBV_WC_MW_BIND_ERR:
            return "Memory window bind error - invalid memory window "
                   "operation.";
        case IBV_WC_BAD_RESP_ERR:
            return "Bad response error - invalid response from peer. Check "
                   "peer configuration.";
        case IBV_WC_LOC_ACCESS_ERR:
            return "Local access error - invalid memory access. Check memory "
                   "registration.";
        case IBV_WC_REM_INV_REQ_ERR:
            return "Invalid request error - peer rejected request. Check peer "
                   "capabilities.";
        case IBV_WC_REM_ACCESS_ERR:
            return "Remote access error - peer denied access. Check remote "
                   "memory permissions.";
        case IBV_WC_REM_OP_ERR:
            return "Remote operation error - peer operation failed. Check peer "
                   "state.";
        case IBV_WC_RETRY_EXC_ERR:
            return "Retry counter exceeded - network congestion or physical "
                   "link issue.";
        case IBV_WC_RNR_RETRY_EXC_ERR:
            return "Receiver not ready retry exceeded - peer queue full or not "
                   "posting receives.";
        case IBV_WC_LOC_RDD_VIOL_ERR:
            return "Local RDD violation - Reliable Datagram domain error.";
        case IBV_WC_REM_INV_RD_REQ_ERR:
            return "Invalid read request - peer rejected read request.";
        case IBV_WC_REM_ABORT_ERR:
            return "Remote abort error - peer aborted operation.";
        case IBV_WC_INV_EECN_ERR:
            return "Invalid EECN error - invalid extended channel number.";
        case IBV_WC_INV_EEC_STATE_ERR:
            return "Invalid EEC state error - extended channel in wrong state.";
        case IBV_WC_FATAL_ERR:
            return "Fatal error - unrecoverable error. Reset required.";
        case IBV_WC_RESP_TIMEOUT_ERR:
            return "Response timeout - peer not responding. Check peer "
                   "liveness.";
        case IBV_WC_GENERAL_ERR:
            return "General error - unspecified error. Check logs for details.";
        default:
            return "Unknown error code.";
    }
}

Workers::Workers(RdmaTransport* transport)
    : transport_(transport), num_workers_(0), running_(false) {
    device_quota_ = std::make_unique<DeviceQuota>();
    device_quota_->loadTopology(transport_->local_topology_);
    auto& conf = transport_->conf_;
    auto shared_quota_shm_path =
        conf->get("transports/rdma/shared_quota_shm_path", "");
    if (!shared_quota_shm_path.empty()) {
        device_quota_->enableSharedQuota(shared_quota_shm_path);
    }
    auto cross_numa_access =
        conf->get("transports/rdma/cross_numa_access", false);
    device_quota_->setCrossNumaAccess(cross_numa_access);
    auto local_weight = conf->get("transports/rdma/local_weight", 1.0);
    device_quota_->setLocalWeight(local_weight);
    auto learning_rate = conf->get("transports/rdma/learning_rate", 0.1);
    device_quota_->setLearningRate(learning_rate);
    auto diffusion_interval =
        conf->get("transports/rdma/diffusion_interval", 10);
    device_quota_->setDiffusionInterval(diffusion_interval);
    VLOG(1) << "RDMA Workers initialized: cross_numa=" << cross_numa_access
            << ", local_weight=" << local_weight
            << ", learning_rate=" << learning_rate;
}

Workers::~Workers() {
    if (running_) stop();
}

Status Workers::start() {
    const static uint64_t kDefaultMaxTimeoutNs = 10000000000ull;
    if (!running_) {
        running_ = true;
        monitor_ = std::thread([this] { monitorThread(); });
        num_workers_ = transport_->params_->workers.num_workers;
        slice_timeout_ns_ = transport_->conf_->get(
            "transports/rdma/max_timeout_ns", kDefaultMaxTimeoutNs);
        worker_context_ = new WorkerContext[num_workers_];
        for (size_t id = 0; id < num_workers_; ++id) {
            worker_context_[id].thread =
                std::thread([this, id] { workerThread(id); });
        }
        VLOG(1) << "RDMA workers started: count=" << num_workers_;
    }
    return Status::OK();
}

Status Workers::stop() {
    if (!running_) return Status::OK();
    running_ = false;
    for (size_t id = 0; id < num_workers_; ++id) {
        auto& worker = worker_context_[id];
        {
            std::lock_guard<std::mutex> lock(worker.mutex);
            worker.cv.notify_all();
        }
        worker.thread.join();
    }
    monitor_.join();
    delete[] worker_context_;
    worker_context_ = nullptr;
    VLOG(1) << "RDMA workers stopped";
    return Status::OK();
}

Status Workers::submit(RdmaSliceList& slice_list, int worker_id) {
    if (worker_id < 0 || worker_id >= (int)num_workers_) {
        // If caller didn't specify the worker, find the least loaded one
        long min_inflight = INT64_MAX;
        int start_id = SimpleRandom::Get().next(num_workers_);
        for (size_t i = start_id; i < start_id + num_workers_; ++i) {
            auto current =
                worker_context_[i % num_workers_].inflight_slices.load(
                    std::memory_order_relaxed);
            if (current < min_inflight) {
                worker_id = i % num_workers_;
                min_inflight = current;
            }
        }
    }
    auto& worker = worker_context_[worker_id];
    worker.queue.push(slice_list);
    if (!worker.inflight_slices.fetch_add(slice_list.num_slices)) {
        std::lock_guard<std::mutex> lock(worker.mutex);
        if (worker.in_suspend) worker.cv.notify_all();
    }
    return Status::OK();
}

Status Workers::submit(RdmaSlice* slice) {
    RdmaSliceList slice_list;
    slice_list.first = slice;
    slice_list.num_slices = 1;
    return submit(slice_list);
}

Status Workers::cancel(RdmaSliceList& slice_list) {
    return Status::NotImplemented("cancel not implemented" LOC_MARK);
}

std::shared_ptr<RdmaEndPoint> Workers::getEndpoint(Workers::PostPath path) {
    std::string target_seg_name, target_dev_name;
    RouteHint hint;
    auto target_id = path.remote_segment_id;
    auto device_id = path.remote_device_id;
    auto& segment_manager = transport_->metadata_->segmentManager();
    if (target_id == LOCAL_SEGMENT_ID) {
        hint.segment = segment_manager.getLocal().get();
    } else {
        segment_manager.getRemoteCached(hint.segment, target_id);
    }
    if (hint.segment->type != SegmentType::Memory) return nullptr;
    hint.topo = &std::get<MemorySegmentDesc>(hint.segment->detail).topology;
    target_seg_name = hint.segment->name;
    target_dev_name = hint.topo->getNicName(device_id);
    if (target_seg_name.empty() || target_dev_name.empty()) {
        LOG(ERROR) << "Empty target segment or device name";
        return nullptr;
    }
    auto context = transport_->context_set_[path.local_device_id].get();
    if (context->status() != RdmaContext::DEVICE_ENABLED) {
        // LOG(WARNING) << "Context " << context->name() << " is not serving";
        return nullptr;  // experimental: force to fail this slice and mark this
                         // connection unavailable
    }
    std::shared_ptr<RdmaEndPoint> endpoint;
    auto peer_name = MakeNicPath(target_seg_name, target_dev_name);
    endpoint = context->endpointStore()->getOrInsert(peer_name);
    if (endpoint && endpoint->status() == RdmaEndPoint::EP_RESET) {
        context->endpointStore()->remove(endpoint.get());
        endpoint = context->endpointStore()->getOrInsert(peer_name);
    }
    if (!endpoint) {
        LOG(ERROR) << "Cannot allocate endpoint " << peer_name;
        return nullptr;
    }
    if (endpoint->status() != RdmaEndPoint::EP_READY) {
        auto status = endpoint->connect(target_seg_name, target_dev_name);
        if (!status.ok()) {
            thread_local LogRateLimiter rate_limiter(LOG_RATE_LIMIT_10S);
            if (rate_limiter.shouldLog()) {
                LOG(ERROR) << "Endpoint connection failed: " << peer_name
                           << ", segment=" << target_seg_name
                           << ", device=" << target_dev_name
                           << ", status=" << status.ToString();
            }
            return nullptr;
        }
    }
    return endpoint;
}

void Workers::disableEndpoint(RdmaSlice* slice) {
    SegmentDesc* desc = nullptr;
    auto& segment_manager = transport_->metadata_->segmentManager();
    auto target_id = slice->task->request.target_id;
    if (target_id == LOCAL_SEGMENT_ID) {
        desc = segment_manager.getLocal().get();
    } else {
        auto status = segment_manager.getRemoteCached(desc, target_id);
        if (!status.ok()) return;
    }
    if (desc) {
        auto& worker = worker_context_[tl_wid];
        auto& rail = worker.rails[desc->machine_id];
        rail.markFailed(slice->source_dev_id, slice->target_dev_id);
    }
    if (slice->ep_weak_ptr) {
        slice->ep_weak_ptr->acknowledge(slice, FAILED);
        slice->ep_weak_ptr->reset();
    }
}

void Workers::asyncPostSend() {
    auto& worker = worker_context_[tl_wid];
    std::vector<RdmaSliceList> result;
    worker.queue.pop(result);
    for (auto& slice_list : result) {
        if (slice_list.num_slices == 0) continue;
        auto slice = slice_list.first;
        for (int id = 0; id < slice_list.num_slices; ++id) {
            auto status = generatePostPath(slice);
            if (!status.ok()) {
                thread_local LogRateLimiter rate_limiter(LOG_RATE_LIMIT_2S);
                if (rate_limiter.shouldLog()) {
                    LOG(ERROR) << "Failed to generate post path: "
                               << status.ToString();
                }
                updateSliceStatus(slice, FAILED);
            } else {
                PostPath path{
                    .local_device_id = slice->source_dev_id,
                    .remote_segment_id = slice->task->request.target_id,
                    .remote_device_id = slice->target_dev_id};
                worker.requests[path].push_back(slice);
            }
            slice = slice->next;
        }
    }

    for (auto& entry : worker.requests) {
        auto& path = entry.first;
        auto& slices = entry.second;
        if (slices.empty()) continue;
        auto endpoint = getEndpoint(path);
        if (!endpoint) {
            std::vector<RdmaSlice*> clone;
            slices.swap(clone);
            for (auto slice : clone) {
                slice->retry_count++;
                if (slice->retry_count >=
                    transport_->params_->workers.max_retry_count) {
                    thread_local LogRateLimiter rate_limiter(LOG_RATE_LIMIT_2S);
                    if (rate_limiter.shouldLog()) {
                        LOG(WARNING)
                            << "Slice retry count exceeded: "
                            << transport_->params_->workers.max_retry_count;
                    }
                    disableEndpoint(slice);
                    updateSliceStatus(slice, FAILED);
                } else {
                    submit(slice);
                }
            }
            continue;
        }

        int num_submitted = endpoint->submitSlices(slices, tl_wid);
        for (int id = 0; id < num_submitted; ++id) {
            auto slice = slices[id];
            if (slice->failed) {
                slice->retry_count++;
                if (slice->retry_count >=
                    transport_->params_->workers.max_retry_count) {
                    thread_local LogRateLimiter rate_limiter(LOG_RATE_LIMIT_2S);
                    if (rate_limiter.shouldLog()) {
                        LOG(WARNING)
                            << "Slice retry count exceeded: "
                            << transport_->params_->workers.max_retry_count;
                    }
                    disableEndpoint(slice);
                    updateSliceStatus(slice, FAILED);
                } else {
                    submit(slice);
                }
            } else {
                slice->submit_ts = getCurrentTimeInNano();
            }
        }

        if (num_submitted) {
            worker.inflight_slice_set.insert(slices.begin(),
                                             slices.begin() + num_submitted);
            slices.erase(slices.begin(), slices.begin() + num_submitted);
        }
    }
}

void Workers::asyncPollCq() {
    auto& worker = worker_context_[tl_wid];
    const static size_t kPollCount = 64;
    int num_contexts = (int)transport_->context_set_.size();
    int num_cq_list = transport_->params_->device.num_cq_list;
    int num_slices = 0;

    uint64_t current_ts = getCurrentTimeInNano();
    std::vector<RdmaSlice*> slice_to_remove;
    for (auto& slice : worker.inflight_slice_set) {
        if (slice->word != PENDING) continue;
        if (current_ts - slice->enqueue_ts > slice_timeout_ns_) {
            auto ep = slice->ep_weak_ptr;
            thread_local LogRateLimiter rate_limiter(LOG_RATE_LIMIT_5S);
            if (rate_limiter.shouldLog()) {
                LOG(WARNING) << "Slice transfer timeout (software)";
            }
            auto num_slices = ep->acknowledge(slice, TIMEOUT);
            disableEndpoint(slice);
            worker.inflight_slices.fetch_sub(num_slices);
            slice_to_remove.push_back(slice);
        }
    }
    for (auto& slice : slice_to_remove) worker.inflight_slice_set.erase(slice);

    for (int index = 0; index < num_contexts; index++) {
        auto& context = transport_->context_set_[index];
        auto cq = context->cq(tl_wid % num_cq_list);
        ibv_wc wc[kPollCount];
        int nr_poll = cq->poll(kPollCount, wc);
        if (nr_poll < 0) continue;
        auto poll_ts = getCurrentTimeInNano();
        for (int i = 0; i < nr_poll; ++i) {
            auto slice = (RdmaSlice*)wc[i].wr_id;
            worker.inflight_slice_set.erase(slice);
            auto ep = slice->ep_weak_ptr;
            double enqueue_lat =
                (slice->submit_ts - slice->enqueue_ts) / 1000.0;
            double inflight_lat = (poll_ts - slice->submit_ts) / 1000.0;
            double overall_lat_sec = (poll_ts - slice->enqueue_ts) / 1e9;
            if (slice->retry_count == 0) {
                device_quota_->release(slice->source_dev_id, slice->length,
                                       overall_lat_sec);
            }
            if (slice->word != PENDING) continue;
            if (wc[i].status != IBV_WC_SUCCESS) {
                if (wc[i].status != IBV_WC_WR_FLUSH_ERR) {
                    // Rate limit WC error logging
                    thread_local LogRateLimiter rate_limiter(LOG_RATE_LIMIT_1S);
                    thread_local int tl_error_count_since_last_log = 0;
                    tl_error_count_since_last_log++;
                    if (rate_limiter.shouldLog()) {
                        const char* diagnostic =
                            getWcErrorDiagnostic(wc[i].status);
                        LOG(ERROR)
                            << "WC error: " << ibv_wc_status_str(wc[i].status)
                            << " | " << diagnostic
                            << " | count=" << tl_error_count_since_last_log
                            << ", opcode=" << slice->task->request.opcode
                            << ", length=" << slice->task->request.length
                            << ", nic=" << context->name()
                            << ", qp_num=" << wc[i].qp_num
                            << ", target_id=" << slice->task->request.target_id;
                        tl_error_count_since_last_log = 0;
                    }
                }
                slice->retry_count++;
                if (slice->retry_count >=
                    transport_->params_->workers.max_retry_count) {
                    thread_local LogRateLimiter retry_limiter(
                        LOG_RATE_LIMIT_2S);
                    if (retry_limiter.shouldLog()) {
                        LOG(WARNING)
                            << "Slice retry count exceeded: "
                            << transport_->params_->workers.max_retry_count;
                    }
                    num_slices += ep->acknowledge(slice, FAILED);
                    disableEndpoint(slice);
                } else {
                    num_slices += ep->acknowledge(slice, PENDING);
                    disableEndpoint(slice);
                    submit(slice);
                }
            } else {
                num_slices += ep->acknowledge(slice, COMPLETED);
                worker.perf.inflight_lat.add(inflight_lat);
                worker.perf.enqueue_lat.add(enqueue_lat);
            }
        }
    }
    if (num_slices) {
        worker.inflight_slices.fetch_sub(num_slices);
    }
}

void Workers::showLatencyInfo() {
    auto& worker = worker_context_[tl_wid];
    LOG(INFO) << "[W" << tl_wid << "] enqueue count "
              << worker.perf.enqueue_lat.count() << " avg "
              << worker.perf.enqueue_lat.avg() << " p99 "
              << worker.perf.enqueue_lat.p99() << " p999 "
              << worker.perf.enqueue_lat.p999();
    LOG(INFO) << "[W" << tl_wid << "] submit count "
              << worker.perf.inflight_lat.count() << " avg "
              << worker.perf.inflight_lat.avg() << " p99 "
              << worker.perf.inflight_lat.p99() << " p999 "
              << worker.perf.inflight_lat.p999();
    worker.perf.enqueue_lat.clear();
    worker.perf.inflight_lat.clear();
}

void Workers::workerThread(int thread_id) {
    bindToSocket(thread_id % numa_num_configured_nodes());
    tl_wid = thread_id;
    auto& worker = worker_context_[thread_id];

    uint64_t grace_ts = 0;
    uint64_t last_perf_logging_ts = 0;
    while (running_) {
        auto current_ts = getCurrentTimeInNano();
        auto inflight_slices =
            worker.inflight_slices.load(std::memory_order_relaxed);
        if (inflight_slices ||
            current_ts - grace_ts <
                transport_->params_->workers.grace_period_ns) {
            asyncPostSend();
            asyncPollCq();
            if (inflight_slices) grace_ts = current_ts;
            const static uint64_t ONE_SECOND = 1000000000;
            if (transport_->params_->workers.show_latency_info &&
                current_ts - last_perf_logging_ts > ONE_SECOND) {
                showLatencyInfo();
                last_perf_logging_ts = current_ts;
            }
        } else {
            std::unique_lock<std::mutex> lock(worker.mutex);
            worker.in_suspend = true;
            worker.cv.wait(lock, [&]() -> bool {
                return !running_ || worker.inflight_slices.load(
                                        std::memory_order_acquire) > 0;
            });
            worker.in_suspend = false;
        }
    }
}

int Workers::handleContextEvents(std::shared_ptr<RdmaContext>& context) {
    ibv_async_event event;
    if (ibv_get_async_event(context->nativeContext(), &event) < 0) return -1;
    LOG(WARNING) << "Received context async event "
                 << ibv_event_type_str(event.event_type) << " for context "
                 << context->name();
    if (event.event_type == IBV_EVENT_QP_FATAL ||
        event.event_type == IBV_EVENT_WQ_FATAL) {
        auto endpoint = (RdmaEndPoint*)event.element.qp->qp_context;
        context->endpointStore()->remove(endpoint);
    } else if (event.event_type == IBV_EVENT_CQ_ERR) {
        context->pause();
        context->resume();
        LOG(WARNING) << "Action: " << context->name() << " restarted";
    } else if (event.event_type == IBV_EVENT_DEVICE_FATAL ||
               event.event_type == IBV_EVENT_PORT_ERR) {
        context->pause();
        LOG(WARNING) << "Action: " << context->name() << " down";
    } else if (event.event_type == IBV_EVENT_PORT_ACTIVE) {
        context->resume();
        LOG(WARNING) << "Action: " << context->name() << " up";
    }
    ibv_ack_async_event(&event);
    return 0;
}

void Workers::monitorThread() {
    while (running_) {
        for (auto& context : transport_->context_set_) {
            struct epoll_event event;
            if (context->eventFd() < 0) continue;
            int num_events = epoll_wait(context->eventFd(), &event, 1, 100);
            if (num_events < 0) {
                PLOG(ERROR) << "epoll_wait()";
                continue;
            }
            if (num_events == 0) continue;
            if (!(event.events & EPOLLIN)) continue;
            if (event.data.fd == context->nativeContext()->async_fd)
                handleContextEvents(context);
        }
    }
}

Status Workers::getRouteHint(RouteHint& hint, SegmentID segment_id,
                             uint64_t addr, uint64_t length) {
    auto& segment_manager = transport_->metadata_->segmentManager();
    if (segment_id == LOCAL_SEGMENT_ID) {
        hint.segment = segment_manager.getLocal().get();
    } else {
        CHECK_STATUS(segment_manager.getRemoteCached(hint.segment, segment_id));
    }
    hint.buffer = hint.segment->findBuffer(addr, length);
    if (!hint.buffer) {
        return Status::AddressNotRegistered(
            "No matched buffer in given address range" LOC_MARK);
    }
    if (hint.segment->type != SegmentType::Memory)
        return Status::AddressNotRegistered("Segment type not memory" LOC_MARK);
    hint.topo = &std::get<MemorySegmentDesc>(hint.segment->detail).topology;
    std::string location = hint.buffer->location;
    if (!hint.buffer->regions.empty()) {
        size_t offset = hint.buffer->addr;
        size_t best_overlap = 0;
        size_t target_start = addr;
        size_t target_end = addr + length;
        for (auto& entry : hint.buffer->regions) {
            size_t region_start = offset;
            size_t region_end = offset + entry.size;
            size_t overlap_start = std::max(region_start, target_start);
            size_t overlap_end = std::min(region_end, target_end);
            size_t overlap = (overlap_end > overlap_start)
                                 ? (overlap_end - overlap_start)
                                 : 0;
            if (overlap > best_overlap) {
                best_overlap = overlap;
                location = entry.location;
            }
            offset += entry.size;
        }
    }
    auto mem_id = hint.topo->getMemId(location);
    if (mem_id < 0) mem_id = hint.topo->getMemId(kWildcardLocation);
    hint.topo_entry = hint.topo->getMemEntry(mem_id);
    return Status::OK();
}

int Workers::getDeviceRank(const RouteHint& hint, int device_id) {
    for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
        auto& list = hint.topo_entry->device_list[rank];
        for (auto& entry : list)
            if (entry == device_id) return rank;
    }
    return -1;
}

Status Workers::selectOptimalDevice(RouteHint& source, RouteHint& target,
                                    RdmaSlice* slice) {
    auto& worker = worker_context_[tl_wid];
    if (slice->source_dev_id < 0) {
        CHECK_STATUS(device_quota_->allocate(
            slice->length, source.buffer->location, slice->source_dev_id));
    }

    if (slice->source_dev_id < 0)
        return Status::DeviceNotFound(
            "No device could access the slice memory region" LOC_MARK);

    auto& rail = worker.rails[target.segment->machine_id];
    if (!rail.ready() || target.topo != rail.remote())
        rail.load(source.topo, target.topo);
    if (slice->target_dev_id < 0) {
        int mapped_dev_id = rail.findBestRemoteDevice(
            slice->source_dev_id, target.topo_entry->numa_node);
        for (size_t rank = 0; rank < Topology::DevicePriorityRanks - 1;
             ++rank) {
            if (rank && always_tier1_) break;
            const auto& list = target.topo_entry->device_list[rank];
            if (list.empty()) continue;
            if (std::find(list.begin(), list.end(), mapped_dev_id) !=
                list.end()) {
                slice->target_dev_id = mapped_dev_id;
                break;
            }
        }
    }

    if (slice->target_dev_id < 0) {
        for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
            const auto& list = target.topo_entry->device_list[rank];
            if (list.empty()) continue;
            slice->target_dev_id = list[SimpleRandom::Get().next(list.size())];
            break;
        }
    }
    /*
    if (slice->target_dev_id < 0) {
        int mapped_dev_id = rail.findBestRemoteDevice(
            slice->source_dev_id, target.topo_entry->numa_node);
        for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
            auto &list = target.topo_entry->device_list[rank];
            if (list.empty()) continue;
            auto it = std::find(list.begin(), list.end(), mapped_dev_id);
            if (it != list.end()) {
                slice->target_dev_id = mapped_dev_id;
                break;
            }
            slice->target_dev_id = list[SimpleRandom::Get().next(list.size())];
            break;
        }
    }
    */

    if (slice->target_dev_id < 0)
        return Status::DeviceNotFound(
            "No device could access the slice memory region" LOC_MARK);

    if (!rail.available(slice->source_dev_id, slice->target_dev_id)) {
        LOG(INFO) << "Optimal device pair not available: source_dev_id "
                  << slice->source_dev_id << ", target_dev_id "
                  << slice->target_dev_id;
        return selectFallbackDevice(source, target, slice);
    }

    return Status::OK();
}

int Workers::getDeviceByFlatIndex(const RouteHint& hint, size_t flat_idx) {
    for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
        auto& list = hint.topo_entry->device_list[rank];
        if (flat_idx < list.size()) return list[flat_idx];
        flat_idx -= list.size();
    }
    return -1;
}

Status Workers::selectFallbackDevice(RouteHint& source, RouteHint& target,
                                     RdmaSlice* slice) {
    // Rate limit fallback logging
    thread_local LogRateLimiter rate_limiter(LOG_RATE_LIMIT_5S);
    if (rate_limiter.shouldLog()) {
        LOG(INFO) << "Using fallback device selection for slice";
    }
    bool same_machine =
        (source.segment->machine_id == target.segment->machine_id);

    size_t src_total = 0;
    for (size_t srank = 0; srank < Topology::DevicePriorityRanks; ++srank)
        src_total += source.topo_entry->device_list[srank].size();

    size_t dst_total = 0;
    for (size_t trank = 0; trank < Topology::DevicePriorityRanks; ++trank)
        dst_total += target.topo_entry->device_list[trank].size();

    size_t total_combos = src_total * dst_total;
    if ((size_t)slice->retry_count >= total_combos)
        return Status::DeviceNotFound("No available path" LOC_MARK);

    size_t idx = slice->retry_count;
    while (idx < total_combos) {
        size_t src_idx = idx / dst_total;
        size_t dst_idx = idx % dst_total;
        int sdev = getDeviceByFlatIndex(source, src_idx);
        int tdev = getDeviceByFlatIndex(target, dst_idx);
        bool reachable = true;

        if (same_machine) {
            reachable = (sdev == tdev);  // loopback is safe
        } else {
            auto& worker = worker_context_[tl_wid];
            auto& rail = worker.rails[target.segment->machine_id];
            reachable = rail.available(sdev, tdev);
        }

        if (reachable) {
            slice->source_dev_id = sdev;
            slice->target_dev_id = tdev;
            slice->source_lkey = source.buffer->lkey[slice->source_dev_id];
            slice->target_rkey = target.buffer->rkey[slice->target_dev_id];
            return Status::OK();
        }

        ++idx;
    }

    return Status::DeviceNotFound("No available path" LOC_MARK);
}

Status Workers::generatePostPath(RdmaSlice* slice) {
    RouteHint source, target;
    CHECK_STATUS(getRouteHint(source, LOCAL_SEGMENT_ID,
                              (uint64_t)slice->source_addr, slice->length));

    auto target_id = slice->task->request.target_id;
    CHECK_STATUS(getRouteHint(target, target_id, (uint64_t)slice->target_addr,
                              slice->length));

    if (slice->retry_count == 0)
        CHECK_STATUS(selectOptimalDevice(source, target, slice));
    else
        CHECK_STATUS(selectFallbackDevice(source, target, slice));
    slice->source_lkey = source.buffer->lkey[slice->source_dev_id];
    slice->target_rkey = target.buffer->rkey[slice->target_dev_id];
    return Status::OK();
}
}  // namespace tent
}  // namespace mooncake
