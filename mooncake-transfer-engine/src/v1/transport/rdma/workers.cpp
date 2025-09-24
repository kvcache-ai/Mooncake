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

#include "v1/transport/rdma/workers.h"

#include <sys/epoll.h>

#include <cassert>

#include "v1/transport/rdma/endpoint_store.h"
#include "v1/utility/ip.h"
#include "v1/utility/string_builder.h"
#include "v1/utility/system.h"
#include "v1/utility/random.h"

namespace mooncake {
namespace v1 {
thread_local int tl_wid = -1;
Workers::Workers(RdmaTransport *transport)
    : transport_(transport), num_workers_(0), running_(false) {}

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
    }
    return Status::OK();
}

Status Workers::stop() {
    if (!running_) return Status::OK();
    running_ = false;
    for (size_t id = 0; id < num_workers_; ++id) {
        auto &worker = worker_context_[id];
        {
            std::lock_guard<std::mutex> lock(worker.mutex);
            worker.cv.notify_all();
        }
        worker.thread.join();
    }
    monitor_.join();
    delete[] worker_context_;
    worker_context_ = nullptr;
    return Status::OK();
}

Status Workers::submit(RdmaSliceList &slice_list, int worker_id) {
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
    auto &worker = worker_context_[worker_id];
    worker.queue.push(slice_list);
    if (!worker.inflight_slices.fetch_add(slice_list.num_slices)) {
        std::lock_guard<std::mutex> lock(worker.mutex);
        if (worker.in_suspend) worker.cv.notify_all();
    }
    return Status::OK();
}

Status Workers::submit(RdmaSlice *slice) {
    RdmaSliceList slice_list;
    slice_list.first = slice;
    slice_list.num_slices = 1;
    return submit(slice_list);
}

Status Workers::cancel(RdmaSliceList &slice_list) {
    return Status::NotImplemented("cancel not implemented" LOC_MARK);
}

std::shared_ptr<RdmaEndPoint> Workers::getEndpoint(Workers::PostPath path) {
    std::string target_seg_name, target_dev_name;
    RouteHint hint;
    auto target_id = path.remote_segment_id;
    auto device_id = path.remote_device_id;
    auto &segment_manager = transport_->metadata_->segmentManager();
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
            LOG(ERROR) << "Unable to connect endpoint " << peer_name << ": "
                       << status.ToString();
            return nullptr;
        }
    }
    return endpoint;
}

void Workers::disableEndpoint(RdmaSlice *slice) {
    SegmentDesc *desc = nullptr;
    auto &segment_manager = transport_->metadata_->segmentManager();
    auto target_id = slice->task->request.target_id;
    if (target_id == LOCAL_SEGMENT_ID) {
        desc = segment_manager.getLocal().get();
    } else {
        auto status = segment_manager.getRemoteCached(desc, target_id);
        if (!status.ok()) {
            LOG(ERROR) << "Unable to mark rail failed, target_id " << target_id
                       << ": " << status.ToString();
        }
    }
    if (desc) {
        auto &worker = worker_context_[tl_wid];
        auto &rail = worker.rails[desc->machine_id];
        rail.markFailed(slice->source_dev_id, slice->target_dev_id);
    }
    if (slice->ep_weak_ptr) slice->ep_weak_ptr->reset();
}

void Workers::asyncPostSend() {
    auto &worker = worker_context_[tl_wid];
    std::vector<RdmaSliceList> result;
    worker.queue.pop(result);
    for (auto &slice_list : result) {
        if (slice_list.num_slices == 0) continue;
        auto slice = slice_list.first;
        for (int id = 0; id < slice_list.num_slices; ++id) {
            auto status = generatePostPath(slice);
            if (!status.ok()) {
                LOG(ERROR) << "Failed to generate post path for slice " << slice
                           << ": " << status.ToString();
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

    for (auto &entry : worker.requests) {
        auto &path = entry.first;
        auto &slices = entry.second;
        if (slices.empty()) continue;
        auto endpoint = getEndpoint(path);
        if (!endpoint) {
            for (auto slice : slices) {
                updateSliceStatus(slice, FAILED);
                disableEndpoint(slice);
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
                    LOG(WARNING)
                        << "Slice " << slice << " failed: retry count exceeded";
                    updateSliceStatus(slice, FAILED);
                    disableEndpoint(slice);
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
    auto &worker = worker_context_[tl_wid];
    const static size_t kPollCount = 64;
    int num_contexts = (int)transport_->context_set_.size();
    int num_cq_list = transport_->params_->device.num_cq_list;
    int num_slices = 0;

    uint64_t current_ts = getCurrentTimeInNano();
    std::vector<RdmaSlice *> slice_to_remove;
    for (auto &slice : worker.inflight_slice_set) {
        if (slice->word != PENDING) continue;
        if (current_ts - slice->enqueue_ts > slice_timeout_ns_) {
            auto ep = slice->ep_weak_ptr;
            LOG(WARNING) << "Slice " << slice
                         << " failed: transfer timeout (software)";
            auto num_slices = ep->acknowledge(slice, TIMEOUT);
            disableEndpoint(slice);
            worker.inflight_slices.fetch_sub(num_slices);
            slice_to_remove.push_back(slice);
        }
    }
    for (auto &slice : slice_to_remove) worker.inflight_slice_set.erase(slice);

    for (int index = 0; index < num_contexts; index++) {
        auto &context = transport_->context_set_[index];
        auto cq = context->cq(tl_wid % num_cq_list);
        ibv_wc wc[kPollCount];
        int nr_poll = cq->poll(kPollCount, wc);
        if (nr_poll < 0) continue;
        auto poll_ts = getCurrentTimeInNano();
        for (int i = 0; i < nr_poll; ++i) {
            auto slice = (RdmaSlice *)wc[i].wr_id;
            worker.inflight_slice_set.erase(slice);
            auto ep = slice->ep_weak_ptr;
            double enqueue_lat =
                (slice->submit_ts - slice->enqueue_ts) / 1000.0;
            double inflight_lat = (poll_ts - slice->submit_ts) / 1000.0;
            if (slice->retry_count == 0) {
                worker.device_quota->release(slice->source_dev_id,
                                             slice->length, inflight_lat);
            }
            if (slice->word != PENDING) continue;
            if (wc[i].status != IBV_WC_SUCCESS) {
                if (wc[i].status != IBV_WC_WR_FLUSH_ERR) {
                    // TE handles them automatically
                    LOG(INFO) << "Detected error WQE for slice " << slice
                              << " (opcode: " << slice->task->request.opcode
                              << ", source_addr: " << (void *)slice->source_addr
                              << ", dest_addr: " << (void *)slice->target_addr
                              << ", length: " << slice->length
                              << ", local_nic: " << context->name()
                              << "): " << ibv_wc_status_str(wc[i].status);
                }
                slice->retry_count++;
                if (slice->retry_count >=
                    transport_->params_->workers.max_retry_count) {
                    LOG(WARNING)
                        << "Slice " << slice << " failed: retry count exceeded";
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
    auto &worker = worker_context_[tl_wid];
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
    auto &worker = worker_context_[thread_id];
    worker.device_quota = std::make_unique<DeviceQuota>();
    worker.device_quota->loadTopology(transport_->local_topology_);

    auto shared_quota_shm_path =
        transport_->conf_->get("transports/rdma/shared_quota_shm_path", "");
    if (!shared_quota_shm_path.empty())
        worker.device_quota->enableSharedQuota(shared_quota_shm_path);

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

int Workers::handleContextEvents(std::shared_ptr<RdmaContext> &context) {
    ibv_async_event event;
    if (ibv_get_async_event(context->nativeContext(), &event) < 0) return -1;
    LOG(WARNING) << "Received context async event "
                 << ibv_event_type_str(event.event_type) << " for context "
                 << context->name();
    if (event.event_type == IBV_EVENT_QP_FATAL ||
        event.event_type == IBV_EVENT_WQ_FATAL) {
        auto endpoint = (RdmaEndPoint *)event.element.qp->qp_context;
        context->endpointStore()->remove(endpoint);
    } else if (event.event_type == IBV_EVENT_CQ_ERR) {
        context->disable();
        context->enable();
        LOG(WARNING) << "Action: " << context->name() << " restarted";
    } else if (event.event_type == IBV_EVENT_DEVICE_FATAL ||
               event.event_type == IBV_EVENT_PORT_ERR) {
        context->disable();
        LOG(WARNING) << "Action: " << context->name() << " down";
    } else if (event.event_type == IBV_EVENT_PORT_ACTIVE) {
        context->enable();
        LOG(WARNING) << "Action: " << context->name() << " up";
    }
    ibv_ack_async_event(&event);
    return 0;
}

void Workers::monitorThread() {
    while (running_) {
        for (auto &context : transport_->context_set_) {
            struct epoll_event event;
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

Status Workers::getRouteHint(RouteHint &hint, SegmentID segment_id,
                             uint64_t addr, uint64_t length) {
    auto &segment_manager = transport_->metadata_->segmentManager();
    if (segment_id == LOCAL_SEGMENT_ID) {
        hint.segment = segment_manager.getLocal().get();
    } else {
        CHECK_STATUS(segment_manager.getRemoteCached(hint.segment, segment_id));
    }
    hint.buffer = getBufferDesc(hint.segment, addr, length);
    if (!hint.buffer) {
        return Status::AddressNotRegistered(
            "No matched buffer in given address range" LOC_MARK);
    }
    if (hint.segment->type != SegmentType::Memory)
        return Status::AddressNotRegistered("Segment type not memory" LOC_MARK);
    hint.topo = &std::get<MemorySegmentDesc>(hint.segment->detail).topology;
    auto &matrix = hint.topo->getResolvedMatrix();
    auto &raw_matrix = hint.topo->getMatrix();
    if (!matrix.count(hint.buffer->location)) {
        hint.topo_entry = &matrix.at(kWildcardLocation);
        hint.topo_entry_raw = &raw_matrix.at(kWildcardLocation);
    } else {
        hint.topo_entry = &matrix.at(hint.buffer->location);
        hint.topo_entry_raw = &raw_matrix.at(hint.buffer->location);
    }
    return Status::OK();
}

int Workers::getDeviceRank(const RouteHint &hint, int device_id) {
    for (size_t rank = 0; rank < DevicePriorityRanks; ++rank) {
        auto &list = hint.topo_entry->device_list[rank];
        for (auto &entry : list)
            if (entry == device_id) return rank;
    }
    return -1;
}

Status Workers::selectOptimalDevice(RouteHint &source, RouteHint &target,
                                    RdmaSlice *slice) {
    auto &worker = worker_context_[tl_wid];
    if (slice->source_dev_id < 0)
        CHECK_STATUS(worker.device_quota->allocate(
            slice->length, source.buffer->location, slice->source_dev_id));

    if (slice->source_dev_id < 0)
        return Status::DeviceNotFound(
            "No device could access the slice memory region" LOC_MARK);

    bool same_machine =
        (source.segment->machine_id == target.segment->machine_id);
    if (!same_machine && slice->target_dev_id < 0) {
        auto &rail = worker.rails[target.segment->machine_id];
        if (!rail.ready()) rail.load(source.topo, target.topo);
        int mapped_dev_id = rail.findBestRemoteDevice(
            slice->source_dev_id, target.topo_entry->numa_node);
        for (size_t rank = 0; rank < DevicePriorityRanks; ++rank) {
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
    } else if (slice->target_dev_id < 0) {
        slice->target_dev_id = slice->source_dev_id;
    }

    if (slice->target_dev_id < 0)
        return Status::DeviceNotFound(
            "No device could access the slice memory region" LOC_MARK);

    return Status::OK();
}

int Workers::getDeviceByFlatIndex(const RouteHint &hint, size_t flat_idx) {
    for (size_t rank = 0; rank < DevicePriorityRanks; ++rank) {
        auto &list = hint.topo_entry->device_list[rank];
        if (flat_idx < list.size()) return list[flat_idx];
        flat_idx -= list.size();
    }
    return -1;
}

Status Workers::selectFallbackDevice(RouteHint &source, RouteHint &target,
                                     RdmaSlice *slice) {
    bool same_machine =
        (source.segment->machine_id == target.segment->machine_id);

    size_t src_total = 0;
    for (size_t srank = 0; srank < DevicePriorityRanks; ++srank)
        src_total += source.topo_entry->device_list[srank].size();

    size_t dst_total = 0;
    for (size_t trank = 0; trank < DevicePriorityRanks; ++trank)
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
            auto &worker = worker_context_[tl_wid];
            auto &rail = worker.rails[target.segment->machine_id];
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

Status Workers::generatePostPath(RdmaSlice *slice) {
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
}  // namespace v1
}  // namespace mooncake