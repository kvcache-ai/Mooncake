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

static inline void markSliceSuccess(RdmaSlice *slice) {
    auto task = slice->task;
    __sync_fetch_and_add(&task->transferred_bytes, slice->length);
    auto success_slices = __sync_fetch_and_add(&task->success_slices, 1);
    if (success_slices + 1 == task->num_slices) {
        task->status_word = COMPLETED;
    }
}

static inline void markSliceFailed(RdmaSlice *slice) {
    auto task = slice->task;
    __sync_fetch_and_add(&task->failed_slices, 1);
    task->status_word = FAILED;
}

Workers::Workers(RdmaTransport *transport)
    : transport_(transport), num_workers_(0), running_(false) {}

Workers::~Workers() {
    if (running_) stop();
}

Status Workers::start() {
    if (!running_) {
        running_ = true;
        monitor_ = std::thread([this] { monitorThread(); });
        num_workers_ = transport_->params_->workers.num_workers;
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
        worker.notifyIfNeeded();
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
        worker.notifyIfNeeded();
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

std::shared_ptr<RdmaEndPoint> Workers::getEndpoint(int thread_id,
                                                   Workers::PostPath path) {
    auto context = transport_->context_set_[path.local_device_id].get();
    std::shared_ptr<RdmaEndPoint> endpoint;
    SegmentDesc *desc = nullptr;
    auto status = transport_->metadata_->segmentManager().getRemoteCached(
        desc, path.remote_segment_id);
    if (!status.ok() || desc->type != SegmentType::Memory) {
        LOG(ERROR) << "Failed to get remote segment: " << status.ToString();
        return nullptr;
    }
    auto peer_segment_name = desc->name;
    auto peer_device_name = std::get<MemorySegmentDesc>(desc->detail)
                                .devices[path.remote_device_id]
                                .name;
    auto peer_name = MakeNicPath(peer_segment_name, peer_device_name);
    endpoint = context->endpoint(peer_name);
    if (endpoint->status() != RdmaEndPoint::EP_READY) {
        std::lock_guard<std::mutex> lock(ep_mutex_);
        if (endpoint->status() != RdmaEndPoint::EP_READY &&
            doHandshake(endpoint, peer_segment_name, peer_device_name)) {
            return nullptr;
        }
    }
    return endpoint;
}

void Workers::asyncPostSend(int thread_id) {
    auto &worker = worker_context_[thread_id];
    std::vector<RdmaSliceList> result;
    worker.queue.pop(result);
    for (auto &slice_list : result) {
        if (slice_list.num_slices == 0) continue;
        auto slice = slice_list.first;
        for (int id = 0; id < slice_list.num_slices; ++id) {
            auto status = generatePostPath(thread_id, slice);
            if (!status.ok()) {
                LOG(ERROR) << "[TE Worker] Detected asynchronous error: "
                           << status.ToString();
                markSliceFailed(slice);
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

        auto endpoint = getEndpoint(thread_id, path);
        if (!endpoint) {
            LOG(ERROR) << "[TE Worker] Unable to allocate endpoint: "
                       << path.local_device_id << " -> "
                       << path.remote_segment_id << ":"
                       << path.remote_device_id;
            for (auto slice : slices) markSliceFailed(slice);
            continue;
        }

        int num_submitted = endpoint->submitSlices(slices, thread_id);
        for (int id = 0; id < num_submitted; ++id) {
            auto slice = slices[id];
            if (slice->failed) {
                slice->retry_count++;
                if (slice->retry_count >=
                    transport_->params_->workers.max_retry_count) {
                    LOG(ERROR) << "[TE Worker] Slice retry count exceeded";
                    markSliceFailed(slice);
                } else {
                    submit(slice);
                }
            } else {
                slice->submit_ts = getCurrentTimeInNano();
            }
        }

        if (num_submitted)
            slices.erase(slices.begin(), slices.begin() + num_submitted);
    }
}

int Workers::doHandshake(std::shared_ptr<RdmaEndPoint> &endpoint,
                         const std::string &peer_server_name,
                         const std::string &peer_nic_name) {
    auto qp_num = endpoint->qpNum();

    BootstrapDesc local_desc, peer_desc;
    local_desc.local_nic_path = MakeNicPath(transport_->local_segment_name_,
                                            endpoint->context().name());
    local_desc.peer_nic_path = MakeNicPath(peer_server_name, peer_nic_name);
    local_desc.qp_num = qp_num;

    std::shared_ptr<SegmentDesc> segment_desc;
    if (local_desc.local_nic_path == local_desc.peer_nic_path) {
        segment_desc = transport_->metadata_->segmentManager().getLocal();
    } else {
        auto &manager = transport_->metadata_->segmentManager();
        auto status = manager.getRemote(segment_desc, peer_server_name);
        if (!status.ok() || segment_desc->type != SegmentType::Memory)
            return ERR_ENDPOINT;
        auto &detail = std::get<MemorySegmentDesc>(segment_desc->detail);
        status =
            RpcClient::bootstrap(detail.rpc_server_addr, local_desc, peer_desc);
        if (!status.ok()) return ERR_ENDPOINT;

        qp_num = peer_desc.qp_num;
    }

    assert(qp_num.size());
    if (segment_desc) {
        auto &detail = std::get<MemorySegmentDesc>(segment_desc->detail);
        for (auto &nic : detail.devices) {
            if (nic.name == peer_nic_name) {
                return endpoint->configurePeer(nic.gid, nic.lid, qp_num);
            }
        }
    }

    return ERR_ENDPOINT;
}

void Workers::asyncPollCq(int thread_id) {
    const static size_t kPollCount = 64;
    int num_contexts = (int)transport_->context_set_.size();
    int num_cq_list = transport_->params_->device.num_cq_list;
    int nr_poll_total = 0;
    std::unordered_map<volatile int *, int> endpoint_quota_map;
    for (int index = thread_id; index < num_contexts * num_cq_list;
         index += num_workers_) {
        auto &context = transport_->context_set_[index / num_cq_list];
        auto cq = context->cq(index % num_cq_list);
        ibv_wc wc[kPollCount];
        int nr_poll = cq->poll(kPollCount, wc);
        if (nr_poll < 0) {
            LOG(ERROR) << "[TE Worker] Failed to poll completion queue";
            continue;
        }
        for (int i = 0; i < nr_poll; ++i) {
            auto slice = (RdmaSlice *)wc[i].wr_id;
            if (endpoint_quota_map.count(slice->endpoint_quota))
                endpoint_quota_map[slice->endpoint_quota]++;
            else
                endpoint_quota_map[slice->endpoint_quota] = 1;
            if (slice->retry_count == 0) {
                worker_context_[thread_id].device_quota->release(
                    slice->source_dev_id, slice->length);
            }
            if (wc[i].status != IBV_WC_SUCCESS) {
                if (wc[i].status != IBV_WC_WR_FLUSH_ERR) {
                    LOG(ERROR)
                        << "[TE Worker] Process failed for slice (opcode: "
                        << slice->task->request.opcode
                        << ", source_addr: " << (void *)slice->source_addr
                        << ", dest_addr: " << (void *)slice->target_addr
                        << ", length: " << slice->length
                        << ", local_nic: " << context->name()
                        << "): " << ibv_wc_status_str(wc[i].status);
                }
                slice->retry_count++;
                if (slice->retry_count >=
                    transport_->params_->workers.max_retry_count) {
                    LOG(ERROR) << "[TE Worker] Slice retry count exceeded";
                    markSliceFailed(slice);
                } else {
                    submit(slice);
                }
            } else {
                markSliceSuccess(slice);
                auto poll_ts = getCurrentTimeInNano();
                auto &worker = worker_context_[thread_id];
                worker.perf.inflight_lat.add((poll_ts - slice->submit_ts) /
                                             1000.0);
                worker.perf.enqueue_lat.add(
                    (slice->submit_ts - slice->enqueue_ts) / 1000.0);
            }
        }
        if (nr_poll > 0) {
            nr_poll_total += nr_poll;
        }
    }
    for (auto &entry : endpoint_quota_map) {
        __sync_fetch_and_sub(entry.first, entry.second);
    }
    if (nr_poll_total) {
        worker_context_[thread_id].inflight_slices.fetch_sub(nr_poll_total);
    }
}

void Workers::workerThread(int thread_id) {
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
            asyncPostSend(thread_id);
            asyncPollCq(thread_id);
            if (inflight_slices) grace_ts = current_ts;
            const static uint64_t ONE_SECOND = 1000000000;
            if (current_ts - last_perf_logging_ts > ONE_SECOND) {
                LOG(INFO) << "[W" << thread_id << "] enqueue count "
                          << worker.perf.enqueue_lat.count() << " avg "
                          << worker.perf.enqueue_lat.avg() << " p95 "
                          << worker.perf.enqueue_lat.p95() << " p99 "
                          << worker.perf.enqueue_lat.p99();
                LOG(INFO) << "[W" << thread_id << "] submit count "
                          << worker.perf.inflight_lat.count() << " avg "
                          << worker.perf.inflight_lat.avg() << " p95 "
                          << worker.perf.inflight_lat.p95() << " p99 "
                          << worker.perf.inflight_lat.p99();
                worker.perf.enqueue_lat.clear();
                worker.perf.inflight_lat.clear();
                last_perf_logging_ts = current_ts;
            }
        } else {
            std::unique_lock<std::mutex> lock(worker.mutex);
            if (worker.inflight_slices.load(std::memory_order_relaxed) ||
                !running_)
                continue;
            worker.in_suspend = true;
            worker.cv.wait_for(lock, std::chrono::milliseconds(1));
        }
    }
}

int Workers::handleContextEvents(std::shared_ptr<RdmaContext> &context) {
    ibv_async_event event;
    if (ibv_get_async_event(context->nativeContext(), &event) < 0)
        return ERR_CONTEXT;
    LOG(WARNING) << "Received context async event "
                 << ibv_event_type_str(event.event_type) << " for context "
                 << context->name();
    if (event.event_type == IBV_EVENT_DEVICE_FATAL ||
        event.event_type == IBV_EVENT_CQ_ERR ||
        event.event_type == IBV_EVENT_WQ_FATAL ||
        event.event_type == IBV_EVENT_PORT_ERR ||
        event.event_type == IBV_EVENT_LID_CHANGE) {
        // context->disable();
        LOG(INFO) << "Disabling context " << context->name();
    } else if (event.event_type == IBV_EVENT_PORT_ACTIVE) {
        // context->enable();
        LOG(INFO) << "Enable context " << context->name();
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
                PLOG(ERROR) << "Worker: epoll_wait()";
                continue;
            }
            if (num_events == 0) continue;
            if (!(event.events & EPOLLIN)) continue;
            if (event.data.fd == context->nativeContext()->async_fd)
                handleContextEvents(context);
        }
    }
}

Status Workers::getRouteHint(RuoteHint &hint, SegmentID segment_id,
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

int Workers::getDeviceRank(const RuoteHint &hint, int device_id) {
    for (size_t rank = 0; rank < DevicePriorityRanks; ++rank) {
        auto &list = hint.topo_entry->device_list[rank];
        for (auto &entry : list)
            if (entry == device_id) return rank;
    }
    return -1;
}

Status Workers::selectOptimalDevice(RuoteHint &source, RuoteHint &target,
                                    RdmaSlice *slice, int thread_id) {
    if (slice->source_dev_id < 0)
        CHECK_STATUS(worker_context_[thread_id].device_quota->allocate(
            slice->length, source.buffer->location, slice->source_dev_id));

    if (slice->source_dev_id < 0)
        return Status::DeviceNotFound(
            "No device could access the slice memory region");

    bool same_machine =
        (source.segment->machine_id == target.segment->machine_id);
    if (!same_machine && slice->target_dev_id < 0) {
        auto &ctx = worker_context_[thread_id];
        auto &instance = ctx.rail_topology_map_[target.segment->machine_id];
        if (!instance) {
            instance = std::make_shared<RailTopology>();
            instance->load(source.topo, target.topo,
                           transport_->params_->workers.rail_topo_path,
                           source.segment->machine_id,
                           target.segment->machine_id);
        }
        slice->target_dev_id = instance->findRemoteDeviceID(
            slice->source_dev_id, target.topo_entry->numa_node);
    } else {
        slice->target_dev_id = slice->source_dev_id;
    }

    if (slice->target_dev_id < 0)
        return Status::DeviceNotFound(
            "No device could access the slice memory region");

    return Status::OK();
}

int Workers::getDeviceByFlatIndex(const RuoteHint &hint, size_t flat_idx) {
    for (size_t rank = 0; rank < DevicePriorityRanks; ++rank) {
        auto &list = hint.topo_entry->device_list[rank];
        if (flat_idx < list.size()) return list[flat_idx];
        flat_idx -= list.size();
    }
    return -1;
}

Status Workers::selectFallbackDevice(RuoteHint &source, RuoteHint &target,
                                     RdmaSlice *slice, int thread_id) {
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
        return Status::DeviceNotFound("All devices are tried but failed");

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
            auto &ctx = worker_context_[thread_id];
            auto &instance = ctx.rail_topology_map_[target.segment->machine_id];
            if (!instance) {
                instance = std::make_shared<RailTopology>();
                instance->load(source.topo, target.topo,
                               transport_->params_->workers.rail_topo_path,
                               source.segment->machine_id,
                               target.segment->machine_id);
            }
            reachable = instance->connected(sdev, tdev);
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

    return Status::DeviceNotFound("All devices are tried but failed");
}

Status Workers::generatePostPath(int thread_id, RdmaSlice *slice) {
    RuoteHint source, target;
    CHECK_STATUS(getRouteHint(source, LOCAL_SEGMENT_ID,
                              (uint64_t)slice->source_addr, slice->length));

    auto target_id = slice->task->request.target_id;
    CHECK_STATUS(getRouteHint(target, target_id, (uint64_t)slice->target_addr,
                              slice->length));

    if (slice->retry_count == 0)
        CHECK_STATUS(selectOptimalDevice(source, target, slice, thread_id));
    else
        CHECK_STATUS(selectFallbackDevice(source, target, slice, thread_id));
    slice->source_lkey = source.buffer->lkey[slice->source_dev_id];
    slice->target_rkey = target.buffer->rkey[slice->target_dev_id];
    return Status::OK();
}
}  // namespace v1
}  // namespace mooncake