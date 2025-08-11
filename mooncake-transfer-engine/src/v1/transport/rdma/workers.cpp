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

static std::atomic<int> g_next_tid(0);
thread_local int tl_tid = g_next_tid.fetch_add(1);

Status Workers::submit(RdmaSliceList &slice_list) {
    auto &worker = worker_context_[tl_tid % num_workers_];
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
    uint64_t grace_ts = 0;
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

Status Workers::generatePostPath(int thread_id, RdmaSlice *slice) {
    auto target_id = slice->task->request.target_id;
#ifndef LEGACY_PATH_SELECTION
    auto &segment_manager = transport_->metadata_->segmentManager();

    auto local_segment_desc = segment_manager.getLocal().get();
    auto local_buffer_desc = getBufferDesc(
        local_segment_desc, (uint64_t)slice->source_addr, slice->length);
    if (!local_buffer_desc) {
        return Status::AddressNotRegistered(
            "No matched buffer in given address range" LOC_MARK);
    }

    int device_id;
    int rand_seed = -1;
    CHECK_STATUS(transport_->local_topology_->selectDevice(
        device_id, local_buffer_desc->location, slice->retry_count, rand_seed));
    slice->source_dev_id = device_id;
    slice->source_lkey = local_buffer_desc->lkey[device_id];
    if (target_id == LOCAL_SEGMENT_ID) {
        slice->target_dev_id = device_id;
        slice->target_rkey = local_buffer_desc->rkey[device_id];
        return Status::OK();
    }

    SegmentDesc *target_segment_desc = nullptr;
    CHECK_STATUS(
        segment_manager.getRemoteCached(target_segment_desc, target_id));
    auto &target_topology =
        std::get<MemorySegmentDesc>(target_segment_desc->detail).topology;
    auto target_buffer_desc = getBufferDesc(
        target_segment_desc, (uint64_t)slice->target_addr, slice->length);
    if (!target_buffer_desc) {
        return Status::AddressNotRegistered(
            "No matched buffer in given address range" LOC_MARK);
    }

    CHECK_STATUS(target_topology.selectDevice(device_id,
                                              target_buffer_desc->location,
                                              slice->retry_count, rand_seed));
    slice->target_dev_id = device_id;
    slice->target_rkey = target_buffer_desc->rkey[slice->target_dev_id];
    return Status::OK();
#else
    std::vector<BufferQueryResult> local, remote;
    auto status = transport_->local_buffer_manager_.query(
        AddressRange{slice->source_addr, slice->length}, local,
        slice->retry_count);
    if (!status.ok()) return status;
    if (target_id == LOCAL_SEGMENT_ID) {
        status = transport_->local_buffer_manager_.query(
            AddressRange{(void *)slice->target_addr, slice->length}, remote,
            slice->retry_count);
        if (!status.ok()) return status;
    } else {
        SegmentDesc *desc = nullptr;
        status = transport_->metadata_->segmentManager().getRemoteCached(
            desc, target_id);
        if (!status.ok()) return status;
        status = queryRemoteSegment(
            desc, AddressRange{(void *)slice->target_addr, slice->length},
            remote, slice->retry_count);
        if (!status.ok()) return status;
    }
    assert(local.size() == 1 && remote.size() == 1);
    slice->source_lkey = local[0].lkey;
    slice->target_rkey = remote[0].rkey;
    slice->source_dev_id = local[0].device_id;
    slice->target_dev_id = remote[0].device_id;
    return Status::OK();
#endif
}
}  // namespace v1
}  // namespace mooncake