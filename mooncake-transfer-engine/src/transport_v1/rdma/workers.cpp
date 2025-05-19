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

#include "transport_v1/rdma/workers.h"

#include <sys/epoll.h>

#include <cassert>

#include "transport_v1/rdma/endpoint_store.h"
namespace mooncake {
namespace v1 {

static inline void markSliceSuccess(RdmaSlice *slice) {
    auto task = slice->task;
    __sync_fetch_and_add(&task->status.transferred_bytes, slice->length);
    auto finish_slices = __sync_fetch_and_add(&task->finish_slices, 1);
    if (finish_slices + 1 == task->num_slices) {
        task->status.s = Transport::COMPLETED;
    }
    // RdmaSliceStorage::Get().deallocate(slice);
}

static inline void markSliceFailed(RdmaSlice *slice) {
    auto task = slice->task;
    __sync_fetch_and_add(&task->finish_slices, 1);
    task->status.s = Transport::FAILED;
    // RdmaSliceStorage::Get().deallocate(slice);
}

Workers::Workers(RdmaTransport *transport)
    : transport_(transport),
      num_workers_(0),
      inflight_slices_(0),
      running_(false) {}

Workers::~Workers() {
    if (running_) stop();
}

int Workers::start() {
    if (!running_) {
        running_ = true;
        stop_flag_ = false;
        monitor_ = std::thread([this] { monitorThread(); });
        num_workers_ = transport_->params_->workers.num_workers;
        worker_context_ = new WorkerContext[num_workers_];
        for (size_t id = 0; id < num_workers_; ++id) {
            worker_context_[id].thread =
                std::thread([this, id] { workerThread(id); });
        }
    }
    return 0;
}

int Workers::stop() {
    if (!running_) return 0;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        stop_flag_ = true;
    }
    cv_.notify_all();
    for (size_t id = 0; id < num_workers_; ++id) {
        worker_context_[id].thread.join();
    }
    monitor_.join();
    delete[] worker_context_;
    worker_context_ = nullptr;
    running_ = false;
    return 0;
}

static std::atomic<int> gNextThreadId(0);
thread_local int tlThreadId =
    gNextThreadId.fetch_add(1, std::memory_order_relaxed);

int Workers::submit(RdmaSliceList &slice_list) {
    int id = tlThreadId % kNumSliceQueue;
    slice_queue_[id].push(slice_list);
    auto prev_inflight_slices =
        inflight_slices_.fetch_add(slice_list.num_slices);
    if (!prev_inflight_slices) cv_.notify_all();
    return 0;
}

int Workers::submit(RdmaSlice *slice) {
    RdmaSliceList slice_list;
    slice_list.first = slice;
    slice_list.num_slices = 1;
    return submit(slice_list);
}

int Workers::cancel(RdmaSliceList &slice_list) { return ERR_NOT_IMPLEMENTED; }

void Workers::dispatchSendRequests(int thread_id) {
    const static size_t kMaxSlicesToSend = 128;
    const static int kMaxProbeCount = 8;
    std::vector<RdmaSlice *> slice_list;

    auto &remote_buffer = worker_context_[thread_id].remote_buffer;
    auto &requests = worker_context_[thread_id].requests;
    auto &queue_id = worker_context_[thread_id].queue_id;

    int probe_count = 0;
    if (queue_id < 0) queue_id = thread_id;
    while (slice_list.size() < kMaxSlicesToSend &&
           probe_count < kMaxProbeCount) {
        slice_queue_[queue_id].pop(kMaxSlicesToSend, slice_list);
        queue_id += num_workers_;
        if (queue_id >= kNumSliceQueue) queue_id = thread_id;
        probe_count++;
    }

    for (auto slice : slice_list) {
        auto target_id = slice->task->request.target_id;
        std::vector<BufferQueryResult> local, remote;
        int ret = transport_->local_buffer_manager_.query(
            AddressRange{slice->source_addr, slice->length}, local,
            slice->retry_count);
        if (ret) {
            markSliceFailed(slice);
            continue;
        }

        if (!remote_buffer.valid(target_id)) {
            auto desc =
                transport_->metadata_manager_->getSegmentDescByID(target_id);
            if (!desc) {
                markSliceFailed(slice);
                continue;
            }
            remote_buffer.reload(target_id, desc);
        }

        ret = remote_buffer.query(
            target_id, AddressRange{(void *)slice->target_addr, slice->length},
            remote, slice->retry_count);
        if (ret) {
            markSliceFailed(slice);
            continue;
        }

        assert(local.size() == 1 && remote.size() == 1);
        auto path = PostPath{.local_device_id = local[0].device_id,
                             .remote_segment_id = target_id,
                             .remote_device_id = remote[0].device_id};
        slice->source_lkey = local[0].lkey;
        slice->target_rkey = remote[0].rkey;
        requests[path].slices.push_back(slice);
    }
}

void Workers::asyncPostSend(int thread_id) {
    auto &remote_buffer = worker_context_[thread_id].remote_buffer;
    auto &requests = worker_context_[thread_id].requests;
    dispatchSendRequests(thread_id);

    for (auto &entry : requests) {
        auto &path = entry.first;
        auto &endpoint = entry.second.endpoint;
        auto &slices = entry.second.slices;
        if (!endpoint) {
            auto context = transport_->context_set_[path.local_device_id].get();
            auto peer_name = std::to_string(path.local_device_id) + "/" +
                             std::to_string(path.remote_segment_id) + "/" +
                             std::to_string(path.remote_device_id);
            endpoint = context->endpoint(peer_name);
            if (endpoint->status() != RdmaEndPoint::EP_READY) {
                mutex_.lock();
                if (endpoint->status() != RdmaEndPoint::EP_READY) {
                    auto peer_segment_name =
                        remote_buffer.segmentName(path.remote_segment_id);
                    auto peer_device_name = remote_buffer.deviceName(
                        path.remote_segment_id, path.remote_device_id);
                    if (doHandshake(endpoint, peer_segment_name,
                                    peer_device_name)) {
                        for (auto slice : slices) {
                            __sync_fetch_and_add(&slice->task->finish_slices,
                                                 1);
                            slice->task->status.s = Transport::FAILED;
                        }
                    }
                }
                mutex_.unlock();
            }
        }

        if (slices.empty()) continue;
        int num_submitted = endpoint->submitSlices(slices);
        for (int id = 0; id < num_submitted; ++id) {
            auto slice = slices[id];
            if (slice->failed) {
                slice->retry_count++;
                if (slice->retry_count >=
                    transport_->params_->workers.max_retry_count) {
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
    // TODO handling loopback handshake
    HandShakeDesc local_desc, peer_desc;
    local_desc.local_nic_path = MakeNicPath(transport_->local_segment_name_,
                                            endpoint->context().name());
    local_desc.peer_nic_path = MakeNicPath(peer_server_name, peer_nic_name);
    local_desc.qp_num = endpoint->qpNum();
    int rc = transport_->metadata_manager_->sendHandshake(
        peer_server_name, local_desc, peer_desc);
    if (rc) return rc;
    assert(peer_desc.qp_num.size());

    auto segment_desc =
        transport_->metadata_manager_->getSegmentDescByName(peer_server_name);
    if (segment_desc) {
        auto &detail = std::get<MemorySegmentDesc>(segment_desc->detail);
        for (auto &nic : detail.devices)
            if (nic.name == peer_nic_name) {
                return endpoint->configurePeer(nic.gid, nic.lid,
                                               peer_desc.qp_num);
            }
    }

    return ERR_DEVICE_NOT_FOUND;
}

void Workers::asyncPollCq(int thread_id) {
    const static size_t kPollCount = 64;
    int num_contexts = (int)transport_->context_set_.size();
    int num_cq_list = transport_->params_->device.num_cq_list;
    for (int index = thread_id; index < num_contexts * num_cq_list;
         index += num_workers_) {
        auto &context = transport_->context_set_[index % num_contexts];
        auto cq = context->cq(index / num_contexts);
        ibv_wc wc[kPollCount];
        int nr_poll = cq->poll(kPollCount, wc);
        if (nr_poll < 0) {
            LOG(ERROR) << "Worker: Failed to poll completion queue";
            continue;
        }
        for (int i = 0; i < nr_poll; ++i) {
            auto slice = (RdmaSlice *)wc[i].wr_id;
            __sync_fetch_and_sub(slice->endpoint_quota, 1);
            if (wc[i].status != IBV_WC_SUCCESS) {
                if (wc[i].status != IBV_WC_WR_FLUSH_ERR) {
                    LOG(ERROR)
                        << "Worker: Process failed for slice (opcode: "
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
                    markSliceFailed(slice);
                } else {
                    submit(slice);
                }
            } else {
                markSliceSuccess(slice);
            }
        }
        if (nr_poll > 0) {
            inflight_slices_.fetch_sub(nr_poll);
        }
    }
}

void Workers::workerThread(int thread_id) {
    while (!stop_flag_) {
        bool executed = true;
        if (inflight_slices_ == 0) {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait_for(lock, std::chrono::microseconds(100), [this]() {
                return inflight_slices_ > 0 || stop_flag_;
            });
            if (stop_flag_) {
                break;
            }
            executed = (inflight_slices_ > 0);
        }
        if (executed) {
            asyncPostSend(thread_id);
            asyncPollCq(thread_id);
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
    while (true) {
        for (auto &context : transport_->context_set_) {
            struct epoll_event event;
            if (stop_flag_) return;
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
}  // namespace v1
}  // namespace mooncake