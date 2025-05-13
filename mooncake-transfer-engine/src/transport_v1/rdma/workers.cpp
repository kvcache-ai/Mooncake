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
        num_workers_ = transport_->params_->workers.num_workers;
        slice_queue_ = new SliceQueue[num_workers_];
        for (size_t index = 0; index < num_workers_; ++index) {
            workers_.emplace_back([this, index] { workerThread(index); });
        }
        monitor_ = std::thread([this] { monitorThread(); });
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
    for (std::thread &worker : workers_) {
        worker.join();
    }
    monitor_.join();
    delete[] slice_queue_;
    slice_queue_ = nullptr;
    running_ = false;
    return 0;
}

void Workers::SliceQueue::push(RdmaSlice *first, RdmaSlice *last,
                               size_t count) {
    RWSpinlock::WriteGuard guard(lock);
    auto slice = first;
    for (size_t i = 0; i < count; ++i) {
        assert(slice);
        auto next = slice->next;
        slice->queue_next = (slice == last) ? nullptr : next;
        slice = next;
    }
    if (!num_entries) {
        head = first;
        tail = last;
    } else {
        assert(head && tail);
        tail->queue_next = first;
        tail = last;
    }
    num_entries.fetch_add(count);
}

RdmaSlice *Workers::SliceQueue::pop(size_t count) {
    if (!num_entries) return nullptr;
    RWSpinlock::WriteGuard guard(lock);
    auto slice = head;
    if (count >= num_entries) {
        slice = head;
        head = tail = nullptr;
        num_entries.store(0);
        return slice;
    }
    auto cursor = slice;
    for (size_t i = 0; i < count - 1; ++i) cursor = cursor->queue_next;
    head = cursor->queue_next;
    cursor->queue_next = nullptr;
    num_entries.fetch_sub(count);
    return slice;
}

int Workers::submit(RdmaSlice *slice) {
    int id = SimpleRandom::Get().next(num_workers_);
    slice_queue_[id].push(slice, slice, 1);
    auto prev_inflight_slices = inflight_slices_.fetch_add(1);
    if (!prev_inflight_slices) cv_.notify_all();
    return 0;
}

int Workers::submit(RdmaSliceList &slice_list) {
    int id = SimpleRandom::Get().next(num_workers_);
    slice_queue_[id].push(slice_list.first, slice_list.last,
                          slice_list.num_slices);
    auto prev_inflight_slices =
        inflight_slices_.fetch_add(slice_list.num_slices);
    if (!prev_inflight_slices) cv_.notify_all();
    return 0;
}

int Workers::cancel(RdmaSliceList &slice_list) { return ERR_NOT_IMPLEMENTED; }

void Workers::asyncPostSend(int thread_id) {
    const static size_t kMaxSlicesToSend = 32;
    
    PeerBuffers local_buffer_;
    std::unordered_map<SegmentID, PeerBuffers> remote_buffers_;

    auto slice = slice_queue_[thread_id].pop(kMaxSlicesToSend);
    if (!slice) return;

    RdmaSlice *slice_to_send[kMaxSlicesToSend];
    int count_slices = 0;
    while (slice) {
        slice_to_send[count_slices] = slice;
        count_slices++;
        slice = slice->queue_next;
    }

    if (!local_buffer_.valid()) {
        auto segment_desc = transport_->metadata_manager_->getSegmentDescByID(LOCAL_SEGMENT_ID);
        local_buffer_.reload(segment_desc);
    }
    for (int i = 0; i < count_slices; ++i) {
        auto &slice = slice_to_send[i];
        auto target_id = slice->task->request.target_id;
        if (remote_buffers_.count(target_id)) continue;
        auto segment_desc =
            transport_->metadata_manager_->getSegmentDescByID(target_id);
        if (!segment_desc) {
            __sync_fetch_and_add(&slice->task->finish_slices, 1);
            slice->task->status.s = Transport::FAILED;
            continue;
        }
        remote_buffers_[target_id].reload(segment_desc);
    }

    for (int i = 0; i < count_slices; ++i) {
        auto &slice = slice_to_send[i];
        auto target_id = slice->task->request.target_id;
        std::vector<PeerBuffers::Result> local_result, remote_result;
        int ret = local_buffer_.query(AddressRange{slice->source_addr, slice->length}, 
            local_result, slice->retry_count);
        auto &remote_item = remote_buffers_[target_id];
        if (!ret) {
            ret = remote_item.query(
                AddressRange{(void *)slice->target_addr, slice->length}, 
                remote_result, slice->retry_count);
        }
        assert(!ret);
        assert(local_result.size() == 1);
        assert(remote_result.size() == 1);
        auto context = transport_->context_set_[local_result[0].device_id];
        auto peer_segment_name = remote_item.segmentName();
        auto peer_device_name = remote_item.deviceName(remote_result[0].device_id);
        auto peer_name = MakeNicPath(peer_segment_name, peer_device_name);
        auto endpoint = context->endpoint(peer_name);
        if (endpoint->status() != RdmaEndPoint::EP_READY) {
            mutex_.lock();
            if (endpoint->status() != RdmaEndPoint::EP_READY) {
                if (doHandshake(endpoint, peer_segment_name, peer_device_name)) {
                    __sync_fetch_and_add(&slice->task->finish_slices, 1);
                    slice->task->status.s = Transport::FAILED;
                }
            }
            mutex_.unlock();
        }
        slice->source_lkey = local_result[0].lkey;
        slice->target_rkey = remote_result[0].rkey;
        ret = endpoint->submitSlices(slice, 1);
        if (ret != 1 || slice->failed) {
            slice->retry_count++;
            if (slice->retry_count >=
                transport_->params_->workers.max_retry_count) {
                __sync_fetch_and_add(&slice->task->finish_slices, 1);
                slice->task->status.s = Transport::FAILED;
            } else {
                submit(slice);
            }
        }
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
    const static size_t kPollCount = 32;
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
                    __sync_fetch_and_add(&slice->task->finish_slices, 1);
                    slice->task->status.s = Transport::FAILED;
                } else {
                    submit(slice);
                }
            } else {
                auto task = slice->task;
                __sync_fetch_and_add(&task->status.transferred_bytes,
                                     slice->length);
                auto finish_slices =
                    __sync_fetch_and_add(&task->finish_slices, 1);
                if (finish_slices + 1 == task->slice_list.num_slices) {
                    task->status.s = Transport::COMPLETED;
                }
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