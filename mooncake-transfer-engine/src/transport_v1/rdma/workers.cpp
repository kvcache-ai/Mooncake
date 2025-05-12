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
    std::lock_guard<std::mutex> lock(mutex);
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
    num_entries += count;
}

RdmaSlice *Workers::SliceQueue::pop(size_t count) {
    std::lock_guard<std::mutex> lock(mutex);
    auto slice = head;
    if (count >= num_entries) {
        slice = head;
        head = tail = nullptr;
        num_entries = 0;
        return slice;
    }
    auto cursor = slice;
    for (size_t i = 0; i < count - 1; ++i) cursor = cursor->queue_next;
    head = cursor->queue_next;
    cursor->queue_next = nullptr;
    num_entries -= count;
    return slice;
}

int Workers::submit(RdmaSlice *slice) {
    int id = SimpleRandom::Get().next(num_workers_);
    slice_queue_[id].push(slice, slice, 1);
    std::unique_lock<std::mutex> lock(mutex_);
    auto prev_inflight_slices = inflight_slices_.fetch_add(1);
    if (!prev_inflight_slices) {
        cv_.notify_all();
    }
    return 0;
}

int Workers::submit(RdmaSliceList &slice_list) {
    int id = SimpleRandom::Get().next(num_workers_);
    slice_queue_[id].push(slice_list.first, slice_list.last,
                          slice_list.num_slices);
    std::unique_lock<std::mutex> lock(mutex_);
    auto prev_inflight_slices =
        inflight_slices_.fetch_add(slice_list.num_slices);
    if (!prev_inflight_slices) {
        cv_.notify_all();
    }
    return 0;
}

int Workers::cancel(RdmaSliceList &slice_list) { return ERR_NOT_IMPLEMENTED; }

static inline int selectDevice(std::shared_ptr<SegmentDesc> &desc,
                               uint64_t offset, size_t length, int &buffer_id,
                               int &device_id, int retry_count = 0) {
    auto detail = std::get<MemorySegmentDesc>(desc->detail);
    for (buffer_id = 0; buffer_id < (int)detail.buffers.size(); ++buffer_id) {
        auto &buffer_desc = detail.buffers[buffer_id];
        if (buffer_desc.addr > offset ||
            offset + length > buffer_desc.addr + buffer_desc.length)
            continue;
        device_id =
            detail.topology.selectDevice(buffer_desc.location, retry_count);
        if (device_id >= 0) return 0;
        device_id =
            detail.topology.selectDevice(kWildcardLocation, retry_count);
        if (device_id >= 0) return 0;
    }
    return ERR_ADDRESS_NOT_REGISTERED;
}

void Workers::asyncPostSend(int thread_id) {
    const static size_t kMaxSlicesToSend = 32;

    auto slice = slice_queue_[thread_id].pop(kMaxSlicesToSend);
    if (!slice) return;

    std::vector<RdmaSlice *> slice_to_send;
    while (slice) {
        slice_to_send.push_back(slice);
        slice = slice->queue_next;
    }

    auto local_segment_desc =
        transport_->metadata_manager_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    std::unordered_map<SegmentID, std::shared_ptr<SegmentDesc>>
        segment_desc_map;
    for (auto &slice : slice_to_send) {
        auto target_id = slice->task->request.target_id;
        if (segment_desc_map.count(target_id)) continue;
        auto segment_desc = transport_->metadata_manager_->getSegmentDescByID(
            slice->task->request.target_id);
        if (!segment_desc) {
            __sync_fetch_and_add(&slice->task->finish_slices, 1);
            slice->task->status.s = Transport::FAILED;
            continue;
        }
        segment_desc_map[target_id] = segment_desc;
    }

    for (auto &slice : slice_to_send) {
        auto target_id = slice->task->request.target_id;
        auto &peer_segment_desc = segment_desc_map[target_id];
        int buffer_id = 0, local_device_id = 0, peer_device_id = 0;
        uint32_t source_lkey = 0, dest_rkey = 0;

        int local_device_count = (int)transport_->context_set_.size();
        selectDevice(local_segment_desc, (uint64_t)slice->source_addr,
                     slice->length, buffer_id, local_device_id,
                     slice->retry_count % local_device_count);
        auto &local_detail =
            std::get<MemorySegmentDesc>(local_segment_desc->detail);
        source_lkey = local_detail.buffers[buffer_id].lkey[local_device_id];

        selectDevice(peer_segment_desc, slice->target_addr, slice->length,
                     buffer_id, peer_device_id,
                     slice->retry_count / local_device_count);
        auto &detail = std::get<MemorySegmentDesc>(peer_segment_desc->detail);
        dest_rkey = detail.buffers[buffer_id].rkey[peer_device_id];

        auto peer_nic_path = MakeNicPath(peer_segment_desc->name,
                                         detail.devices[peer_device_id].name);

        auto context = transport_->context_set_[local_device_id];
        auto endpoint = context->endpoint(peer_nic_path);
        if (endpoint->status() != RdmaEndPoint::EP_READY) {
            mutex_.lock();
            if (endpoint->status() != RdmaEndPoint::EP_READY) {
                doHandshake(endpoint, peer_segment_desc->name,
                            detail.devices[peer_device_id].name);
            }
            mutex_.unlock();
        }
        slice->source_lkey = source_lkey;
        slice->target_rkey = dest_rkey;
        int ret = endpoint->submitSlices(slice, 1);
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
    const static size_t kPollCount = 64;
    for (auto &context : transport_->context_set_) {
        int cq_count = context->cqCount();
        for (int cq_index = 0; cq_index < cq_count; ++cq_index) {
            auto cq = context->cq(cq_index);
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
}

void Workers::workerThread(int thread_id) {
    while (!stop_flag_) {
        bool executed = true;
        if (inflight_slices_ == 0) {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock,
                     [this]() { return inflight_slices_ > 0 || stop_flag_; });
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