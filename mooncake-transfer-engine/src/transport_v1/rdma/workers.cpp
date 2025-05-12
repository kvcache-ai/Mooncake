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

#include <cassert>

#include "transport_v1/rdma/endpoint_store.h"
namespace mooncake {
namespace v1 {
const static size_t kDefaultNumWorkers = 4;

Workers::Workers(RdmaTransport *transport)
    : transport_(transport),
      num_workers_(kDefaultNumWorkers),
      running_(false) {}

Workers::~Workers() {
    if (running_) stop();
}

int Workers::start() {
    if (!running_) {
        running_ = true;
        stop_flag_ = false;
        for (size_t index = 0; index < kDefaultNumWorkers; ++index) {
            workers_.emplace_back([this] { workerThread(); });
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
    for (std::thread &worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    running_ = false;
    return 0;
}

int Workers::submit(RdmaSliceList &slice_list) {
    mutex_.lock();
    auto slice = slice_list.first;
    for (int slice_id = 0; slice_id < slice_list.num_slices; ++slice_id) {
        assert(slice);
        slices_.push(slice);
        slice = slice->next;
    }
    mutex_.unlock();
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

void Workers::asyncPostSend() {
    const static size_t kMaxSlicesToSend = 32;
    const static int kMaxRetryCount = 16;
    std::vector<RdmaSlice *> slice_to_send;
    mutex_.lock();
    while (!slices_.empty() && slice_to_send.size() < kMaxSlicesToSend) {
        auto slice = slices_.front();
        if (slice->retry_count >= kMaxRetryCount) {
            __sync_fetch_and_add(&slice->task->finish_slices, 1);
            slice->task->status.s = Transport::FAILED;
        } else {
            slice_to_send.push_back(slice);
        }
        slices_.pop();
    }
    mutex_.unlock();
    if (slice_to_send.empty()) return;

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
        auto local_device_name = local_detail.devices[local_device_id].name;

        selectDevice(peer_segment_desc, slice->target_addr, slice->length,
                     buffer_id, peer_device_id,
                     slice->retry_count / local_device_count);
        auto &detail = std::get<MemorySegmentDesc>(peer_segment_desc->detail);
        dest_rkey = detail.buffers[buffer_id].rkey[peer_device_id];

        auto peer_nic_path = MakeNicPath(peer_segment_desc->name,
                                         detail.devices[peer_device_id].name);

        auto context = transport_->context_set_[local_device_name];
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
            mutex_.lock();
            slices_.push(slice);
            mutex_.unlock();
        }
    }
}

int Workers::doHandshake(std::shared_ptr<RdmaEndPoint> &endpoint,
                         const std::string &peer_server_name,
                         const std::string &peer_nic_name) {
    // TODO loopback
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

void Workers::asyncPollCq() {
    const static size_t kPollCount = 64;
    for (auto &entry : transport_->context_set_) {
        int cq_count = entry.second->cqCount();
        for (int cq_index = 0; cq_index < cq_count; ++cq_index) {
            auto cq = entry.second->cq(cq_index);
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
                            << "Worker: Process failed for slice (opcode:"
                            << slice->task->request.opcode
                            << ", source_addr: " << (void *)slice->source_addr
                            << ", dest_addr: " << (void *)slice->target_addr
                            << ", length: " << slice->length
                            << ", local_nic: " << entry.second->name()
                            << "): " << ibv_wc_status_str(wc[i].status);
                    }
                    slice->retry_count++;
                    mutex_.lock();
                    slices_.push(slice);
                    mutex_.unlock();
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
        }
    }
}

void Workers::workerThread() {
    while (!stop_flag_) {
        asyncPostSend();
        asyncPollCq();
    }
}

// int WorkerPool::doProcessContextEvents() {
//     ibv_async_event event;
//     if (ibv_get_async_event(context_.context(), &event) < 0) return
//     ERR_CONTEXT; LOG(WARNING) << "Worker: Received context async event "
//                  << ibv_event_type_str(event.event_type) << " for context "
//                  << context_.deviceName();
//     if (event.event_type == IBV_EVENT_DEVICE_FATAL ||
//         event.event_type == IBV_EVENT_CQ_ERR ||
//         event.event_type == IBV_EVENT_WQ_FATAL ||
//         event.event_type == IBV_EVENT_PORT_ERR ||
//         event.event_type == IBV_EVENT_LID_CHANGE) {
//         context_.set_active(false);
//         context_.disconnectAllEndpoints();
//         LOG(INFO) << "Worker: Context " << context_.deviceName()
//                   << " is now inactive";
//     } else if (event.event_type == IBV_EVENT_PORT_ACTIVE) {
//         context_.set_active(true);
//         LOG(INFO) << "Worker: Context " << context_.deviceName()
//                   << " is now active";
//     }
//     ibv_ack_async_event(&event);
//     return 0;
// }

// void WorkerPool::monitorWorker() {
//     bindToSocket(numa_socket_id_);
//     while (workers_running_) {
//         struct epoll_event event;
//         int num_events = epoll_wait(context_.eventFd(), &event, 1, 100);
//         if (num_events < 0) {
//             PLOG(ERROR) << "Worker: epoll_wait()";
//             continue;
//         }

//         if (num_events == 0) continue;

//         if (!(event.events & EPOLLIN)) continue;

//         if (event.data.fd == context_.context()->async_fd)
//             doProcessContextEvents();
//     }
// }
}  // namespace v1
}  // namespace mooncake