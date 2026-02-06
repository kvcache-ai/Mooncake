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

#include "transport/rdma_transport/worker_pool.h"

#include <sys/epoll.h>

#include <cassert>

#include "config.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_endpoint.h"
#include "transport/rdma_transport/rdma_transport.h"

// Experimental: Per-thread SegmentDesc & EndPoint Caches
// #define CONFIG_CACHE_SEGMENT_DESC
// #define CONFIG_CACHE_ENDPOINT

namespace mooncake {

const static int kTransferWorkerCount = globalConfig().workers_per_ctx;

WorkerPool::WorkerPool(RdmaContext& context, int numa_socket_id)
    : context_(context),
      numa_socket_id_(numa_socket_id),
      workers_running_(true),
      suspended_flag_(0),
      redispatch_counter_(0),
      submitted_slice_count_(0),
      processed_slice_count_(0) {
    for (int i = 0; i < kShardCount; ++i)
        slice_queue_count_[i].store(0, std::memory_order_relaxed);
    collective_slice_queue_.resize(kTransferWorkerCount);
    for (int i = 0; i < kTransferWorkerCount; ++i)
        worker_thread_.emplace_back(
            std::thread(std::bind(&WorkerPool::transferWorker, this, i)));
    worker_thread_.emplace_back(
        std::thread(std::bind(&WorkerPool::monitorWorker, this)));
}

WorkerPool::~WorkerPool() {
    if (workers_running_) {
        cond_var_.notify_all();
        workers_running_.store(false);
        for (auto& entry : worker_thread_) entry.join();
    }
}

int WorkerPool::submitPostSend(
    const std::vector<Transport::Slice*>& slice_list) {
#ifdef CONFIG_CACHE_SEGMENT_DESC
    thread_local uint64_t tl_last_cache_ts = getCurrentTimeInNano();
    thread_local std::unordered_map<SegmentID,
                                    std::shared_ptr<RdmaTransport::SegmentDesc>>
        segment_desc_map;
    uint64_t current_ts = getCurrentTimeInNano();

    if (current_ts - tl_last_cache_ts > 1000000000) {
        segment_desc_map.clear();
        tl_last_cache_ts = current_ts;
    }

    for (auto& slice : slice_list) {
        auto target_id = slice->target_id;
        if (!segment_desc_map.count(target_id)) {
            segment_desc_map[target_id] =
                context_.engine().meta()->getSegmentDescByID(target_id);
            if (!segment_desc_map[target_id]) {
                segment_desc_map.clear();
                LOG(ERROR) << "Cannot get target segment description #"
                           << target_id;
                return ERR_INVALID_ARGUMENT;
            }
        }
    }
#else
    std::unordered_map<SegmentID, std::shared_ptr<RdmaTransport::SegmentDesc>>
        segment_desc_map;
    for (auto& slice : slice_list) {
        auto target_id = slice->target_id;
        if (!segment_desc_map.count(target_id))
            segment_desc_map[target_id] =
                context_.engine().meta()->getSegmentDescByID(target_id);
    }
#endif  // CONFIG_CACHE_SEGMENT_DESC

    SliceList slice_list_map[kShardCount];
    uint64_t submitted_slice_count = 0;
    thread_local std::unordered_map<int, uint64_t> failed_target_ids;
    for (auto& slice : slice_list) {
        if (failed_target_ids.count(slice->target_id)) {
            auto ts = failed_target_ids[slice->target_id];
            if (getCurrentTimeInNano() - ts < 100000000ull) {
                slice->markFailed();
                continue;
            } else {
                failed_target_ids.erase(slice->target_id);
            }
        }
        auto& peer_segment_desc = segment_desc_map[slice->target_id];
        int buffer_id, device_id;
        auto hint = globalConfig().enable_dest_device_affinity
                        ? context_.deviceName()
                        : "";
        if (RdmaTransport::selectDevice(peer_segment_desc.get(),
                                        slice->rdma.dest_addr, slice->length,
                                        hint, buffer_id, device_id)) {
            peer_segment_desc = context_.engine().meta()->getSegmentDescByID(
                slice->target_id, true);
            if (!peer_segment_desc) {
                LOG(ERROR) << "Cannot reload target segment #"
                           << slice->target_id;
                slice->markFailed();
                failed_target_ids[slice->target_id] = getCurrentTimeInNano();
                continue;
            }

            if (RdmaTransport::selectDevice(
                    peer_segment_desc.get(), slice->rdma.dest_addr,
                    slice->length, hint, buffer_id, device_id)) {
                slice->markFailed();
                context_.engine().meta()->dumpMetadataContent(
                    peer_segment_desc->name, slice->rdma.dest_addr,
                    slice->length);
                continue;
            }
        }
        if (!peer_segment_desc) {
            slice->markFailed();
            continue;
        }
        slice->rdma.dest_rkey =
            peer_segment_desc->buffers[buffer_id].rkey[device_id];
        auto peer_nic_path =
            MakeNicPath(peer_segment_desc->name,
                        peer_segment_desc->devices[device_id].name);
        slice->peer_nic_path = peer_nic_path;
        int shard_id = (slice->target_id * 10007 + device_id) % kShardCount;
        slice_list_map[shard_id].push_back(slice);
        submitted_slice_count++;
    }

    for (int shard_id = 0; shard_id < kShardCount; ++shard_id) {
        if (slice_list_map[shard_id].empty()) continue;
        slice_queue_lock_[shard_id].lock();
        for (auto& slice : slice_list_map[shard_id])
            slice_queue_[shard_id][slice->peer_nic_path].push_back(slice);
        slice_queue_count_[shard_id].fetch_add(slice_list_map[shard_id].size(),
                                               std::memory_order_relaxed);
        slice_queue_lock_[shard_id].unlock();
    }

    submitted_slice_count_.fetch_add(submitted_slice_count,
                                     std::memory_order_relaxed);
    if (suspended_flag_.load(std::memory_order_relaxed)) {
        std::lock_guard<std::mutex> lock(cond_mutex_);
        cond_var_.notify_all();
    }

    return 0;
}

void WorkerPool::performPostSend(int thread_id) {
    auto& local_slice_queue = collective_slice_queue_[thread_id];
    for (int shard_id = thread_id; shard_id < kShardCount;
         shard_id += kTransferWorkerCount) {
        if (slice_queue_count_[shard_id].load(std::memory_order_relaxed) == 0)
            continue;

        slice_queue_lock_[shard_id].lock();
        for (auto& entry : slice_queue_[shard_id]) {
            for (auto& slice : entry.second)
                local_slice_queue[entry.first].push_back(slice);
            entry.second.clear();
        }
        slice_queue_count_[shard_id].store(0, std::memory_order_relaxed);
        slice_queue_lock_[shard_id].unlock();
    }

    // Redispatch slices to other endpoints, for temporary failures
    thread_local int tl_redispatch_counter = 0;
    if (tl_redispatch_counter <
        redispatch_counter_.load(std::memory_order_relaxed)) {
        tl_redispatch_counter =
            redispatch_counter_.load(std::memory_order_relaxed);
        auto local_slice_queue_clone = local_slice_queue;
        local_slice_queue.clear();
        for (auto& entry : local_slice_queue_clone)
            redispatch(entry.second, thread_id);
        return;
    }

#ifdef CONFIG_CACHE_ENDPOINT
    thread_local uint64_t tl_last_cache_ts = getCurrentTimeInNano();
    thread_local std::unordered_map<std::string, std::shared_ptr<RdmaEndPoint>>
        endpoint_map;
    uint64_t current_ts = getCurrentTimeInNano();
    if (current_ts - tl_last_cache_ts > 1000000000) {
        endpoint_map.clear();
        tl_last_cache_ts = current_ts;
    }
#endif

    SliceList failed_slice_list;
    for (auto& entry : local_slice_queue) {
        if (entry.second.empty()) continue;

#ifdef USE_FAKE_POST_SEND
        for (auto& slice : entry.second) slice->markSuccess();
        processed_slice_count_.fetch_add(entry.second.size());
        entry.second.clear();
#else
#ifdef CONFIG_CACHE_ENDPOINT
        auto& endpoint = endpoint_map[entry.first];
        if (endpoint == nullptr) endpoint = context_.endpoint(entry.first);
#else
        auto endpoint = context_.endpoint(entry.first);
#endif
        if (!endpoint) {
            for (auto& slice : entry.second) failed_slice_list.push_back(slice);
            entry.second.clear();
            continue;
        }
        if (!endpoint->connected() && endpoint->setupConnectionsByActive()) {
            LOG(ERROR) << "Worker: Cannot make connection for endpoint: "
                       << entry.first << ", mark it inactive";
            context_.deleteEndpoint(entry.first);
            for (auto& slice : entry.second) failed_slice_list.push_back(slice);
            entry.second.clear();
            continue;
        }
        endpoint->submitPostSend(entry.second, failed_slice_list);
#endif
    }

    if (!failed_slice_list.empty()) {
        for (auto& slice : failed_slice_list) slice->rdma.retry_cnt++;
        redispatch(failed_slice_list, thread_id);
    }
}

void WorkerPool::performPollCq(int thread_id) {
    int processed_slice_count = 0;
    const static size_t kPollCount = 64;
    std::unordered_map<volatile int*, int> qp_depth_set;
    for (int cq_index = thread_id; cq_index < context_.cqCount();
         cq_index += kTransferWorkerCount) {
        ibv_wc wc[kPollCount];
        int nr_poll = context_.poll(kPollCount, wc, cq_index);
        if (nr_poll < 0) {
            LOG(ERROR) << "Worker: Failed to poll completion queues";
            continue;
        }

        for (int i = 0; i < nr_poll; ++i) {
            Transport::Slice* slice = (Transport::Slice*)wc[i].wr_id;
            assert(slice);
            if (qp_depth_set.count(slice->rdma.qp_depth))
                qp_depth_set[slice->rdma.qp_depth]++;
            else
                qp_depth_set[slice->rdma.qp_depth] = 1;
            // __sync_fetch_and_sub(slice->rdma.qp_depth, 1);
            if (wc[i].status != IBV_WC_SUCCESS) {
                bool show_work_request_flushed_error = globalConfig().trace;
                // After detect an error, subsequent work requests will result
                // in work_request_flushed_error, we hide this by default
                if (wc[i].status != IBV_WC_WR_FLUSH_ERR ||
                    show_work_request_flushed_error)
                    LOG(ERROR)
                        << "Worker: Process failed for slice (opcode: "
                        << slice->opcode
                        << ", source_addr: " << slice->source_addr
                        << ", length: " << slice->length
                        << ", dest_addr: " << (void*)slice->rdma.dest_addr
                        << ", local_nic: " << context_.deviceName()
                        << ", peer_nic: " << slice->peer_nic_path
                        << ", dest_rkey: " << slice->rdma.dest_rkey
                        << ", retry_cnt: " << slice->rdma.retry_cnt
                        << "): " << ibv_wc_status_str(wc[i].status);
                failed_nr_polls++;
                if (context_.active() && failed_nr_polls > 32 &&
                    !success_nr_polls) {
                    LOG(WARNING) << "Too many errors found in local RNIC "
                                 << context_.nicPath() << ", mark it inactive";
                    context_.set_active(false);
                }
                context_.deleteEndpoint(slice->peer_nic_path);
                slice->rdma.retry_cnt++;
                if (slice->rdma.retry_cnt >= slice->rdma.max_retry_cnt) {
                    slice->markFailed();
                    processed_slice_count_++;
                } else {
                    collective_slice_queue_[thread_id][slice->peer_nic_path]
                        .push_back(slice);
                    redispatch_counter_++;
                    // std::vector<RdmaTransport::Slice *> slice_list { slice };
                    // redispatch(slice_list, thread_id);
                }
            } else {
                slice->markSuccess();
                processed_slice_count++;
                success_nr_polls++;
            }
        }
        if (nr_poll)
            __sync_fetch_and_sub(context_.cqOutstandingCount(cq_index),
                                 nr_poll);
    }

    for (auto& entry : qp_depth_set)
        __sync_fetch_and_sub(entry.first, entry.second);

    if (processed_slice_count)
        processed_slice_count_.fetch_add(processed_slice_count);
}

void WorkerPool::redispatch(std::vector<Transport::Slice*>& slice_list,
                            int thread_id) {
    std::unordered_map<SegmentID, std::shared_ptr<Transport::SegmentDesc>>
        segment_desc_map;
    for (auto& slice : slice_list) {
        auto target_id = slice->target_id;
        if (!segment_desc_map.count(target_id)) {
            segment_desc_map[target_id] =
                context_.engine().meta()->getSegmentDescByID(target_id, true);
        }
    }

    for (auto& slice : slice_list) {
        if (slice->rdma.retry_cnt >= slice->rdma.max_retry_cnt) {
            slice->markFailed();
            processed_slice_count_++;
        } else {
            auto& peer_segment_desc = segment_desc_map[slice->target_id];
            int buffer_id, device_id;
            if (!peer_segment_desc ||
                RdmaTransport::selectDevice(peer_segment_desc.get(),
                                            slice->rdma.dest_addr,
                                            slice->length, buffer_id, device_id,
                                            slice->rdma.retry_cnt)) {
                slice->markFailed();
                processed_slice_count_++;
                continue;
            }
            slice->rdma.dest_rkey =
                peer_segment_desc->buffers[buffer_id].rkey[device_id];
            auto peer_nic_path =
                MakeNicPath(peer_segment_desc->name,
                            peer_segment_desc->devices[device_id].name);
            slice->peer_nic_path = peer_nic_path;
            collective_slice_queue_[thread_id][peer_nic_path].push_back(slice);
        }
    }
}

void WorkerPool::transferWorker(int thread_id) {
    bindToSocket(numa_socket_id_);
    const static uint64_t kWaitPeriodInNano = 100000000;  // 100ms
    uint64_t last_wait_ts = getCurrentTimeInNano();
    while (workers_running_.load(std::memory_order_relaxed)) {
        auto processed_slice_count =
            processed_slice_count_.load(std::memory_order_relaxed);
        auto submitted_slice_count =
            submitted_slice_count_.load(std::memory_order_relaxed);
        if (processed_slice_count == submitted_slice_count) {
            uint64_t curr_wait_ts = getCurrentTimeInNano();
            if (curr_wait_ts - last_wait_ts > kWaitPeriodInNano) {
                std::unique_lock<std::mutex> lock(cond_mutex_);
                suspended_flag_.fetch_add(1);
                // Double-check condition after acquiring lock to avoid lost
                // wakeup
                if (processed_slice_count_.load(std::memory_order_relaxed) ==
                    submitted_slice_count_.load(std::memory_order_relaxed)) {
                    cond_var_.wait_for(lock, std::chrono::seconds(1));
                }
                suspended_flag_.fetch_sub(1);
                last_wait_ts = curr_wait_ts;
            }
            continue;
        }
        performPostSend(thread_id);
#ifndef USE_FAKE_POST_SEND
        performPollCq(thread_id);
#endif
    }
}

int WorkerPool::doProcessContextEvents() {
    ibv_async_event event;
    if (ibv_get_async_event(context_.context(), &event) < 0) return ERR_CONTEXT;
    LOG(WARNING) << "Worker: Received context async event "
                 << ibv_event_type_str(event.event_type) << " for context "
                 << context_.deviceName();
    if (event.event_type == IBV_EVENT_QP_FATAL) {
        auto endpoint = (RdmaEndPoint*)event.element.qp->qp_context;
        context_.deleteEndpoint(endpoint->getPeerNicPath());
    } else if (event.event_type == IBV_EVENT_DEVICE_FATAL ||
               event.event_type == IBV_EVENT_CQ_ERR ||
               event.event_type == IBV_EVENT_WQ_FATAL ||
               event.event_type == IBV_EVENT_PORT_ERR ||
               event.event_type == IBV_EVENT_LID_CHANGE) {
        context_.set_active(false);
        context_.disconnectAllEndpoints();
        LOG(INFO) << "Worker: Context " << context_.deviceName()
                  << " is now inactive";
    } else if (event.event_type == IBV_EVENT_PORT_ACTIVE) {
        context_.set_active(true);
        LOG(INFO) << "Worker: Context " << context_.deviceName()
                  << " is now active";
    }
    ibv_ack_async_event(&event);
    return 0;
}

void WorkerPool::monitorWorker() {
    bindToSocket(numa_socket_id_);
    auto last_reset_ts = getCurrentTimeInNano();
    while (workers_running_) {
        auto current_ts = getCurrentTimeInNano();
        if (current_ts - last_reset_ts > 1000000000ll) {
            context_.set_active(true);
            last_reset_ts = current_ts;
        }
        struct epoll_event event;
        int num_events = epoll_wait(context_.eventFd(), &event, 1, 100);
        if (num_events < 0) {
            if (errno != EWOULDBLOCK && errno != EINTR)
                PLOG(ERROR) << "Worker: epoll_wait()";
            continue;
        }

        if (num_events == 0) continue;

        if (!(event.events & EPOLLIN)) continue;

        if (event.data.fd == context_.context()->async_fd)
            doProcessContextEvents();
    }
}
}  // namespace mooncake
