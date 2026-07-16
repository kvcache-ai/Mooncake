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
#include <functional>

#include "config.h"
#include "memory_location.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_endpoint.h"
#include "transport/rdma_transport/rdma_transport.h"

// Experimental: Per-thread SegmentDesc & EndPoint Caches
// #define CONFIG_CACHE_SEGMENT_DESC
// #define CONFIG_CACHE_ENDPOINT

namespace mooncake {

const static int kTransferWorkerCount = globalConfig().workers_per_ctx;

static std::string resolveBufferLocation(
    const TransferMetadata::BufferDesc &buffer, uint64_t offset) {
    std::string location = buffer.name;
    SegmentsLocationInfo seg_info;
    if (parseSegmentsLocation(buffer.name, seg_info)) {
        location = resolveSegmentsLocation(seg_info, buffer.length,
                                           offset - buffer.addr);
    }
    return location;
}

static const std::string &sourceLocationOrUnknown(Transport::Slice *slice) {
    static const std::string kUnknown = "<unknown>";
    return slice->source_location.empty() ? kUnknown : slice->source_location;
}

static int selectPeerDevice(RdmaTransport::SegmentDesc *peer_segment_desc,
                            uint64_t offset, size_t length,
                            const std::string &local_hca, int &buffer_id,
                            int &device_id, int retry_count = 0) {
    const auto &config = globalConfig();
    if (config.enable_hca_peer_affinity) {
        return RdmaTransport::selectDeviceByLocalHca(
            peer_segment_desc, offset, length, local_hca, buffer_id, device_id,
            retry_count);
    }

    auto hint = config.enable_dest_device_affinity ? std::string_view(local_hca)
                                                   : std::string_view();
    return RdmaTransport::selectDevice(peer_segment_desc, offset, length, hint,
                                       buffer_id, device_id, retry_count);
}

static bool workerCanPost(int thread_id) {
    return kTransferWorkerCount == 1 || thread_id != 0;
}

static bool workerCanPoll(int thread_id) {
    return kTransferWorkerCount == 1 || thread_id == 0;
}

static void getPostingShardAssignment(int thread_id, int &post_tid,
                                      int &post_count) {
    assert(workerCanPost(thread_id));
    if (kTransferWorkerCount > 1) {
        post_tid = thread_id - 1;
        post_count = kTransferWorkerCount - 1;
    } else {
        post_tid = thread_id;
        post_count = kTransferWorkerCount;
    }
}

WorkerPool::WorkerPool(RdmaContext &context, int numa_socket_id)
    : context_(context),
      numa_socket_id_(numa_socket_id),
      workers_running_(true),
      parked_worker_count_(0),
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
        for (auto &entry : worker_thread_) entry.join();
    }
}

int WorkerPool::submitPostSend(
    const std::vector<Transport::Slice *> &slice_list) {
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

    for (auto &slice : slice_list) {
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
    for (auto &slice : slice_list) {
        auto target_id = slice->target_id;
        if (!segment_desc_map.count(target_id))
            segment_desc_map[target_id] =
                context_.engine().meta()->getSegmentDescByID(target_id);
    }
#endif  // CONFIG_CACHE_SEGMENT_DESC

    SliceList slice_list_map[kShardCount];
    uint64_t submitted_slice_count = 0;
    int all_rails_failed_count = 0;
    thread_local std::unordered_map<int, uint64_t> failed_target_ids;
    for (auto &slice : slice_list) {
        if (failed_target_ids.count(slice->target_id)) {
            auto ts = failed_target_ids[slice->target_id];
            if (getCurrentTimeInNano() - ts < 100000000ull) {
                slice->markFailed();
                continue;
            } else {
                failed_target_ids.erase(slice->target_id);
            }
        }
        auto &peer_segment_desc = segment_desc_map[slice->target_id];
        int buffer_id, device_id;
        if (selectPeerDevice(peer_segment_desc.get(), slice->rdma.dest_addr,
                             slice->length, context_.deviceName(), buffer_id,
                             device_id)) {
            peer_segment_desc = context_.engine().meta()->getSegmentDescByID(
                slice->target_id, true);
            if (!peer_segment_desc) {
                LOG(ERROR) << "Cannot reload target segment #"
                           << slice->target_id;
                slice->markFailed();
                failed_target_ids[slice->target_id] = getCurrentTimeInNano();
                continue;
            }

            if (selectPeerDevice(peer_segment_desc.get(), slice->rdma.dest_addr,
                                 slice->length, context_.deviceName(),
                                 buffer_id, device_id)) {
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
            MakeNicPath(peer_segment_desc->nicPathServerName(),
                        peer_segment_desc->devices[device_id].name);

        // If selected rail is paused, try alternative devices
        if (!isRailAvailable(peer_nic_path)) {
            bool found = false;
            for (size_t alt_dev_id = 0;
                 alt_dev_id < peer_segment_desc->devices.size(); ++alt_dev_id) {
                if (alt_dev_id == (size_t)device_id ||
                    alt_dev_id >=
                        peer_segment_desc->buffers[buffer_id].rkey.size()) {
                    continue;
                }
                auto alt_path =
                    MakeNicPath(peer_segment_desc->nicPathServerName(),
                                peer_segment_desc->devices[alt_dev_id].name);
                if (isRailAvailable(alt_path)) {
                    device_id = alt_dev_id;
                    slice->rdma.dest_rkey =
                        peer_segment_desc->buffers[buffer_id].rkey[device_id];
                    peer_nic_path = alt_path;
                    found = true;
                    break;
                }
            }
            if (!found) {
                slice->markFailed();  // All rails unavailable
                all_rails_failed_count++;
                continue;
            }
        }

        slice->peer_nic_path = peer_nic_path;
        if (globalConfig().log_rdma_slice_affinity) {
            VLOG(1) << "RDMA slice affinity: source_location="
                    << sourceLocationOrUnknown(slice) << ", target_location="
                    << resolveBufferLocation(
                           peer_segment_desc->buffers[buffer_id],
                           slice->rdma.dest_addr)
                    << ", local_device_name=" << context_.deviceName()
                    << ", peer_device_name="
                    << peer_segment_desc->devices[device_id].name
                    << ", target_id=" << slice->target_id
                    << ", source_addr=" << slice->source_addr << ", dest_addr="
                    << reinterpret_cast<void *>(slice->rdma.dest_addr)
                    << ", length=" << slice->length;
        }
        int shard_id = (slice->target_id * 10007 + device_id) % kShardCount;
        slice_list_map[shard_id].push_back(slice);
        submitted_slice_count++;
    }

    enqueuePreparedSlices(slice_list_map, submitted_slice_count);

    // Context-level health tracking: if all slices failed due to no available
    // rails, increment the context failure counter. This detects catastrophic
    // local RNIC hardware failure where all paths through the RNIC are down.
    if (submitted_slice_count == 0 &&
        all_rails_failed_count == (int)slice_list.size()) {
        if (markContextFailure()) refreshPublishedLocalTopology();
    }

    return 0;
}

void WorkerPool::enqueuePreparedSlices(SliceList (&slice_list_map)[kShardCount],
                                       uint64_t submitted_slice_count) {
    for (int shard_id = 0; shard_id < kShardCount; ++shard_id) {
        if (slice_list_map[shard_id].empty()) continue;
        slice_queue_lock_[shard_id].lock();
        for (auto &slice : slice_list_map[shard_id])
            slice_queue_[shard_id][slice->peer_nic_path].push_back(slice);
        slice_queue_count_[shard_id].fetch_add(slice_list_map[shard_id].size(),
                                               std::memory_order_relaxed);
        slice_queue_lock_[shard_id].unlock();
    }

    submitted_slice_count_.fetch_add(submitted_slice_count);
    if (submitted_slice_count &&
        parked_worker_count_.load(std::memory_order_acquire) > 0) {
        std::lock_guard<std::mutex> lock(cond_mutex_);
        cond_var_.notify_all();
    }
}

int WorkerPool::submitPreparedPostSend(
    const std::vector<Transport::Slice *> &slice_list) {
    // Called by a different local RNIC's worker during local failover. The
    // slice already carries the chosen peer_nic_path and refreshed local lkey,
    // so enqueue it directly instead of running remote-path selection again.
    SliceList slice_list_map[kShardCount];
    uint64_t submitted_slice_count = 0;

    for (auto &slice : slice_list) {
        if (slice->peer_nic_path.empty()) {
            slice->markFailed();
            continue;
        }
        auto shard_id = static_cast<int>(
            std::hash<std::string>{}(slice->peer_nic_path) % kShardCount);
        slice_list_map[shard_id].push_back(slice);
        submitted_slice_count++;
    }

    enqueuePreparedSlices(slice_list_map, submitted_slice_count);

    return 0;
}

void WorkerPool::trackPostedSlices(
    const std::vector<Transport::Slice *> &slice_list, size_t first,
    size_t count) {
    if (!globalConfig().track_rdma_posted_slices) return;

    std::lock_guard<std::mutex> lock(posted_slices_mutex_);
    for (size_t i = first; i < first + count; ++i)
        posted_slices_.insert(slice_list[i]);
}

void WorkerPool::untrackPostedSlices(
    const std::vector<Transport::Slice *> &slice_list, size_t first,
    size_t count) {
    if (!globalConfig().track_rdma_posted_slices) return;

    std::lock_guard<std::mutex> lock(posted_slices_mutex_);
    for (size_t i = first; i < first + count; ++i)
        posted_slices_.erase(slice_list[i]);
}

void WorkerPool::performPostSend(int thread_id) {
    int post_tid = 0;
    int post_count = 0;
    getPostingShardAssignment(thread_id, post_tid, post_count);
    auto &local_slice_queue = collective_slice_queue_[thread_id];

    // If this local RNIC is inactive/unhealthy, the remote rail is not the
    // problem. Move queued work to another local RNIC while preserving the
    // already selected peer rail.
    if (!context_.active() || !contextHealthy()) {
        auto local_slice_queue_clone = local_slice_queue;
        local_slice_queue.clear();
        for (auto &entry : local_slice_queue_clone)
            redispatch(entry.second, thread_id, true);

        for (int shard_id = post_tid; shard_id < kShardCount;
             shard_id += post_count) {
            if (slice_queue_count_[shard_id].load(std::memory_order_relaxed) ==
                0)
                continue;
            slice_queue_lock_[shard_id].lock();
            auto slice_queue_clone = slice_queue_[shard_id];
            slice_queue_[shard_id].clear();
            slice_queue_count_[shard_id].store(0, std::memory_order_relaxed);
            slice_queue_lock_[shard_id].unlock();
            for (auto &entry : slice_queue_clone)
                redispatch(entry.second, thread_id, true);
        }
        return;
    }

    for (int shard_id = post_tid; shard_id < kShardCount;
         shard_id += post_count) {
        if (slice_queue_count_[shard_id].load(std::memory_order_relaxed) == 0)
            continue;

        slice_queue_lock_[shard_id].lock();
        for (auto &entry : slice_queue_[shard_id]) {
            for (auto &slice : entry.second)
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
        bool handoff_to_local_worker = !context_.active() || !contextHealthy();
        for (auto &entry : local_slice_queue_clone)
            redispatch(entry.second, thread_id, handoff_to_local_worker);
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
    for (auto &entry : local_slice_queue) {
        if (entry.second.empty()) continue;

#ifdef USE_FAKE_POST_SEND
        for (auto &slice : entry.second) slice->markSuccess();
        processed_slice_count_.fetch_add(entry.second.size());
        entry.second.clear();
#else
        if (!isRailAvailable(entry.first)) {
            for (auto &slice : entry.second) failed_slice_list.push_back(slice);
            entry.second.clear();
            continue;
        }
#ifdef CONFIG_CACHE_ENDPOINT
        auto &endpoint = endpoint_map[entry.first];
        if (endpoint == nullptr || !endpoint->active())
            endpoint = context_.endpoint(entry.first);
#else
        auto endpoint = context_.endpoint(entry.first);
#endif
        if (!endpoint) {
            for (auto &slice : entry.second) failed_slice_list.push_back(slice);
            entry.second.clear();
            continue;
        }
        if (!endpoint->connected()) {
            int setup_ret = endpoint->setupConnectionsByActive();
            if (setup_ret) {
                // Active handshake setup failures are ambiguous: the failed
                // side may be the peer rail, or this local RNIC may have just
                // gone inactive. Prefer switching peer rails when one is
                // available; otherwise hand off to another local RNIC only when
                // this context is already known inactive.
                bool local_context_inactive = !context_.active();
                bool has_peer_alternative = false;
                for (auto &slice : entry.second) {
                    if (hasAvailablePeerRailAlternative(slice, entry.first)) {
                        has_peer_alternative = true;
                        break;
                    }
                }
                LOG(WARNING) << "Worker: Cannot make connection for endpoint: "
                             << entry.first
                             << (has_peer_alternative
                                     ? ", pausing peer rail and retrying "
                                       "through an alternate peer RNIC"
                                 : local_context_inactive
                                     ? ", local RNIC is inactive; "
                                       "trying another local RNIC"
                                     : ", no alternate peer RNIC is available; "
                                       "retrying without pausing peer rail");
                if (has_peer_alternative) {
                    markRailFailed(entry.first, true);
                    redispatch_counter_++;
                } else if (local_context_inactive) {
                    context_.set_active(false);
                    refreshPublishedLocalTopology();
                    redispatch_counter_++;
                }
                context_.deleteEndpointByPtr(endpoint.get());
                for (auto &slice : entry.second) {
                    if (!has_peer_alternative && local_context_inactive &&
                        tryHandoffToAnotherLocalWorker(slice)) {
                        processed_slice_count_++;
                    } else {
                        failed_slice_list.push_back(slice);
                    }
                }
                entry.second.clear();
                continue;
            }
        }
        if (!endpoint->readyToSend()) {
            if (endpoint->readyAckTimedOut()) {
                LOG(ERROR) << "Worker: Timed out waiting for RDMA ready ACK "
                           << "for endpoint: " << entry.first
                           << ", deleting endpoint";
                markRailFailed(entry.first, true);
                redispatch_counter_++;
                context_.deleteEndpointByPtr(endpoint.get());
                for (auto &slice : entry.second)
                    failed_slice_list.push_back(slice);
                entry.second.clear();
            }
            continue;
        }
        // Set endpoint pointer for each slice before submitting
        for (auto &slice : entry.second) {
            slice->rdma.endpoint = endpoint.get();
        }
        endpoint->submitPostSend(entry.second, failed_slice_list);
#endif
    }

    if (!failed_slice_list.empty()) {
        SliceList retry_list;
        SliceList local_retry_list;
        for (auto &slice : failed_slice_list) {
            if (shouldRetrySlice(slice)) {
                if (!context_.active())
                    local_retry_list.push_back(slice);
                else
                    retry_list.push_back(slice);
            } else {
                slice->markFailed();
                processed_slice_count_++;
            }
        }
        if (!retry_list.empty()) {
            redispatch(retry_list, thread_id);
        }
        if (!local_retry_list.empty())
            redispatch(local_retry_list, thread_id, true);
    }
}

void WorkerPool::performPollCq(int thread_id) {
    const uint64_t poll_ts = getCurrentTimeInNano();
    const uint64_t previous_poll_ts =
        last_poll_ts_ns_.exchange(poll_ts, std::memory_order_relaxed);
    if (previous_poll_ts > 0 && poll_ts > previous_poll_ts) {
        const uint64_t interval = poll_ts - previous_poll_ts;
        last_poll_interval_ns_.store(interval, std::memory_order_relaxed);
        uint64_t previous_max =
            max_poll_interval_ns_.load(std::memory_order_relaxed);
        while (interval > previous_max &&
               !max_poll_interval_ns_.compare_exchange_weak(
                   previous_max, interval, std::memory_order_relaxed)) {
        }
    }

    int processed_slice_count = 0;
    const static size_t kPollCount = 64;
    std::unordered_map<std::atomic<int> *, int> qp_depth_set;
    std::unordered_set<RdmaEndPoint *> local_failed_endpoints;
    bool recorded_local_context_failure = false;
    SliceList failed_slice_list;
    SliceList local_failed_slice_list;
    for (int cq_index = 0; cq_index < context_.cqCount(); cq_index++) {
        ibv_wc wc[kPollCount];
        int nr_poll = context_.poll(kPollCount, wc, cq_index);
        if (nr_poll < 0) {
            LOG(ERROR) << "Worker: Failed to poll completion queues";
            continue;
        }

        if (nr_poll > 0 && globalConfig().track_rdma_posted_slices) {
            std::lock_guard<std::mutex> lock(posted_slices_mutex_);
            for (int i = 0; i < nr_poll; ++i) {
                auto *slice = reinterpret_cast<Transport::Slice *>(wc[i].wr_id);
                posted_slices_.erase(slice);
            }
        }

        for (int i = 0; i < nr_poll; ++i) {
            Transport::Slice *slice = (Transport::Slice *)wc[i].wr_id;
            assert(slice);
            if (qp_depth_set.count(slice->rdma.qp_depth))
                qp_depth_set[slice->rdma.qp_depth]++;
            else
                qp_depth_set[slice->rdma.qp_depth] = 1;
            // __sync_fetch_and_sub(slice->rdma.qp_depth, 1);
            if (wc[i].status != IBV_WC_SUCCESS) {
                // Flush errors are generated when QPs transition to ERR state
                // during normal endpoint destruction (beginDestroy). They are
                // not real network errors and should not trigger rail failure
                // handling or endpoint deletion.
                if (wc[i].status == IBV_WC_WR_FLUSH_ERR) {
                    if (!context_.active()) {
                        if (globalConfig().trace)
                            LOG(INFO)
                                << "Worker: WR flush error on inactive "
                                << "local context " << context_.deviceName()
                                << " (peer_nic: " << slice->peer_nic_path
                                << "), handing off if retry allows";
                        if (shouldRetrySlice(slice))
                            local_failed_slice_list.push_back(slice);
                        else {
                            slice->markFailed();
                            processed_slice_count++;
                        }
                    } else {
                        if (globalConfig().trace)
                            LOG(INFO) << "Worker: WR flush error (peer_nic: "
                                      << slice->peer_nic_path
                                      << "), redispatching if retry allows";
                        if (shouldRetrySlice(slice))
                            failed_slice_list.push_back(slice);
                        else {
                            slice->markFailed();
                            processed_slice_count++;
                        }
                    }
                    continue;
                }

                // Completion errors are split by local context health. Local
                // faults hand off to another local RNIC; remote/default faults
                // keep this local context and switch peer rails.
                LOG(ERROR) << "Worker: Process failed for slice (opcode: "
                           << slice->opcode
                           << ", source_addr: " << slice->source_addr
                           << ", length: " << slice->length
                           << ", dest_addr: " << (void *)slice->rdma.dest_addr
                           << ", local_nic: " << context_.deviceName()
                           << ", peer_nic: " << slice->peer_nic_path
                           << ", dest_rkey: " << slice->rdma.dest_rkey
                           << ", retry_cnt: " << slice->rdma.retry_cnt
                           << ", max_retry_cnt: " << slice->rdma.max_retry_cnt
                           << "): " << ibv_wc_status_str(wc[i].status);
                auto *retry_list = &failed_slice_list;
                if (!context_.active() || isLocalWcFailure(wc[i])) {
                    if (!recorded_local_context_failure) {
                        handleLocalFailure(slice->peer_nic_path,
                                           slice->rdma.endpoint);
                        recorded_local_context_failure = true;
                        if (slice->rdma.endpoint)
                            local_failed_endpoints.insert(slice->rdma.endpoint);
                    } else if (slice->rdma.endpoint &&
                               !local_failed_endpoints.count(
                                   slice->rdma.endpoint)) {
                        context_.deleteEndpointByPtr(slice->rdma.endpoint);
                        local_failed_endpoints.insert(slice->rdma.endpoint);
                    }
                    retry_list = &local_failed_slice_list;
                } else {
                    if (hasAvailablePeerRailAlternative(slice,
                                                        slice->peer_nic_path)) {
                        markRailFailed(slice->peer_nic_path, true);
                        redispatch_counter_++;
                    }
                    if (slice->rdma.endpoint) {
                        context_.deleteEndpointByPtr(slice->rdma.endpoint);
                    }
                }
                if (shouldRetrySlice(slice)) {
                    retry_list->push_back(slice);
                } else {
                    slice->markFailed();
                    processed_slice_count_++;
                }
            } else {
                slice->markSuccess();
                processed_slice_count++;
            }
        }
        if (nr_poll)
            context_.cqOutstandingCount(cq_index)->fetch_sub(
                nr_poll, std::memory_order_acq_rel);
    }

    for (auto &entry : qp_depth_set)
        entry.first->fetch_sub(entry.second, std::memory_order_acq_rel);

    if (processed_slice_count) {
        processed_slice_count_.fetch_add(processed_slice_count);
        markContextSuccess();
    }

    if (!local_failed_slice_list.empty()) {
        redispatch(local_failed_slice_list, thread_id, true);
    }
    if (!failed_slice_list.empty()) {
        redispatch(failed_slice_list, thread_id);
    }
}

void WorkerPool::redispatch(std::vector<Transport::Slice *> &slice_list,
                            int thread_id, bool handoff_to_local_worker) {
    std::unordered_map<SegmentID, std::shared_ptr<Transport::SegmentDesc>>
        segment_desc_map;
    const bool use_local_queue = workerCanPost(thread_id);
    int shared_redispatch_count = 0;
    // Remote redispatch needs target metadata to choose a new peer RNIC.
    // Local handoff keeps the peer RNIC fixed and only switches source RNIC, so
    // it can skip this lookup.
    if (!handoff_to_local_worker) {
        for (auto &slice : slice_list) {
            auto target_id = slice->target_id;
            if (!segment_desc_map.count(target_id)) {
                segment_desc_map[target_id] =
                    context_.engine().meta()->getSegmentDescByID(target_id,
                                                                 true);
            }
        }
    }

    for (auto &slice : slice_list) {
        if (slice->rdma.retry_cnt >= slice->rdma.max_retry_cnt) {
            slice->markFailed();
            processed_slice_count_++;
        } else {
            if (handoff_to_local_worker) {
                if (tryHandoffToAnotherLocalWorker(slice)) {
                    processed_slice_count_++;
                    continue;
                }
                // A local RNIC failure cannot be repaired by keeping this
                // worker/context and changing the remote rail. If no other
                // local worker can take the slice, fail it immediately.
                slice->markFailed();
                processed_slice_count_++;
                continue;
            }

            // Remote-side/default policy: keep local context fixed and switch
            // remote path.
            auto &peer_segment_desc = segment_desc_map[slice->target_id];
            int buffer_id, device_id;
            if (!peer_segment_desc ||
                selectPeerDevice(peer_segment_desc.get(), slice->rdma.dest_addr,
                                 slice->length, context_.deviceName(),
                                 buffer_id, device_id, slice->rdma.retry_cnt)) {
                LOG(ERROR) << "Worker: Cannot redispatch slice for target "
                           << slice->target_id
                           << ", peer segment unavailable or no target RNIC, "
                           << "dest_addr=" << (void *)slice->rdma.dest_addr
                           << ", length=" << slice->length
                           << ", retry_cnt=" << slice->rdma.retry_cnt;
                slice->markFailed();
                processed_slice_count_++;
                continue;
            }
            slice->rdma.dest_rkey =
                peer_segment_desc->buffers[buffer_id].rkey[device_id];
            auto peer_nic_path =
                MakeNicPath(peer_segment_desc->nicPathServerName(),
                            peer_segment_desc->devices[device_id].name);
            if (!isRailAvailable(peer_nic_path)) {
                bool found = false;
                for (size_t alt_dev_id = 0;
                     alt_dev_id < peer_segment_desc->devices.size();
                     ++alt_dev_id) {
                    if (alt_dev_id == (size_t)device_id ||
                        alt_dev_id >=
                            peer_segment_desc->buffers[buffer_id].rkey.size()) {
                        continue;
                    }
                    auto alt_path = MakeNicPath(
                        peer_segment_desc->nicPathServerName(),
                        peer_segment_desc->devices[alt_dev_id].name);
                    if (isRailAvailable(alt_path)) {
                        device_id = alt_dev_id;
                        slice->rdma.dest_rkey =
                            peer_segment_desc->buffers[buffer_id]
                                .rkey[device_id];
                        peer_nic_path = alt_path;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    LOG(ERROR)
                        << "Worker: Cannot redispatch slice because all peer "
                           "rails are paused for target "
                        << slice->target_id
                        << ", selected peer=" << peer_nic_path
                        << ", retry_cnt=" << slice->rdma.retry_cnt;
                    slice->markFailed();
                    processed_slice_count_++;
                    continue;
                }
            }
            slice->peer_nic_path = peer_nic_path;
            if (globalConfig().log_rdma_slice_affinity) {
                VLOG(1) << "RDMA slice affinity: source_location="
                        << sourceLocationOrUnknown(slice)
                        << ", target_location="
                        << resolveBufferLocation(
                               peer_segment_desc->buffers[buffer_id],
                               slice->rdma.dest_addr)
                        << ", local_device_name=" << context_.deviceName()
                        << ", peer_device_name="
                        << peer_segment_desc->devices[device_id].name
                        << ", target_id=" << slice->target_id
                        << ", source_addr=" << slice->source_addr
                        << ", dest_addr="
                        << reinterpret_cast<void *>(slice->rdma.dest_addr)
                        << ", length=" << slice->length
                        << ", retry_cnt=" << slice->rdma.retry_cnt;
            }
            slice->ts = 0;
            if (use_local_queue) {
                collective_slice_queue_[thread_id][peer_nic_path].push_back(
                    slice);
            } else {
                int shard_id =
                    (slice->target_id * 10007 + device_id) % kShardCount;
                slice_queue_lock_[shard_id].lock();
                slice_queue_[shard_id][peer_nic_path].push_back(slice);
                slice_queue_count_[shard_id].fetch_add(
                    1, std::memory_order_relaxed);
                slice_queue_lock_[shard_id].unlock();
                shared_redispatch_count++;
            }
        }
    }

    if (shared_redispatch_count &&
        parked_worker_count_.load(std::memory_order_acquire) > 0) {
        std::lock_guard<std::mutex> lock(cond_mutex_);
        cond_var_.notify_all();
    }
}

bool WorkerPool::tryHandoffToAnotherLocalWorker(Transport::Slice *slice) {
    // Local failover changes only the source RNIC. Keep target_id,
    // peer_nic_path, dest_addr, and dest_rkey intact; only replace source_lkey
    // for the selected alternate local context.
    auto local_segment_desc =
        context_.engine().meta()->getSegmentDescByID(LOCAL_SEGMENT_ID);
    auto &contexts = context_.engine().context_list_;
    if (!local_segment_desc || contexts.size() <= 1) {
        return false;
    }

    int current_ctx_id = -1;
    for (size_t i = 0; i < contexts.size(); ++i) {
        if (contexts[i] && contexts[i].get() == &context_) {
            current_ctx_id = static_cast<int>(i);
            break;
        }
    }
    if (current_ctx_id < 0) {
        return false;
    }

    int start_ctx = static_cast<int>(slice->rdma.retry_cnt % contexts.size());
    for (size_t offset = 0; offset < contexts.size(); ++offset) {
        int device_id = (start_ctx + static_cast<int>(offset)) %
                        static_cast<int>(contexts.size());
        if (device_id == current_ctx_id) continue;

        auto &alt_ctx = contexts[device_id];
        if (!alt_ctx || !alt_ctx->active()) continue;

        int buffer_id = -1;
        for (size_t idx = 0; idx < local_segment_desc->buffers.size(); ++idx) {
            auto &buffer = local_segment_desc->buffers[idx];
            auto source = reinterpret_cast<uint64_t>(slice->source_addr);
            auto buffer_start = reinterpret_cast<uint64_t>(buffer.addr);
            auto buffer_end = buffer_start + buffer.length;
            if (buffer_start <= source &&
                source + slice->length <= buffer_end) {
                buffer_id = static_cast<int>(idx);
                break;
            }
        }
        if (buffer_id < 0) {
            continue;
        }
        if (device_id >=
            static_cast<int>(
                local_segment_desc->buffers[buffer_id].lkey.size())) {
            continue;
        }

        slice->rdma.source_lkey =
            local_segment_desc->buffers[buffer_id].lkey[device_id];
        slice->rdma.endpoint = nullptr;
        slice->ts = 0;

        std::vector<Transport::Slice *> handoff{slice};
        alt_ctx->worker_pool_->submitPreparedPostSend(handoff);

        VLOG(1) << "Local-side retry handed slice from worker pool on "
                << context_.deviceName() << " to worker pool on "
                << alt_ctx->deviceName() << " while keeping remote peer "
                << slice->peer_nic_path;
        return true;
    }

    return false;
}

bool WorkerPool::hasOutstandingCq(int thread_id) {
    if (!workerCanPoll(thread_id)) return false;
    for (int cq_index = 0; cq_index < context_.cqCount(); ++cq_index) {
        if (context_.cqOutstandingCount(cq_index)->load(
                std::memory_order_relaxed) > 0)
            return true;
    }
    return false;
}

void WorkerPool::transferWorker(int thread_id) {
    bindToSocket(numa_socket_id_);
    const static uint64_t kWaitPeriodInNano = 100000000;  // 100ms
    uint64_t last_wait_ts = getCurrentTimeInNano();
    const bool can_post = workerCanPost(thread_id);
    const bool can_poll = workerCanPoll(thread_id);
    while (workers_running_.load(std::memory_order_relaxed)) {
        auto processed_slice_count =
            processed_slice_count_.load(std::memory_order_relaxed);
        auto submitted_slice_count =
            submitted_slice_count_.load(std::memory_order_relaxed);
        if (processed_slice_count == submitted_slice_count &&
            !hasOutstandingCq(thread_id)) {
            uint64_t curr_wait_ts = getCurrentTimeInNano();
            if (curr_wait_ts - last_wait_ts > kWaitPeriodInNano) {
                std::unique_lock<std::mutex> lock(cond_mutex_);
                parked_worker_count_.fetch_add(1, std::memory_order_acq_rel);
                // Double-check condition after acquiring lock to avoid lost
                // wakeup. parked_worker_count_ is set before this check so
                // producers that submit after it will notify this worker.
                if (processed_slice_count_.load(std::memory_order_relaxed) ==
                        submitted_slice_count_.load() &&
                    !hasOutstandingCq(thread_id)) {
                    cond_var_.wait_for(lock, std::chrono::seconds(1));
                }
                parked_worker_count_.fetch_sub(1, std::memory_order_acq_rel);
                last_wait_ts = curr_wait_ts;
            }
            continue;
        }
        if (can_post) {
            performPostSend(thread_id);
        }
#ifndef USE_FAKE_POST_SEND
        if (can_poll) {
            performPollCq(thread_id);
        }
#endif
        last_wait_ts = getCurrentTimeInNano();
    }
}

int WorkerPool::doProcessContextEvents() {
    ibv_async_event event;
    bool event_acked = false;
    if (ibv_get_async_event(context_.context(), &event) < 0) return ERR_CONTEXT;
    LOG(WARNING) << "Worker: Received context async event "
                 << ibv_event_type_str(event.event_type) << " for context "
                 << context_.deviceName();
    if (event.event_type == IBV_EVENT_QP_FATAL) {
        auto endpoint_ptr = (RdmaEndPoint *)event.element.qp->qp_context;

        /**
         * There might be a deadlock if we call endpoint->set_active(false)
         * before ack the event:
         *
         * Thread A:
         *     Holding endpoint->lock_ and calling ibv_destroy_qp (if using
         * eRDMA), ibv_destroy_qp will block until the event is acked.
         *
         * Thread B (this thread):
         *     Calling endpoint->set_active(false), which blocks as
         * endpoint->lock_ is held by Thread A.
         */
        ibv_ack_async_event(&event);
        event_acked = true;

        /**
         * After ack the event, the endpoint might be destroyed if it happened
         * to be destroying event.element.qp. Therefore, we cannot just
         * dereference endpoint_ptr. Instead, we need to get the shared_ptr of
         * the endpoint from context_ and use that shared_ptr to access the
         * endpoint.
         */
        context_.deleteEndpointByPtr(endpoint_ptr);
    } else if (event.event_type == IBV_EVENT_DEVICE_FATAL ||
               event.event_type == IBV_EVENT_CQ_ERR ||
               event.event_type == IBV_EVENT_WQ_FATAL ||
               event.event_type == IBV_EVENT_PORT_ERR ||
               event.event_type == IBV_EVENT_LID_CHANGE) {
        recovery_activate_after_ns_.store(0, std::memory_order_relaxed);
        context_.set_active(false);
        refreshPublishedLocalTopology();

        /**
         * Similar deadlock might happen if we call
         * context_.disconnectAllEndpoints() before ack the event:
         *
         * Thread A:
         *     Holding endpoint->lock_ and calling ibv_destroy_qp (if using
         * eRDMA), ibv_destroy_qp will block until the event is acked.
         *
         * Thread B (this thread):
         *     Calling endpoint->disconnect(), which blocks as endpoint->lock_
         * is held by Thread A.
         */
        ibv_ack_async_event(&event);
        event_acked = true;

        context_.disconnectAllEndpoints();
        LOG(INFO) << "Worker: Context " << context_.deviceName()
                  << " is now inactive due to fatal event: "
                  << event.event_type;
    } else if (event.event_type == IBV_EVENT_GID_CHANGE) {
        auto gid_refresh_result = refreshPublishedLocalGid();
        ibv_ack_async_event(&event);
        event_acked = true;

        if (gid_refresh_result != GidRefreshResult::UNCHANGED) {
            context_.disconnectAllEndpoints();
            LOG(INFO) << "Worker: Context " << context_.deviceName()
                      << " GID refresh result="
                      << static_cast<int>(gid_refresh_result)
                      << ", disconnected all endpoints";
        }
    } else if (event.event_type == IBV_EVENT_PORT_ACTIVE) {
        // PORT_ACTIVE only means the link started coming back. Real mlx5/RoCE
        // data path can still reject RTR for a while after link-up, so delay
        // publishing this local RNIC back to metadata.
        scheduleContextRecovery();
    }

    if (!event_acked) {
        ibv_ack_async_event(&event);
    }

    return 0;
}

void WorkerPool::processContextEventForTest(ibv_event_type event_type) {
    LOG(WARNING) << "Worker: Injected context async event "
                 << ibv_event_type_str(event_type) << " for context "
                 << context_.deviceName();

    if (event_type == IBV_EVENT_DEVICE_FATAL ||
        event_type == IBV_EVENT_CQ_ERR || event_type == IBV_EVENT_WQ_FATAL ||
        event_type == IBV_EVENT_PORT_ERR ||
        event_type == IBV_EVENT_LID_CHANGE) {
        recovery_activate_after_ns_.store(0, std::memory_order_relaxed);
        context_.set_active(false);
        refreshPublishedLocalTopology();
        context_.disconnectAllEndpoints();
        LOG(INFO) << "Worker: Context " << context_.deviceName()
                  << " is now inactive due to injected fatal event: "
                  << event_type;
    } else if (event_type == IBV_EVENT_GID_CHANGE) {
        auto gid_refresh_result = refreshPublishedLocalGid();
        if (gid_refresh_result != GidRefreshResult::UNCHANGED) {
            context_.disconnectAllEndpoints();
            LOG(INFO) << "Worker: Context " << context_.deviceName()
                      << " injected GID refresh result="
                      << static_cast<int>(gid_refresh_result)
                      << ", disconnected all endpoints";
        }
    } else if (event_type == IBV_EVENT_PORT_ACTIVE) {
        // Match the real async-event path: injected recovery also waits before
        // re-enabling the local RNIC.
        scheduleContextRecovery();
    }
}

void WorkerPool::scheduleContextRecovery(uint64_t delay_ns) {
    uint64_t activate_after = getCurrentTimeInNano() + delay_ns;
    recovery_activate_after_ns_.store(activate_after,
                                      std::memory_order_relaxed);
    LOG(INFO) << "Worker: Context " << context_.deviceName()
              << " scheduled recovery probe after " << delay_ns / 1000000000ull
              << " seconds";
}

void WorkerPool::maybeActivateRecoveredContext() {
    uint64_t activate_after =
        recovery_activate_after_ns_.load(std::memory_order_relaxed);
    if (activate_after == 0 ||
        static_cast<uint64_t>(getCurrentTimeInNano()) < activate_after)
        return;

    uint64_t expected = activate_after;
    if (!recovery_activate_after_ns_.compare_exchange_strong(
            expected, 0, std::memory_order_relaxed)) {
        return;
    }

    auto gid_refresh_result = refreshPublishedLocalGid();
    if (gid_refresh_result == GidRefreshResult::FAILED) {
        context_.set_active(false);
        refreshPublishedLocalTopology();
        scheduleContextRecovery();
        LOG(WARNING) << "Worker: Context " << context_.deviceName()
                     << " failed to refresh GID during recovery; "
                        "keeping inactive";
        return;
    }
    if (gid_refresh_result == GidRefreshResult::CHANGED) {
        context_.disconnectAllEndpoints();
        LOG(INFO) << "Worker: Context " << context_.deviceName()
                  << " GID changed during recovery, disconnected all endpoints";
    }

    context_.set_active(true);
    refreshPublishedLocalTopology();
    context_failure_count_.store(0, std::memory_order_relaxed);
    LOG(INFO) << "Worker: Context " << context_.deviceName()
              << " is now active after recovery delay";
}

bool WorkerPool::hasAvailablePeerRailAlternative(
    Transport::Slice *slice, const std::string &failed_peer_path) {
    auto peer_segment_desc =
        context_.engine().meta()->getSegmentDescByID(slice->target_id, false);
    if (!peer_segment_desc) return false;

    int buffer_id = -1;
    for (size_t idx = 0; idx < peer_segment_desc->buffers.size(); ++idx) {
        auto &buffer = peer_segment_desc->buffers[idx];
        uint64_t buffer_start = reinterpret_cast<uint64_t>(buffer.addr);
        uint64_t buffer_end = buffer_start + buffer.length;
        if (buffer_start <= slice->rdma.dest_addr &&
            slice->rdma.dest_addr + slice->length <= buffer_end) {
            buffer_id = static_cast<int>(idx);
            break;
        }
    }
    if (buffer_id < 0) return false;

    auto server_name = peer_segment_desc->nicPathServerName();
    for (size_t dev_id = 0; dev_id < peer_segment_desc->devices.size();
         ++dev_id) {
        if (dev_id >= peer_segment_desc->buffers[buffer_id].rkey.size()) {
            continue;
        }
        auto peer_path =
            MakeNicPath(server_name, peer_segment_desc->devices[dev_id].name);
        if (peer_path != failed_peer_path && isRailAvailable(peer_path)) {
            return true;
        }
    }
    return false;
}

void WorkerPool::refreshPublishedLocalTopology() {
    std::lock_guard<std::mutex> guard(context_.engine().local_desc_lock_);
    auto desc =
        context_.engine().metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    if (!desc || !context_.engine().local_topology_) return;

    auto updated_desc = std::make_shared<RdmaTransport::SegmentDesc>(*desc);
    updated_desc->topology = *context_.engine().local_topology_;
    for (const auto &context : context_.engine().context_list_) {
        if (context->active()) continue;
        updated_desc->topology.disableDevice(context->deviceName());
    }

    context_.engine().metadata_->addLocalSegment(
        LOCAL_SEGMENT_ID, updated_desc->name, std::move(updated_desc));
    int ret = context_.engine().metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(WARNING) << "Failed to publish RDMA topology update for "
                     << context_.deviceName() << ", ret=" << ret;
    }
}

GidRefreshResult WorkerPool::refreshPublishedLocalGid() {
    std::string previous_gid;
    std::string next_gid;
    auto result = context_.refreshCurrentGid(&previous_gid, &next_gid);
    if (result == GidRefreshResult::CHANGED) {
        LOG(WARNING) << "Worker: refreshed published GID for "
                     << context_.deviceName() << ": " << previous_gid << " -> "
                     << next_gid;
    } else if (result == GidRefreshResult::UNCHANGED) {
        LOG(INFO) << "Worker: received GID change event for "
                  << context_.deviceName() << ", current GID is unchanged";
    } else {
        LOG(ERROR) << "Worker: failed to refresh published GID for "
                   << context_.deviceName()
                   << ", disconnecting endpoints to avoid stale GID reuse";
    }
    return result;
}

void WorkerPool::monitorWorker() {
    bindToSocket(numa_socket_id_);
    auto last_reset_ts = getCurrentTimeInNano();
    uint64_t outstanding_since_ns = 0;
    uint64_t last_timeout_log_ns = 0;
    uint64_t last_processed_count =
        processed_slice_count_.load(std::memory_order_relaxed);
    while (workers_running_) {
        const uint64_t current_ts =
            static_cast<uint64_t>(getCurrentTimeInNano());
        maybeActivateRecoveredContext();
        if (current_ts - last_reset_ts > 1000000000ll) {
            // Drain endpoint_store_->waiting_list_ even when no new
            // insertions are happening. Without this, reclaim only runs
            // from RdmaContext::endpoint() and the waiting list grows
            // unboundedly under failure load. See issue #1845.
            context_.reclaimEndpoints();
            last_reset_ts = current_ts;
        }

        int64_t cq_outstanding = 0;
        for (int cq_index = 0; cq_index < context_.cqCount(); ++cq_index) {
            cq_outstanding += context_.cqOutstandingCount(cq_index)->load(
                std::memory_order_relaxed);
        }
        const uint64_t processed_count =
            processed_slice_count_.load(std::memory_order_relaxed);
        if (processed_count != last_processed_count) {
            last_processed_count = processed_count;
            outstanding_since_ns =
                cq_outstanding > 0 ? current_ts : static_cast<uint64_t>(0);
        }
        if (cq_outstanding > 0) {
            if (outstanding_since_ns == 0) outstanding_since_ns = current_ts;

            const uint64_t outstanding_age_ns =
                current_ts - outstanding_since_ns;
            const uint64_t last_poll_ts =
                last_poll_ts_ns_.load(std::memory_order_relaxed);
            const uint64_t poll_gap_ns =
                last_poll_ts > 0 && current_ts > last_poll_ts
                    ? current_ts - last_poll_ts
                    : 0;

            // Log a stalled poller quickly, and also log at the same 30-second
            // boundary used by TransferEnginePy when polling continues.
            const bool poll_stalled = poll_gap_ns >= 5ULL * 1000 * 1000 * 1000;
            const bool transfer_timed_out =
                outstanding_age_ns >= 30ULL * 1000 * 1000 * 1000;
            if ((poll_stalled || transfer_timed_out) &&
                current_ts - last_timeout_log_ns >= 5ULL * 1000 * 1000 * 1000) {
                LOG(ERROR)
                    << "CQ completion timeout diagnostic: context="
                    << context_.deviceName()
                    << ", outstanding=" << cq_outstanding
                    << ", outstanding_age_ms=" << outstanding_age_ns / 1000000
                    << ", poll_gap_ms=" << poll_gap_ns / 1000000
                    << ", last_poll_interval_ms="
                    << last_poll_interval_ns_.load(std::memory_order_relaxed) /
                           1000000
                    << ", max_poll_interval_ms="
                    << max_poll_interval_ns_.load(std::memory_order_relaxed) /
                           1000000
                    << ", submitted="
                    << submitted_slice_count_.load(std::memory_order_relaxed)
                    << ", processed="
                    << processed_slice_count_.load(std::memory_order_relaxed);

                if (globalConfig().track_rdma_posted_slices) {
                    struct StuckGroup {
                        size_t slice_count = 0;
                        uint64_t total_bytes = 0;
                        uint64_t oldest_post_ts = 0;
                        void *sample_source_addr = nullptr;
                        uint64_t sample_dest_addr = 0;
                    };
                    std::unordered_map<std::string, StuckGroup> stuck_groups;
                    {
                        std::lock_guard<std::mutex> lock(posted_slices_mutex_);
                        for (auto *slice : posted_slices_) {
                            auto &group = stuck_groups[slice->peer_nic_path];
                            group.slice_count++;
                            group.total_bytes += slice->length;
                            if (group.oldest_post_ts == 0 ||
                                static_cast<uint64_t>(slice->ts) <
                                    group.oldest_post_ts) {
                                group.oldest_post_ts =
                                    static_cast<uint64_t>(slice->ts);
                                group.sample_source_addr = slice->source_addr;
                                group.sample_dest_addr = slice->rdma.dest_addr;
                            }
                        }
                    }
                    for (const auto &entry : stuck_groups) {
                        const auto &group = entry.second;
                        const uint64_t oldest_age_ms =
                            group.oldest_post_ts > 0 &&
                                    current_ts > group.oldest_post_ts
                                ? (current_ts - group.oldest_post_ts) / 1000000
                                : 0;
                        LOG(ERROR)
                            << "CQ stuck transfer group: context="
                            << context_.deviceName()
                            << ", peer_nic=" << entry.first
                            << ", slices=" << group.slice_count
                            << ", bytes=" << group.total_bytes
                            << ", oldest_post_age_ms=" << oldest_age_ms
                            << ", sample_source_addr="
                            << group.sample_source_addr << ", sample_dest_addr="
                            << reinterpret_cast<void *>(group.sample_dest_addr);
                    }
                }
                last_timeout_log_ns = current_ts;
            }
        } else {
            outstanding_since_ns = 0;
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

void WorkerPool::markRailFailed(const std::string &peer_nic_path,
                                bool immediate_pause) {
    std::lock_guard<std::mutex> lock(rail_state_lock_);
    auto &state = rail_states_[peer_nic_path];
    uint64_t now = getCurrentTimeInNano();
    state.error_count++;
    if (immediate_pause && state.error_count < kRailErrorThreshold) {
        state.error_count = kRailErrorThreshold;
    }
    if (state.error_count >= kRailErrorThreshold) {
        state.pause_until_ns = now + kRailPauseNs;
        LOG(WARNING) << "Rail paused: peer=" << peer_nic_path
                     << " error_count=" << state.error_count
                     << " pause_ms=" << kRailPauseNs / 1000000ull;
    }
}

bool WorkerPool::isRailAvailable(const std::string &peer_nic_path) {
    std::lock_guard<std::mutex> lock(rail_state_lock_);
    auto it = rail_states_.find(peer_nic_path);
    if (it == rail_states_.end()) return true;
    auto &state = it->second;
    if (state.pause_until_ns == 0) return true;
    uint64_t now = getCurrentTimeInNano();
    if (now >= state.pause_until_ns) {
        // Auto-recover: pause expired
        state.error_count = 0;
        state.pause_until_ns = 0;
        return true;
    }
    return false;
}

// Unified retry logic: increment retry count and return whether retry is
// allowed
bool WorkerPool::shouldRetrySlice(Transport::Slice *slice) {
    slice->rdma.retry_cnt++;
    return slice->rdma.retry_cnt < slice->rdma.max_retry_cnt;
}

bool WorkerPool::isLocalWcFailure(const ibv_wc &wc) {
    // IBV_WC_GENERAL_ERR is intentionally not treated as a local RNIC failure.
    // Providers use it for broad connection/path failures too, and disabling
    // the local context here can mask endpoint GID reprobe and remote rail
    // recovery paths.
    switch (wc.status) {
        case IBV_WC_LOC_LEN_ERR:
        case IBV_WC_LOC_QP_OP_ERR:
        case IBV_WC_LOC_PROT_ERR:
        case IBV_WC_MW_BIND_ERR:
        case IBV_WC_LOC_ACCESS_ERR:
#ifdef IBV_WC_LOC_RDD_VIOL_ERR
        case IBV_WC_LOC_RDD_VIOL_ERR:
#endif
#ifdef IBV_WC_LOC_EEC_OP_ERR
        case IBV_WC_LOC_EEC_OP_ERR:
#endif
#ifdef IBV_WC_LOC_EEC_STATE_ERR
        case IBV_WC_LOC_EEC_STATE_ERR:
#endif
            return true;

        default:
            return false;
    }
}

void WorkerPool::handleLocalFailure(const std::string &peer_nic_path,
                                    RdmaEndPoint *endpoint) {
    // Local completion faults can be caused by a poisoned QP/MR as well as a
    // bad RNIC. Retry this slice elsewhere, but only disable the whole context
    // after repeated local failures or an async port/device event.
    bool context_disabled = markContextFailure();
    if (context_disabled) refreshPublishedLocalTopology();
    redispatch_counter_++;

    // Endpoint may also be poisoned; retire it for safety.
    if (endpoint) {
        context_.deleteEndpointByPtr(endpoint);
    }

    LOG(WARNING) << "Local-side RDMA failure detected on context "
                 << context_.deviceName() << ", peer=" << peer_nic_path;
}

}  // namespace mooncake
