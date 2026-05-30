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

#include <glog/logging.h>
#include <atomic>
#include <memory>
#include <utility>
#include <cassert>
#include <sys/epoll.h>
#include "config.h"
#include "transport/kunpeng_transport/ub_context.h"
#include "transport/kunpeng_transport/ub_endpoint.h"

namespace mooncake {
std::shared_ptr<UbEndPoint> UbSIEVEEndpointStore::getEndpoint(
    const std::string& peer_nic_path) {
    RWSpinlock::ReadGuard guard(endpoint_map_lock_);
    auto iter = endpoint_map_.find(peer_nic_path);
    if (iter != endpoint_map_.end()) {
        iter->second.second.store(
            true, std::memory_order_relaxed);  // This is safe within read lock
        // because of idempotence
        return iter->second.first;
    }
    return nullptr;
}

std::shared_ptr<UbEndPoint> UbSIEVEEndpointStore::insertEndpoint(
    const std::string& peer_nic_path, UbContext* context) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    if (endpoint_map_.find(peer_nic_path) != endpoint_map_.end()) {
        LOG(INFO) << "Endpoint " << peer_nic_path
                  << " already exists in SIEVEEndpointStore";
        return endpoint_map_[peer_nic_path].first;
    }
    auto endpoint = context->makeEndpoint();
    if (!endpoint) {
        LOG(ERROR) << "Failed to allocate memory for UbEndPoint";
        return nullptr;
    }
    auto& config = globalConfig();
    int ret = endpoint->construct(config);
    if (ret) return nullptr;

    while (this->getSize() >= max_size_) evictEndpoint();

    endpoint->setPeerNicPath(peer_nic_path);
    endpoint_map_[peer_nic_path] = std::make_pair(endpoint, false);
    fifo_list_.push_front(peer_nic_path);
    fifo_map_[peer_nic_path] = fifo_list_.begin();
    return endpoint;
}

int UbSIEVEEndpointStore::deleteEndpoint(const std::string& peer_nic_path) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    auto iter = endpoint_map_.find(peer_nic_path);
    if (iter != endpoint_map_.end()) {
        waiting_list_len_++;
        waiting_list_.insert(iter->second.first);
        endpoint_map_.erase(iter);
        auto fifo_iter = fifo_map_[peer_nic_path];
        if (hand_.has_value() && hand_.value() == fifo_iter) {
            fifo_iter == fifo_list_.begin() ? hand_ = std::nullopt
                                            : hand_ = std::prev(fifo_iter);
        }
        fifo_list_.erase(fifo_iter);
        fifo_map_.erase(peer_nic_path);
    }
    return 0;
}

void UbSIEVEEndpointStore::evictEndpoint() {
    if (fifo_list_.empty()) {
        return;
    }
    auto o = hand_.has_value() ? hand_.value() : --fifo_list_.end();
    std::string victim;
    while (true) {
        victim = *o;
        if (endpoint_map_[victim].second.load(std::memory_order_relaxed)) {
            endpoint_map_[victim].second.store(false,
                                               std::memory_order_relaxed);
            o = (o == fifo_list_.begin() ? --fifo_list_.end() : std::prev(o));
        } else {
            break;
        }
    }
    hand_ = (o == fifo_list_.begin() ? --fifo_list_.end() : std::prev(o));
    fifo_list_.erase(o);
    fifo_map_.erase(victim);
    auto victim_instance = endpoint_map_[victim].first;
    victim_instance->set_active(false);
    waiting_list_len_++;
    waiting_list_.insert(victim_instance);
    endpoint_map_.erase(victim);
    return;
}

void UbSIEVEEndpointStore::reclaimEndpoint() {
    if (waiting_list_len_.load(std::memory_order_relaxed) == 0) return;
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    std::vector<std::shared_ptr<UbEndPoint>> to_delete;
    for (auto& endpoint : waiting_list_)
        if (!endpoint->hasOutstandingSlice()) to_delete.push_back(endpoint);
    for (auto& endpoint : to_delete) waiting_list_.erase(endpoint);
    waiting_list_len_ -= to_delete.size();
}

int UbSIEVEEndpointStore::destroy() {
    for (auto& endpoint : waiting_list_) endpoint->deconstruct();
    for (auto& kv : endpoint_map_) kv.second.first->deconstruct();
    return 0;
}

int UbSIEVEEndpointStore::disconnect() {
    for (auto& endpoint : waiting_list_) endpoint->disconnect();
    for (auto& kv : endpoint_map_) kv.second.first->disconnect();
    return 0;
}

size_t UbSIEVEEndpointStore::getSize() { return endpoint_map_.size(); }

const static int kTransferWorkerCount = globalConfig().workers_per_ctx;

UbWorkerPool::UbWorkerPool(UbContext& context, int numa_socket_id)
    : context_(context),
      numa_socket_id_(numa_socket_id),
      workers_running_(true),
      suspended_flag_(0),
      redispatch_counter_(0),
      submitted_slice_count_(0),
      processed_slice_count_(0) {
    for (auto& i : slice_queue_count_) i.store(0, std::memory_order_relaxed);
    collective_slice_queue_.resize(kTransferWorkerCount);
    for (int i = 0; i < kTransferWorkerCount; ++i) {
        worker_thread_.emplace_back([this, i] { transferWorker(i); });
    }
    worker_thread_.emplace_back([this] { monitorWorker(); });
}

UbWorkerPool::~UbWorkerPool() {
    if (workers_running_) {
        cond_var_.notify_all();
        workers_running_.store(false);
        for (auto& entry : worker_thread_) entry.join();
    }
}

int UbWorkerPool::submitPostSend(
    const std::vector<Transport::Slice*>& slice_list) {
#ifdef CONFIG_CACHE_SEGMENT_DESC
    thread_local uint64_t tl_last_cache_ts = getCurrentTimeInNano();
    thread_local std::unordered_map<SegmentID,
                                    std::shared_ptr<UbTransport::SegmentDesc>>
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
    std::unordered_map<UbTransport::SegmentID,
                       std::shared_ptr<UbTransport::SegmentDesc>>
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
        if (UbTransport::selectDevice(peer_segment_desc.get(),
                                      slice->ub.dest_addr, slice->length, hint,
                                      buffer_id, device_id)) {
            peer_segment_desc = context_.engine().meta()->getSegmentDescByID(
                slice->target_id, true);
            if (!peer_segment_desc) {
                LOG(ERROR) << "Cannot reload target segment #"
                           << slice->target_id;
                slice->markFailed();
                failed_target_ids[slice->target_id] = getCurrentTimeInNano();
                continue;
            }

            if (UbTransport::selectDevice(peer_segment_desc.get(),
                                          slice->ub.dest_addr, slice->length,
                                          hint, buffer_id, device_id)) {
                slice->markFailed();
                for (const auto& dev_desc : peer_segment_desc.get()->devices) {
                    LOG(ERROR) << "peer device : " << dev_desc.name;
                }
                context_.engine().meta()->dumpMetadataContent(
                    peer_segment_desc->name, slice->ub.dest_addr,
                    slice->length);
                continue;
            }
        }
        if (!peer_segment_desc) {
            slice->markFailed();
            continue;
        }
        auto targetSegment =
            peer_segment_desc->buffers[buffer_id].tseg[device_id];
        slice->ub.r_seg = context_.retrieveRemoteSeg(targetSegment);
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
    if (suspended_flag_.load(std::memory_order_relaxed)) cond_var_.notify_all();
    return 0;
}

void UbWorkerPool::performPostSend(int thread_id) {
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
    thread_local std::unordered_map<std::string, std::shared_ptr<UbEndPoint>>
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
        if (endpoint == nullptr || !endpoint->active())
            endpoint = context_.endpoint(entry.first);
#else
        auto endpoint = context_.endpoint(entry.first);
#endif
        if (!endpoint) {
            for (auto& slice : entry.second) failed_slice_list.push_back(slice);
            entry.second.clear();
            continue;
        }
        if (!endpoint->active()) {
            if (endpoint->inactiveTime() > 1.0)
                context_.deleteEndpoint(entry.first);
            // enable for re-establishation
            for (auto& slice : entry.second) failed_slice_list.push_back(slice);
            entry.second.clear();
            continue;
        }
        if (!endpoint->connected() && endpoint->setupConnectionsByActive()) {
            LOG(ERROR) << "Worker: Cannot make connection for endpoint: "
                       << entry.first << ", mark it inactive";
            for (auto& slice : entry.second) failed_slice_list.push_back(slice);
            endpoint->set_active(false);
            failed_nr_polls++;
            if (context_.active() && failed_nr_polls > 32 &&
                !success_nr_polls) {
                LOG(WARNING)
                    << "Failed to establish peer endpoints in local RNIC "
                    << context_.nicPath() << ", mark it inactive";
                context_.set_active(false);
            }
            entry.second.clear();
            continue;
        }
        endpoint->submitPostSend(entry.second, failed_slice_list);
#endif
    }

    if (!failed_slice_list.empty()) {
        for (auto& slice : failed_slice_list) slice->ub.retry_cnt++;
        redispatch(failed_slice_list, thread_id);
    }
}

void UbWorkerPool::performPoll(int thread_id) {
    int processed_slice_count = 0;
    const static size_t kPollCount = 64;
    std::unordered_map<volatile int*, int> jetty_depth_set;
    for (int jfc_index = thread_id; jfc_index < context_.jfcCount();
         jfc_index += kTransferWorkerCount) {
        UbTransport::Slice* cr[kPollCount];
        int nr_poll = context_.poll(kPollCount, cr, jfc_index);
        if (nr_poll < 0) {
            LOG(ERROR) << "Worker: Failed to poll jetty for complete";
            continue;
        }
        for (int i = 0; i < nr_poll; ++i) {
            UbTransport::Slice* slice = cr[i];
            assert(slice);
            if (jetty_depth_set.count(slice->ub.jetty_depth))
                jetty_depth_set[slice->ub.jetty_depth]++;
            else
                jetty_depth_set[slice->ub.jetty_depth] = 1;
            if (cr[i]->status != Transport::Slice::SUCCESS) {
                failed_nr_polls++;
                if (context_.active() && failed_nr_polls > 32 &&
                    !success_nr_polls) {
                    LOG(WARNING) << "Too many errors found in local RNIC "
                                 << context_.nicPath() << ", mark it inactive";
                    context_.set_active(false);
                }
                context_.deleteEndpoint(slice->peer_nic_path);
                slice->ub.retry_cnt++;
                if (slice->ub.retry_cnt >= slice->ub.max_retry_cnt) {
                    slice->markFailed();
                    processed_slice_count_++;
                } else {
                    collective_slice_queue_[thread_id][slice->peer_nic_path]
                        .push_back(slice);
                    redispatch_counter_++;
                }
            } else {
                // slice->markSuccess();
                processed_slice_count++;
                success_nr_polls++;
            }
        }
        if (nr_poll)
            __sync_fetch_and_sub(context_.outstandingCount(jfc_index), nr_poll);
    }

    for (auto& entry : jetty_depth_set)
        __sync_fetch_and_sub(entry.first, entry.second);

    if (processed_slice_count)
        processed_slice_count_.fetch_add(processed_slice_count);
}

void UbWorkerPool::redispatch(std::vector<Transport::Slice*>& slice_list,
                              int thread_id) {
    std::unordered_map<Transport::SegmentID,
                       std::shared_ptr<Transport::SegmentDesc>>
        segment_desc_map;
    for (auto& slice : slice_list) {
        auto target_id = slice->target_id;
        if (!segment_desc_map.count(target_id)) {
            segment_desc_map[target_id] =
                context_.engine().meta()->getSegmentDescByID(target_id, true);
        }
    }
    for (auto& slice : slice_list) {
        if (slice->ub.retry_cnt >= slice->ub.max_retry_cnt) {
            slice->markFailed();
            processed_slice_count_++;
        } else {
            auto& peer_segment_desc = segment_desc_map[slice->target_id];
            int buffer_id, device_id;
            if (!peer_segment_desc ||
                UbTransport::selectDevice(
                    peer_segment_desc.get(), slice->ub.dest_addr, slice->length,
                    buffer_id, device_id, slice->ub.retry_cnt)) {
                slice->markFailed();
                processed_slice_count_++;
                continue;
            }
            auto targetSegment =
                peer_segment_desc->buffers[buffer_id].tseg[device_id];
            slice->ub.r_seg = context_.retrieveRemoteSeg(targetSegment);
            auto peer_nic_path =
                MakeNicPath(peer_segment_desc->name,
                            peer_segment_desc->devices[device_id].name);
            slice->peer_nic_path = peer_nic_path;
            collective_slice_queue_[thread_id][peer_nic_path].push_back(slice);
        }
    }
}

void UbWorkerPool::transferWorker(int thread_id) {
    thread_local struct {
        uint64_t post_send_count = 0;
        uint64_t post_send_total_ns = 0;
        std::array<uint64_t, 6> post_send_buckets{};

        uint64_t poll_jfc_count = 0;
        uint64_t poll_jfc_total_ns = 0;
        std::array<uint64_t, 6> poll_jfc_buckets{};
    } stats;
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
                cond_var_.wait_for(lock, std::chrono::seconds(1));
                suspended_flag_.fetch_sub(1);
                last_wait_ts = curr_wait_ts;
            }
            continue;
        }
        performPostSend(thread_id);
#ifndef USE_FAKE_POST_SEND
        performPoll(thread_id);
#endif
    }
}

void UbWorkerPool::monitorWorker() {
    bindToSocket(numa_socket_id_);
    auto last_reset_ts = getCurrentTimeInNano();
    while (workers_running_) {
        auto current_ts = getCurrentTimeInNano();
        if (current_ts - last_reset_ts > 1000000000ll) {
            context_.set_active(true);
            last_reset_ts = current_ts;
        }
        struct epoll_event event{};
        int num_events = epoll_wait(context_.eventFd(), &event, 1, 100);
        if (num_events < 0) {
            if (errno != EWOULDBLOCK && errno != EINTR)
                PLOG(ERROR) << "Worker: epoll_wait()";
            continue;
        }

        if (num_events == 0) continue;

        if (!(event.events & EPOLLIN)) continue;

        if (event.data.fd == context_.getAsyncFd()) doProcessContextEvents();
    }
}

int UbWorkerPool::doProcessContextEvents() {
    return context_.doProcessContextEvents();
}
}  // namespace mooncake