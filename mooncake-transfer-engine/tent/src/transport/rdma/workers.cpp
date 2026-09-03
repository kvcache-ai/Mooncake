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

#include "tent/transport/rdma/workers.h"

#include "tent/transport/rdma/gdr_reachability.h"

#include <sys/epoll.h>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <fstream>
#include <sstream>

#include "tent/transport/rdma/bw_arbitration.h"
#include "tent/transport/rdma/endpoint_store.h"
#include "tent/transport/rdma/promotion_policy.h"
#include "tent/transport/rdma/shared_quota.h"
#include "tent/common/utils/ip.h"
#include "tent/common/utils/string_builder.h"
#include "tent/common/utils/os.h"
#include "tent/common/utils/random.h"

namespace mooncake {
namespace tent {
thread_local int tl_wid = -1;

namespace {
// Look up (or create) the RailMonitor for `machine_id` on this worker's
// map. Returning a stable reference is safe because the map stores values
// via unique_ptr -- rehashes move the pointer slot, not the RailMonitor.
RailMonitor& getOrCreateRail(
    std::unordered_map<std::string, std::unique_ptr<RailMonitor>>& rails,
    const std::string& machine_id) {
    auto it = rails.find(machine_id);
    if (it != rails.end()) return *it->second;
    auto [ins, _] = rails.emplace(machine_id, std::make_unique<RailMonitor>());
    return *ins->second;
}
}  // namespace

Workers::Workers(RdmaTransport* transport)
    : transport_(transport),
      num_workers_(0),
      running_(false),
      worker_context_(nullptr) {
    device_selector_ = std::make_unique<DeviceSelector>();
    device_selector_->loadTopology(transport_->local_topology_);
    auto& conf = transport_->conf_;
    GdrReachability::instance().configure(conf.get());

    // RailMonitor consumes JSON text, while the public configuration is a file
    // path. Load it once here instead of reopening the file for every worker
    // and every remote machine. Invalid/missing files fall back to automatic
    // topology matching in RailMonitor::load().
    const auto& rail_topo_path = transport_->params_->workers.rail_topo_path;
    if (!rail_topo_path.empty()) {
        std::ifstream input(rail_topo_path);
        if (!input.is_open()) {
            LOG(WARNING) << "Unable to open RDMA rail topology file "
                         << rail_topo_path << "; using automatic rail mapping";
        } else {
            std::ostringstream contents;
            contents << input.rdbuf();
            rail_topo_json_ = contents.str();
            if (rail_topo_json_.empty()) {
                LOG(WARNING) << "RDMA rail topology file " << rail_topo_path
                             << " is empty; using automatic rail mapping";
            }
        }
    }

    // ============================================================
    // Core Scheduling Configuration
    // ============================================================

    // Enable/disable smart scheduling (false = simple round-robin)
    bool enable_smart_scheduling =
        conf->get("transports/rdma/enable_smart_scheduling", true);
    device_selector_->setSmartSelection(enable_smart_scheduling);

    // ============================================================
    // NUMA Distance Penalties
    // Higher values = higher penalty for cross-NUMA access
    // Format: [local_numa, remote_numa1, remote_numa2, ...]
    // ============================================================
    DeviceSelector::SchedulingParams params;

    auto numa_penalties =
        conf->get("transports/rdma/numa_penalties", std::vector<double>{});
    if (numa_penalties.size() == Topology::DevicePriorityRanks) {
        for (size_t i = 0; i < Topology::DevicePriorityRanks; ++i) {
            params.numa_tier_weights[i] = numa_penalties[i];
        }
    }

    params.strict_local_numa =
        conf->get("transports/rdma/strict_local_numa", false);

    // ============================================================
    // Bandwidth Estimation (EWMA)
    // ============================================================

    // Learning rate: 0.0 = full adaptation, 1.0 = no adaptation
    params.bandwidth_learning_rate =
        conf->get("transports/rdma/bandwidth_learning_rate", 0.01);

    // Same convention for the transmit estimate the deadline predictors read
    params.transmit_bandwidth_learning_rate =
        conf->get("transports/rdma/transmit_bandwidth_learning_rate",
                  params.transmit_bandwidth_learning_rate);

    // How often that estimate is metered, and how stale an interval may be
    // before it is re-baselined instead of learned from.
    params.transmit_meter_interval_ns =
        conf->get("transports/rdma/transmit_meter_interval_ns",
                  params.transmit_meter_interval_ns);
    params.transmit_meter_max_interval_ns =
        conf->get("transports/rdma/transmit_meter_max_interval_ns",
                  params.transmit_meter_max_interval_ns);

    // EWMA bounds as multipliers of theoretical bandwidth
    params.ewma_min_multiplier =
        conf->get("transports/rdma/ewma_min_bandwidth_multiplier", 0.1);
    params.ewma_max_multiplier =
        conf->get("transports/rdma/ewma_max_bandwidth_multiplier", 10.0);

    // ============================================================
    // Device Selection Scoring
    // ============================================================

    // Random jitter to avoid deterministic selection
    params.score_jitter_range =
        conf->get("transports/rdma/score_jitter_range", 1e-9);

    // Small value to prevent division by zero
    params.score_epsilon = conf->get("transports/rdma/score_epsilon", 1e-12);

    // ============================================================
    // Priority-Based Filtering
    // ============================================================

    params.enable_priority_filtering =
        conf->get("transports/rdma/enable_priority_filtering", true);

    // Local device priority rotation interval (microseconds)
    params.local_rotation_interval_us =
        conf->get("transports/rdma/local_rotation_interval_us", 200);

    // ============================================================
    // Priority Promotion (Anti-Starvation)
    // ============================================================

    // Timeout after which low-priority requests get promoted (nanoseconds)
    // Default: 10ms (10000000 ns)
    priority_promotion_timeout_ns_ =
        conf->get("transports/rdma/priority_promotion_timeout_us", 10000) *
        1000ull;

    // Opt-in deadline-aware bandwidth arbitration within a priority tier
    // (RFC #2792). Default false = original FIFO order.
    deadline_bw_arbitration_ =
        conf->get("transports/rdma/deadline_bw_arbitration", false);

    // Opt-in per-entry promotion (issue #2528). Default false = historical
    // head-only "flush the tier" behavior.
    priority_promotion_per_entry_ =
        conf->get("transports/rdma/priority_promotion_per_entry", false);

    // ============================================================
    // Global Slot Coordination (Multi-Process)
    // ============================================================

    params.slot_rotation_interval_ms =
        conf->get("transports/rdma/slot_rotation_interval_ms", 2);

    // ============================================================
    // Bandwidth Constants (Gbps)
    // ============================================================

    params.default_bandwidth_gbps =
        conf->get("transports/rdma/default_bandwidth_gbps",
                  params.default_bandwidth_gbps);
    params.min_bandwidth_gbps = conf->get("transports/rdma/min_bandwidth_gbps",
                                          params.min_bandwidth_gbps);
    params.max_bandwidth_gbps = conf->get("transports/rdma/max_bandwidth_gbps",
                                          params.max_bandwidth_gbps);

    device_selector_->setSchedulingParams(params);
    device_selector_->auditStrictLocalNuma();

    // Seed each device from its context. context_set_ is indexed by NicID,
    // the same id the selector uses. Three cases:
    //   - no usable context (DEVICE_UNINIT: initializeContexts() replaced a
    //     failed construct() with an inert slot; DEVICE_DISABLED handled the
    //     same way defensively): the NIC cannot carry traffic, so it gets no
    //     bandwidth at all -- not the default -- and leaves candidate
    //     selection and the aggregate.
    //   - port down at open (DEVICE_PAUSED): seeded from whatever the port
    //     reports (possibly 0, i.e. the default), but unavailable until
    //     IBV_EVENT_PORT_ACTIVE.
    //   - DEVICE_ENABLED: seeded from the negotiated speed, or the configured
    //     default (with a warning) when the speed could not be read.
    for (size_t dev_id = 0; dev_id < transport_->context_set_.size();
         ++dev_id) {
        const auto* nic = transport_->local_topology_->getNicEntry(dev_id);
        // Only NIC_RDMA entries have a selector slot (see loadTopology()).
        if (!nic || nic->type != Topology::NIC_RDMA) continue;
        const auto& context = transport_->context_set_[dev_id];
        const auto status =
            context ? context->status() : RdmaContext::DEVICE_UNINIT;
        if (status == RdmaContext::DEVICE_UNINIT ||
            status == RdmaContext::DEVICE_DISABLED) {
            device_selector_->setDeviceAvailable(dev_id, false);
            continue;
        }
        device_selector_->setDeviceBandwidth(dev_id, context->linkSpeedGbps());
        device_selector_->setDeviceAvailable(
            dev_id, status == RdmaContext::DEVICE_ENABLED);
    }

    // ============================================================
    // Shared Memory Configuration
    // ============================================================

    auto shared_quota_shm_path =
        conf->get("transports/rdma/shared_quota_shm_path", "");
    if (!shared_quota_shm_path.empty())
        device_selector_->enableSharedQuota(shared_quota_shm_path);
}

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
        auto& worker = worker_context_[id];
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

Status Workers::submit(RdmaSliceList& slice_list, int worker_id) {
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
    auto& worker = worker_context_[worker_id];

    // This lane's set and counter now account for the slices, wherever they
    // are later swept from. First entry only: a slice arriving here has
    // never been counted, so there is nothing to move.
    auto* owned = slice_list.first;
    for (int i = 0; i < slice_list.num_slices && owned; ++i) {
        owned->owner_worker.store(worker_id, std::memory_order_relaxed);
        owned->counted_lane.store(worker_id, std::memory_order_relaxed);
        owned = owned->next;
    }

    // Get priority from first slice (all slices in list have same priority)
    int priority = PRIO_HIGH;
    if (slice_list.first && slice_list.first->task) {
        priority = slice_list.first->priority;
    }

    worker.queues[priority].push(slice_list);
    if (!worker.inflight_slices.fetch_add(slice_list.num_slices)) {
        std::lock_guard<std::mutex> lock(worker.mutex);
        if (worker.in_suspend) worker.cv.notify_all();
    }
    return Status::OK();
}

Status Workers::submit(RdmaSlice* slice) {
    RdmaSliceList slice_list;
    slice_list.first = slice;
    slice_list.num_slices = 1;
    return submit(slice_list);
}

void Workers::submitFromTick(WorkerContext& worker, RdmaSlice* slice) {
    RdmaSliceList slice_list;
    slice_list.first = slice;
    slice_list.num_slices = 1;
    int priority = PRIO_HIGH;
    if (slice && slice->task) {
        priority = slice->priority;
    }
    // This lane's queue and counter are about to account for the slice, so
    // move the count here before it is visible on the queue. With a shared
    // queue pair the lane re-queueing the slice is the one that polled the
    // completion, not necessarily the one that first enqueued it: whichever
    // lane still counts it is paid back in the same exchange, so the slice
    // is never in two counts or in none. A sweep that already discounted it
    // left -1 behind and there is nothing to pay.
    if (slice) {
        const int lane = laneIndex(worker);
        slice->owner_worker.store(lane, std::memory_order_relaxed);
        const int prev =
            slice->counted_lane.exchange(lane, std::memory_order_acq_rel);
        if (prev >= 0 && prev != lane && prev < (int)num_workers_)
            worker_context_[prev].inflight_slices.fetch_sub(1);
        if (prev != lane) worker.inflight_slices.fetch_add(1);
    }
    // The worker must never block on its own queue (issue #3637): a full
    // queue parks the slice in requeue_overflow and the next tick retries.
    // Either way it stays counted as inflight, which keeps the worker from
    // suspending while a parked flush is pending.
    if (!worker.queues[priority].try_push(slice_list)) {
        worker.requeue_overflow.emplace_back(priority, slice_list);
    }
}

Status Workers::cancel(RdmaTask* task) {
    if (!task) return Status::InvalidArgument("Invalid RDMA task" LOC_MARK);
    if (task->cancel_requested.exchange(true, std::memory_order_acq_rel)) {
        return Status::OK();
    }
    if (!running_.load(std::memory_order_acquire) || !worker_context_ ||
        !num_workers_) {
        return Status::OK();
    }
    // Wake every worker because one task may have slices distributed across
    // several queues. Cancellation remains best effort for slices already
    // posted to a QP; those drain through the normal CQ path.
    for (size_t id = 0; id < num_workers_; ++id) {
        auto& worker = worker_context_[id];
        std::lock_guard<std::mutex> lock(worker.mutex);
        if (worker.in_suspend) worker.cv.notify_all();
    }
    return Status::OK();
}

void Workers::orderByDeadline(std::vector<RdmaSlice*>& slices, int dev_id,
                              uint64_t now_ns) {
    if (!device_selector_ || slices.size() < 2) return;
    // This NIC's transmit estimate answers "how fast do these bytes move";
    // <= 0 means nothing to predict from, so the order stays as it is.
    const double bw_bps = device_selector_->getTransmitBandwidth(dev_id);
    if (bw_bps <= 0.0) return;

    thread_local std::vector<ArbFlow> flows;
    flows.clear();
    flows.reserve(slices.size());
    bool any_deadline = false;
    for (const RdmaSlice* s : slices) {
        flows.push_back(s && s->task
                            ? ArbFlow{s->task->request.deadline_ns, s->length}
                            : ArbFlow{0, 0});
        any_deadline |= flows.back().deadline_ns != 0;
    }
    // Without a deadline anywhere every MLU is 0 and the order would come
    // out as it went in; skip the greedy pass rather than run it for that.
    if (!any_deadline) return;

    // What actually precedes these slices: bytes already posted to the NIC.
    // None of the candidates is in it -- they are about to be posted -- and
    // neither is work still sitting in a worker queue.
    const size_t bytes_ahead = device_selector_->getPostedBytes(dev_id);
    const auto order = OrderByUrgency(flows, now_ns, bw_bps, bytes_ahead);

    thread_local std::vector<RdmaSlice*> ordered;
    ordered.clear();
    ordered.reserve(order.size());
    for (size_t i : order) ordered.push_back(slices[i]);
    std::copy(ordered.begin(), ordered.end(), slices.begin());
}

void Workers::rechargeSlice(RdmaSlice* slice, int dev_id) {
    if (!device_selector_ || !slice || dev_id < 0) return;
    if (slice->charged_dev.load(std::memory_order_relaxed) == dev_id) return;
    if (auto status = device_selector_->chargeDevice(dev_id, slice->length);
        !status.ok()) {
        // A device the selector does not track (a topology/quota mismatch).
        // The transfer can still go, so the slice is not failed; it keeps
        // the charge it has, if any, so the release stays balanced -- but
        // this NIC's load is under-counted, so say so (rate-limited).
        LOG_EVERY_N(WARNING, 100)
            << "rechargeSlice: device " << dev_id
            << " is not tracked by DeviceSelector; slice " << slice
            << " keeps its previous charge: " << status.ToString();
        return;
    }
    // Charge the new device before letting go of the old one, so a release
    // racing in between pays back whichever device is on record and never
    // both, and never one that has not been charged. No latency sample --
    // this is not a completion.
    const int prev =
        slice->charged_dev.exchange(dev_id, std::memory_order_acq_rel);
    if (prev >= 0) device_selector_->release(prev, slice->length, 0.0);
}

bool Workers::dropUnpostableSlice(WorkerContext& worker, RdmaSlice* slice) {
    if (!slice || !slice->task) return false;
    // Two reasons not to post. The caller cancelled the transfer; or another
    // lane already resolved this slice while it waited here for a re-post --
    // with a shared queue pair that lane's timeout sweep can still reach it
    // through a set entry it has not drained. Posting a resolved slice would
    // move bytes for a transfer that is already over.
    if (slice->word == PENDING &&
        !slice->task->cancel_requested.load(std::memory_order_acquire))
        return false;
    // Retire it the way a sweep would -- charge, count and set entry all go
    // back where they came from, each idempotently, since whichever path
    // resolved the slice may already have run them -- and updateSliceStatus
    // is a no-op once the slice is terminal.
    retireSweptSlice(worker, slice, getCurrentTimeInNano(), nullptr);
    updateSliceStatus(slice, CANCELED);
    return true;
}

void Workers::releaseSliceQuota(RdmaSlice* slice, uint64_t now_ns,
                                double latency) {
    if (!slice || !device_selector_) return;
    // Each field names the device it was charged to, so the release goes
    // there even if a fallback has since rewritten source_dev_id.
    const int posted =
        slice->posted_dev.exchange(-1, std::memory_order_acq_rel);
    if (posted >= 0)
        device_selector_->notePostEnded(posted, slice->length, now_ns);
    const int charged =
        slice->charged_dev.exchange(-1, std::memory_order_acq_rel);
    if (charged >= 0)
        device_selector_->release(charged, slice->length, latency);
}

Workers::WorkerContext* Workers::ownerContext(const RdmaSlice* slice) {
    if (!worker_context_ || !slice) return nullptr;
    const int lane = slice->owner_worker.load(std::memory_order_relaxed);
    if (lane < 0 || lane >= (int)num_workers_) return nullptr;
    return &worker_context_[lane];
}

int Workers::laneIndex(const WorkerContext& worker) const {
    if (!worker_context_) return -1;
    return static_cast<int>(&worker - worker_context_);
}

bool Workers::routeSetRemoval(WorkerContext& self, RdmaSlice* slice) {
    // Two sets can hold an entry for one slice: a retry re-queued by another
    // lane moves ownership while the lane it left still holds the old
    // entry. So this lane takes out whatever it holds itself, and the
    // owning lane, if it is somebody else, is handed the slice regardless --
    // a hand-over for an entry it does not hold costs one erase of nothing.
    const bool mine = self.inflight_slice_set.count(slice) > 0;
    auto* owner = ownerContext(slice);
    if (owner && owner != &self) {
        std::lock_guard<std::mutex> lock(owner->reclaim_mutex);
        owner->reclaimed.push_back(slice);
    }
    return mine || !owner || owner == &self;
}

void Workers::retireSweptSlice(WorkerContext& self, RdmaSlice* slice,
                               uint64_t now_ns,
                               std::vector<RdmaSlice*>* deferred,
                               bool bytes_moved) {
    if (!slice) return;
    const int posted_dev = slice->posted_dev.load(std::memory_order_relaxed);
    releaseSliceQuota(slice, now_ns);
    // A slice swept off with FAILED or TIMEOUT leaves the hardware without
    // its bytes being counted, so the stretch it was part of cannot be
    // divided into the next sample. One swept with COMPLETED did move them;
    // its own completion, still to be polled, credits them.
    if (posted_dev >= 0 && !bytes_moved && device_selector_)
        device_selector_->resetTransmitMeter(posted_dev);
    discountFromOwner(self, slice);
    if (!routeSetRemoval(self, slice)) return;
    if (deferred)
        deferred->push_back(slice);
    else
        self.inflight_slice_set.erase(slice);
}

void Workers::discountFromOwner(WorkerContext& self, RdmaSlice* slice) {
    if (!slice) return;
    // Several paths can reach one slice -- its own completion, another
    // lane's sweep of a shared queue pair, this lane's timeout pass -- and
    // only the first of them takes it out of the count: the field says
    // which lane's, and comes back -1 for everyone after.
    const int lane =
        slice->counted_lane.exchange(-1, std::memory_order_acq_rel);
    if (lane < 0) return;
    const bool known = worker_context_ && lane < (int)num_workers_;
    (known ? worker_context_[lane] : self).inflight_slices.fetch_sub(1);
}

void Workers::drainReclaimed(WorkerContext& worker) {
    std::vector<RdmaSlice*> taken;
    {
        std::lock_guard<std::mutex> lock(worker.reclaim_mutex);
        if (worker.reclaimed.empty()) return;
        taken.swap(worker.reclaimed);
    }
    for (auto* slice : taken) worker.inflight_slice_set.erase(slice);
}

void Workers::notePostedSlice(RdmaSlice* slice) {
    if (!slice || !device_selector_) return;
    int none = -1;
    if (slice->posted_dev.compare_exchange_strong(none, slice->source_dev_id,
                                                  std::memory_order_acq_rel))
        device_selector_->notePosted(slice->source_dev_id, slice->length,
                                     slice->submit_ts);
}

void Workers::markPosted(WorkerContext& worker, RdmaSlice* slice,
                         uint64_t post_ts) {
    slice->submit_ts = post_ts;
    notePostedSlice(slice);
    worker.inflight_slice_set.insert(slice);
}

std::shared_ptr<RdmaEndPoint> Workers::getEndpoint(Workers::PostPath path) {
    std::string rpc_server_addr, target_seg_name, target_dev_name,
        target_nic_path_name;
    RouteHint hint;
    auto& segment_manager = transport_->metadata_->segmentManager();
    auto target_id = path.remote_segment_id;
    auto device_id = path.remote_device_id;

    auto status = segment_manager.withCachedSegment(
        target_id, hint.pin, [&](SegmentDesc* segment) {
            hint.segment = segment;
            if (segment->type != SegmentType::Memory) {
                return Status::NeedsRefreshCache(
                    "Segment type is not Memory" LOC_MARK);
            }
            hint.topo = &std::get<MemorySegmentDesc>(segment->detail).topology;
            if (target_id != LOCAL_SEGMENT_ID) {
                rpc_server_addr = segment->rpc_server_addr;
            }
            target_seg_name = segment->name;
            target_nic_path_name = segment->nicPathServerName();
            target_dev_name = hint.topo->getNicName(device_id);
            if (target_seg_name.empty() || target_dev_name.empty()) {
                return Status::NeedsRefreshCache(
                    "Empty target segment or device name" LOC_MARK);
            }
            return Status::OK();
        });

    if (!status.ok()) {
        LOG(ERROR) << status.ToString();
        return nullptr;
    }

    auto context = transport_->context_set_[path.local_device_id].get();
    if (context->status() != RdmaContext::DEVICE_ENABLED) {
        // LOG(WARNING) << "Context " << context->name() << " is not serving";
        return nullptr;  // experimental: force to fail this slice and mark this
                         // connection unavailable
    }
    std::shared_ptr<RdmaEndPoint> endpoint;
    auto peer_name = MakeNicPath(target_nic_path_name, target_dev_name);
    endpoint = context->endpointStore()->getOrInsert(peer_name);
    if (!endpoint) {
        LOG(ERROR) << "Cannot allocate endpoint " << peer_name;
        return nullptr;
    }
    if (endpoint->status() != RdmaEndPoint::EP_READY) {
        auto status = endpoint->connect(target_seg_name, target_dev_name,
                                        rpc_server_addr);
        if (!status.ok()) {
            thread_local uint64_t tl_last_output_ts = 0;
            uint64_t current_ts = getCurrentTimeInNano();
            if (current_ts - tl_last_output_ts > 10000000000ull) {
                tl_last_output_ts = current_ts;
                LOG(ERROR) << "Unable to connect endpoint " << peer_name << ": "
                           << status.ToString();
            }
            return nullptr;
        }
    }
    return endpoint;
}

void Workers::disableEndpoint(RdmaSlice* slice) {
    if (auto* rail = slice->rail_monitor) {
        rail->markFailed(slice->source_dev_id, slice->target_dev_id);
    }
    if (auto ep = slice->ep_weak_ptr.lock()) {
        ep->acknowledge(slice, FAILED);
        ep->resetConnection("Endpoint failed");
    }
}

void Workers::asyncPostSend() {
    auto& worker = worker_context_[tl_wid];
    std::vector<RdmaSliceList> result;

    auto shared_quota =
        device_selector_ ? device_selector_->getSharedSlotManager() : nullptr;

    // Promote timed-out low priority requests
    promoteTimedOutRequests(worker);

    // Priority selection: HIGH -> MEDIUM -> LOW. The worker-local overflow is
    // drained before the shared queues, so a parked retry can never starve
    // behind producers that keep refilling freed slots (issue #3637).
    auto& overflow = worker.requeue_overflow;
    for (int prio = PRIO_HIGH; prio < kNumPriorityLevels; ++prio) {
        if (shared_quota && !shared_quota->canSend(prio)) continue;
        for (auto it = overflow.begin(); it != overflow.end();) {
            if (it->first == prio) {
                result.push_back(it->second);
                it = overflow.erase(it);
            } else {
                ++it;
            }
        }
        if (!result.empty()) break;
        worker.queues[prio].pop(result);
        if (!result.empty()) break;
    }

    for (auto& slice_list : result) {
        if (slice_list.num_slices == 0) continue;
        auto slice = slice_list.first;
        for (int id = 0; id < slice_list.num_slices; ++id) {
            if (dropUnpostableSlice(worker, slice)) {
                slice = slice->next;
                continue;
            }
            auto status = generatePostPath(slice);
            if (!status.ok()) {
                LOG(ERROR) << "Failed to generate post path for slice " << slice
                           << ": " << status.ToString();
                releaseSliceQuota(slice, getCurrentTimeInNano());
                updateSliceStatus(slice, slice->task->cancel_requested.load(
                                             std::memory_order_acquire)
                                             ? CANCELED
                                             : FAILED);
                discountFromOwner(worker, slice);
            } else if (dropUnpostableSlice(worker, slice)) {
                slice = slice->next;
                continue;
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

    for (auto& entry : worker.requests) {
        auto& path = entry.first;
        auto& slices = entry.second;
        if (slices.empty()) continue;
        slices.erase(std::remove_if(slices.begin(), slices.end(),
                                    [&](RdmaSlice* slice) {
                                        return dropUnpostableSlice(worker,
                                                                   slice);
                                    }),
                     slices.end());
        if (slices.empty()) continue;
        auto endpoint = getEndpoint(path);
        if (!endpoint) {
            std::vector<RdmaSlice*> clone;
            slices.swap(clone);
            for (auto slice : clone) {
                if (dropUnpostableSlice(worker, slice)) continue;
                slice->retry_count++;
                releaseSliceQuota(slice, getCurrentTimeInNano());
                if (slice->retry_count >=
                    transport_->params_->workers.max_retry_count) {
                    LOG(WARNING)
                        << "Slice " << slice << " failed: retry count exceeded";
                    disableEndpoint(slice);
                    updateSliceStatus(slice, FAILED);
                    discountFromOwner(worker, slice);
                } else {
                    // The re-submit moves the count to the lane it lands on.
                    submitFromTick(worker, slice);
                }
            }
            continue;
        }

        // RFC #2792 (opt-in): these slices all contend for one NIC path
        // (local NIC -> remote NIC). submitSlices posts as many as the QP
        // budget allows and returns num_submitted; the rest wait for the next
        // round. Ordering by deadline urgency here means a flow about to miss
        // its deadline claims the shared NIC's QP slots ahead of looser flows.
        // Default (deadline_bw_arbitration_ == false) leaves order untouched,
        // so behavior is byte-identical to today's FIFO / equal split.
        if (deadline_bw_arbitration_) {
            orderByDeadline(slices, path.local_device_id,
                            getCurrentTimeInNano());
        }

        // Everything a completion's poller must find is written before the
        // work request can reach the wire: with a shared queue pair another
        // lane may poll the completion before submitSlices returns, and a
        // poller that finds no posted device on the slice would leave the
        // device's backlog holding these bytes for good.
        const uint64_t post_ts = getCurrentTimeInNano();
        int num_submitted = endpoint->submitSlices(
            slices, tl_wid,
            [&](RdmaSlice* posted) { markPosted(worker, posted, post_ts); });
        for (int id = 0; id < num_submitted; ++id) {
            auto slice = slices[id];
            if (!slice->failed) continue;
            // Rejected by the hardware: it never went on the wire, so take
            // back what the hook put in place.
            worker.inflight_slice_set.erase(slice);
            releaseSliceQuota(slice, post_ts);
            if (slice->task->cancel_requested.load(std::memory_order_acquire)) {
                updateSliceStatus(slice, CANCELED);
                discountFromOwner(worker, slice);
                continue;
            }
            slice->retry_count++;
            if (slice->retry_count >=
                transport_->params_->workers.max_retry_count) {
                LOG(WARNING)
                    << "Slice " << slice << " failed: retry count exceeded";
                disableEndpoint(slice);
                updateSliceStatus(slice, FAILED);
                discountFromOwner(worker, slice);
            } else {
                submitFromTick(worker, slice);
            }
        }

        if (num_submitted) {
            slices.erase(slices.begin(), slices.begin() + num_submitted);
        }
    }
}

void Workers::promoteTimedOutRequests(WorkerContext& worker) {
    uint64_t current_ts = getCurrentTimeInNano();
    if (current_ts < worker.next_promotion_check_ns) return;

    // Set next check time (1ms from now)
    worker.next_promotion_check_ns = current_ts + 1000000ull;

    // Drain one level, promote the entries the policy selects to `to`, and put
    // the rest back on `from` in their original order. Returns true if anything
    // was promoted (used to preserve the historical "one level per tick" stop).
    auto promote_level = [&](int from, int to) -> bool {
        std::vector<RdmaSliceList> drained;
        worker.queues[from].pop(drained);
        if (drained.empty()) return false;

        // The worker must never block on its own queue: a full target parks
        // the entry in requeue_overflow and the next tick retries (issue
        // #3637). Parked entries stay counted as inflight throughout.
        auto requeue = [&](int target, RdmaSliceList& slice_list) {
            if (!worker.queues[target].try_push(slice_list)) {
                worker.requeue_overflow.emplace_back(target, slice_list);
            }
        };

        std::vector<uint64_t> enqueue_ts;
        enqueue_ts.reserve(drained.size());
        for (auto& slice_list : drained) {
            auto* slice = slice_list.first;
            enqueue_ts.push_back(slice ? slice->enqueue_ts : 0);
        }

        PromotionDecision decision =
            priority_promotion_per_entry_
                ? DecidePromotionPerEntry(enqueue_ts, current_ts,
                                          priority_promotion_timeout_ns_)
                : DecidePromotionHeadOnly(enqueue_ts, current_ts,
                                          priority_promotion_timeout_ns_);

        if (!decision.promoted_any()) {
            for (auto& slice_list : drained) requeue(from, slice_list);
            return false;
        }

        std::vector<bool> promote(drained.size(), false);
        for (size_t idx : decision.promote_indices) {
            if (idx < drained.size()) promote[idx] = true;
        }
        for (size_t i = 0; i < drained.size(); ++i) {
            requeue(promote[i] ? to : from, drained[i]);
        }
        return true;
    };

    // Check MEDIUM -> HIGH promotion. Preserve the historical behavior of
    // handling at most one level per tick when the head-only policy is active;
    // with per-entry promotion, both levels are considered each tick so a
    // starving LOW entry is not stalled behind an unrelated MEDIUM promotion.
    bool promoted_medium = promote_level(PRIO_MEDIUM, PRIO_HIGH);
    if (promoted_medium && !priority_promotion_per_entry_) return;

    // Check LOW -> MEDIUM promotion
    promote_level(PRIO_LOW, PRIO_MEDIUM);
}

void Workers::expireTimedOutSlices(WorkerContext& worker, uint64_t now_ns) {
    drainReclaimed(worker);
    std::vector<RdmaSlice*> slice_to_remove;
    // Erasing from this lane's set has to wait until the loop below is done
    // with it; slices belonging to another lane are handed over instead.
    // One captured reference keeps the std::function inside its small-buffer
    // storage, so a sweep does not allocate.
    struct Sweep {
        Workers* self;
        WorkerContext* worker;
        uint64_t now_ns;
        std::vector<RdmaSlice*>* deferred;
    } sweep{this, &worker, now_ns, &slice_to_remove};
    auto retire = [&sweep](RdmaSlice* swept) {
        sweep.self->retireSweptSlice(*sweep.worker, swept, sweep.now_ns,
                                     sweep.deferred);
    };
    for (auto& slice : worker.inflight_slice_set) {
        if (slice->word != PENDING) {
            // Terminal but still counted here: an endpoint teardown cancels
            // whatever is queued on it without going through a sweep, so
            // nothing has given this lane its slice back. Do it now, or the
            // charge, the NIC's posted bytes and this lane's count carry it
            // for the life of the process.
            retire(slice);
            continue;
        }
        if (now_ns - slice->enqueue_ts > slice_timeout_ns_) {
            auto ep = slice->ep_weak_ptr.lock();
            LOG(WARNING) << "Slice " << slice
                         << " failed: transfer timeout (software)";
            // The slice turns terminal here, not on the CQ. Its flush
            // completion may never be polled (acknowledge() zeroes wr_depth,
            // so the endpoint destroys the QP and its unpolled CQEs), so
            // return the selector charge now for it and every slice swept
            // with it; releaseSliceQuota() is idempotent.
            if (!ep) {
                retire(slice);
                updateSliceStatus(slice, TIMEOUT);
                continue;
            }
            auto num_slices = ep->acknowledge(slice, TIMEOUT, retire);
            disableEndpoint(slice);
            if (num_slices == 0) {
                // A neighbour's retry already popped it off the queue pair
                // and discounted it there, so only its charge and its set
                // entry are still outstanding.
                releaseSliceQuota(slice, now_ns);
                if (routeSetRemoval(worker, slice))
                    slice_to_remove.push_back(slice);
                updateSliceStatus(slice, TIMEOUT);
            }
        }
    }
    for (auto& slice : slice_to_remove) worker.inflight_slice_set.erase(slice);
}

void Workers::handleCompletion(WorkerContext& worker, RdmaContext& context,
                               const ibv_wc& wc, uint64_t poll_ts,
                               bool last_in_pass) {
    auto slice = (RdmaSlice*)wc.wr_id;
    // What the acknowledge callbacks below need, behind one reference so
    // the std::function stays in its small-buffer storage (no allocation
    // per completion).
    struct Sweep {
        Workers* self;
        WorkerContext* worker;
        uint64_t now_ns;
    } sweep{this, &worker, poll_ts};
    // The lane that enqueued this slice owns its set entry; with a
    // shared queue pair that is not necessarily this one.
    if (routeSetRemoval(worker, slice)) worker.inflight_slice_set.erase(slice);
    auto ep = slice->ep_weak_ptr.lock();
    double enqueue_lat = (slice->submit_ts - slice->enqueue_ts) / 1000.0;
    double inflight_lat = (poll_ts - slice->submit_ts) / 1000.0;
    // The selection EWMA learns only from a successful transfer, and only
    // from this attempt's own post->completion time (#3511): the time since
    // first enqueue folds in queueing and earlier failed attempts on other
    // NICs. A failed or flushed work request, or a slice another path has
    // already resolved, contributes no sample and only returns its charge.
    const bool ewma_sample =
        ep && slice->word == PENDING && wc.status == IBV_WC_SUCCESS;
    const double sample_lat_sec =
        ewma_sample ? (poll_ts - slice->submit_ts) / 1e9 : 0.0;
    const int dev_id = slice->source_dev_id;
    const uint64_t moved = slice->length;
    releaseSliceQuota(slice, poll_ts, sample_lat_sec);
    // The transmit estimate is metered from bytes completed over the time
    // the NIC spent busy, so it needs the release above to have taken this
    // slice out of the backlog first -- that is what closes the stretch.
    // Only successful bytes moved on the wire.
    if (device_selector_) {
        if (wc.status == IBV_WC_SUCCESS) {
            device_selector_->noteCompleted(dev_id, moved);
            // Only once the whole poll pass has been counted. Every work
            // request it reaps carries the same timestamp, so a sample taken
            // partway through would end its interval at that timestamp while
            // leaving the rest of the pass's bytes to the next one -- which
            // then gets them for free. The estimate is an average of
            // per-interval rates, so the short interval that is paid twice
            // and the long one that is paid once do not cancel: at 2 GB/s
            // with sixteen-deep polling that read 8% high.
            if (last_in_pass)
                device_selector_->maybeSampleTransmit(dev_id, poll_ts);
        } else {
            // The bytes of a failed or flushed work request never moved, but
            // the stretch the NIC held it for did. Start over here rather
            // than divide that time into whatever completes next.
            device_selector_->resetTransmitMeter(dev_id);
        }
    }
    if (slice->word != PENDING) {
        // Resolved before its completion was polled -- swept off the queue
        // pair by a neighbour, timed out, or cancelled by a teardown. The
        // charge came back above; the lane count is settled here in case
        // that path could not (idempotent, so a path that did is unharmed).
        discountFromOwner(worker, slice);
        return;
    }
    if (!ep) {
        updateSliceStatus(slice, FAILED);
        discountFromOwner(worker, slice);
        return;
    }
    if (wc.status != IBV_WC_SUCCESS) {
        if (wc.status != IBV_WC_WR_FLUSH_ERR) {
            // TE handles them automatically
            LOG(INFO) << "Detected error WQE for slice " << slice
                      << " (opcode: " << slice->task->request.opcode
                      << ", source_addr: " << (void*)slice->source_addr
                      << ", dest_addr: " << (void*)slice->target_addr
                      << ", length: " << slice->length
                      << ", local_nic: " << context.name()
                      << "): " << ibv_wc_status_str(wc.status);
        }
        // GPUDirect reachability learning: a protection/access error
        // on a GPU buffer means the chosen NIC cannot P2P-DMA to that
        // GPU (ibv_reg_mr succeeded but the PCIe path is unusable).
        // Record it so selection avoids that NIC and converges onto a
        // reachable rail instead of exhausting retries. The local side
        // (source NIC -> source GPU) surfaces as LOC_PROT; the remote
        // side (target NIC -> target GPU) as REM_ACCESS or, for a
        // remote GDR-read failure, REM_OP (observed on strict fabrics).
        bool local_gdr_err = (wc.status == IBV_WC_LOC_PROT_ERR);
        bool remote_gdr_err = (wc.status == IBV_WC_REM_ACCESS_ERR ||
                               wc.status == IBV_WC_REM_OP_ERR);
        if (local_gdr_err && slice->source_gpu_ordinal >= 0 &&
            slice->source_nic_name) {
            GdrReachability::instance().reportLocalFailure(
                slice->source_nic_name, slice->source_gpu_ordinal);
        } else if (remote_gdr_err && slice->target_gpu_ordinal >= 0 &&
                   slice->target_nic_name && slice->target_machine_id) {
            GdrReachability::instance().reportRemoteFailure(
                *slice->target_machine_id, slice->target_nic_name,
                slice->target_gpu_ordinal);
        }
        slice->retry_count++;
        if (slice->retry_count >=
            transport_->params_->workers.max_retry_count) {
            LOG(WARNING) << "Slice " << slice
                         << " failed: retry count exceeded";
            ep->acknowledge(slice, FAILED, [&sweep](RdmaSlice* swept) {
                sweep.self->retireSweptSlice(*sweep.worker, swept, sweep.now_ns,
                                             nullptr);
            });
            disableEndpoint(slice);
        } else {
            // Popped but still in flight: their work requests are
            // live and their completions are still coming, so only
            // the owning lane's count stops tracking them here.
            ep->acknowledge(slice, PENDING, [&](RdmaSlice* swept) {
                discountFromOwner(worker, swept);
            });
            disableEndpoint(slice);
            if (slice->task->cancel_requested.load(std::memory_order_acquire)) {
                updateSliceStatus(slice, CANCELED);
            } else {
                submitFromTick(worker, slice);
            }
        }
    } else {
        ep->acknowledge(slice, COMPLETED, [&sweep](RdmaSlice* swept) {
            sweep.self->retireSweptSlice(*sweep.worker, swept, sweep.now_ns,
                                         nullptr, /*bytes_moved=*/true);
        });
        // A successful GPU transfer re-admits any learned GDR
        // unreachability for the (GPU, NIC) pair(s) it used, so a
        // transient exclusion (or a recovered path) heals. Skipped
        // entirely until something has actually been excluded.
        if (GdrReachability::hasAnyExclusion()) {
            auto& gdr = GdrReachability::instance();
            if (slice->source_gpu_ordinal >= 0 && slice->source_nic_name)
                gdr.reportLocalSuccess(slice->source_nic_name,
                                       slice->source_gpu_ordinal);
            if (slice->target_gpu_ordinal >= 0 && slice->target_nic_name &&
                slice->target_machine_id)
                gdr.reportRemoteSuccess(*slice->target_machine_id,
                                        slice->target_nic_name,
                                        slice->target_gpu_ordinal);
        }
        // A successful transfer proves this rail is healthy; clear
        // any accumulated error count so a previously-cooled-down
        // rail can be used again without waiting for the full
        // cooldown to expire. The monitor pointer is resolved once
        // in generatePostPath, so no map lookup is needed here.
        if (auto* rail = slice->rail_monitor; rail && rail->ready())
            rail->markRecovered(slice->source_dev_id, slice->target_dev_id);
        if (transport_->params_->workers.show_latency_info) {
            worker.perf.inflight_lat.add(inflight_lat);
            worker.perf.enqueue_lat.add(enqueue_lat);
        }
    }
}

void Workers::asyncPollCq() {
    auto& worker = worker_context_[tl_wid];
    const static size_t kPollCount = 64;
    int num_contexts = (int)transport_->context_set_.size();
    int num_cq_list = transport_->params_->device.num_cq_list;

    expireTimedOutSlices(worker, getCurrentTimeInNano());

    for (int index = 0; index < num_contexts; index++) {
        auto& context = transport_->context_set_[index];
        auto cq = context->cq(tl_wid % num_cq_list);
        if (!cq) continue;  // inert context for a non-RDMA or failed NIC
        ibv_wc wc[kPollCount];
        int nr_poll = cq->poll(kPollCount, wc);
        if (nr_poll < 0) continue;
        auto poll_ts = getCurrentTimeInNano();
        for (int i = 0; i < nr_poll; ++i)
            handleCompletion(worker, *context, wc[i], poll_ts,
                             i + 1 == nr_poll);
    }
}

void Workers::showLatencyInfo() {
    auto& worker = worker_context_[tl_wid];
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
    auto& worker = worker_context_[thread_id];

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

int Workers::handleContextEvents(int dev_id,
                                 std::shared_ptr<RdmaContext>& context) {
    // The async fd is non-blocking and edge-triggered
    // (joinNonblockingPollList), and ibv_get_async_event() dequeues one record
    // per call, so every queued event has to be consumed here: epoll only
    // reports readiness again once a *new* event arrives. Bursts are routine
    // (IBV_EVENT_COMM_EST fires once per connection), and a PORT_ACTIVE
    // stranded behind one keeps the context paused until some unrelated event
    // happens to release it -- which may be never.
    while (true) {
        ibv_async_event event;
        errno = 0;
        if (ibv_get_async_event(context->nativeContext(), &event) < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;  // drained
            if (errno == EINTR) continue;
            PLOG(ERROR) << "ibv_get_async_event for context "
                        << context->name();
            return -1;
        }
        if (event.event_type == IBV_EVENT_COMM_EST) {
            VLOG(1) << "Received context async event "
                    << ibv_event_type_str(event.event_type) << " for context "
                    << context->name();
        } else {
            LOG(WARNING) << "Received context async event "
                         << ibv_event_type_str(event.event_type)
                         << " for context " << context->name();
        }
        applyContextEvent(dev_id, *context, event);
        ibv_ack_async_event(&event);
    }
}

void Workers::applyContextEvent(int dev_id, RdmaContext& context,
                                const ibv_async_event& event) {
    switch (event.event_type) {
        case IBV_EVENT_QP_FATAL:
        case IBV_EVENT_WQ_FATAL: {
            auto endpoint = (RdmaEndPoint*)event.element.qp->qp_context;
            context.endpointStore()->remove(endpoint);
            break;
        }
        case IBV_EVENT_CQ_ERR:
            context.pause();
            context.resume();
            LOG(WARNING) << "Action: " << context.name() << " restarted";
            break;
        case IBV_EVENT_DEVICE_FATAL:
            // Device-scoped: every port is gone.
            context.pause();
            device_selector_->setDeviceAvailable(dev_id, false);
            LOG(WARNING) << "Action: " << context.name() << " down";
            break;
        case IBV_EVENT_PORT_ERR:
        case IBV_EVENT_PORT_ACTIVE: {
            if (event.element.port_num != context.portNum()) {
                LOG(INFO) << context.name() << ": ignoring "
                          << ibv_event_type_str(event.event_type)
                          << " for port " << event.element.port_num
                          << " (this context uses port "
                          << static_cast<int>(context.portNum()) << ")";
                break;
            }
            if (event.event_type == IBV_EVENT_PORT_ERR) {
                context.pause();
                // Out of selection and out of the aggregate until the port
                // returns; its EWMA cannot learn anything while no traffic
                // flows.
                device_selector_->setDeviceAvailable(dev_id, false);
                LOG(WARNING) << "Action: " << context.name() << " down";
            } else {
                activateContext(dev_id, context);
                LOG(WARNING) << "Action: " << context.name() << " up";
            }
            break;
        }
#ifdef HAVE_IBV_EVENT_DEVICE_SPEED_CHANGE
        case IBV_EVENT_DEVICE_SPEED_CHANGE:
            // rdma-core >= 62: a port speed changed without a link flap
            // (e.g. a VF over LAG losing a PF). Device-level, so the event
            // names no port; each context opens exactly one, so re-query
            // that one.
            refreshLinkSpeed(dev_id, context);
            break;
#endif
        default:
            break;
    }
}

void Workers::activateContext(int dev_id, RdmaContext& context) {
    context.resume();
    // The link may have renegotiated while down: re-seed before the device
    // becomes selectable so no worker scores it on the old rate.
    refreshLinkSpeed(dev_id, context);
    if (device_selector_) device_selector_->setDeviceAvailable(dev_id, true);
}

void Workers::refreshLinkSpeed(int dev_id, RdmaContext& context) {
    if (!device_selector_) return;
    const double before = context.linkSpeedGbps();
    if (context.refreshPortAttributes() != 0) return;  // already logged
    const double after = context.linkSpeedGbps();
    // Both values decode from the same integer encodings, so exact
    // comparison is meaningful. A speed that can no longer be read (0)
    // counts as a change: setDeviceBandwidth() then falls back to the
    // configured default and warns, the same as at startup.
    if (after == before) return;
    LOG(WARNING) << context.name() << " link speed " << before << " -> "
                 << after << " Gbps ("
                 << (context.effectiveSpeedKnown() ? "effective speed"
                                                   : "encoded rate")
                 << "), re-seeding its bandwidth estimate";
    device_selector_->setDeviceBandwidth(dev_id, after);
}

void Workers::reclaimEndpoints() {
    for (auto& context : transport_->context_set_) {
        // Inert contexts never built an endpoint store.
        auto store = context->endpointStore();
        if (store) store->reclaim();
    }
}

void Workers::resumePausedContexts() {
    for (size_t dev_id = 0; dev_id < transport_->context_set_.size();
         ++dev_id) {
        auto& context = transport_->context_set_[dev_id];
        // Only a paused context is waiting for a recovery event; this also
        // filters out inert slots, which never leave DEVICE_UNINIT.
        if (!context || context->status() != RdmaContext::DEVICE_PAUSED)
            continue;
        ibv_port_state state;
        if (context->queryPortState(&state) != 0) continue;  // already logged
        // Only a fully active port carries traffic. Intermediate states
        // (INIT/ARMED/ACTIVE_DEFER) mean the link is still settling, so leave
        // the context paused and re-check on the next tick.
        if (state != IBV_PORT_ACTIVE) continue;
        LOG(WARNING) << "Action: " << context->name()
                     << " up (port reports ACTIVE without an "
                        "IBV_EVENT_PORT_ACTIVE event)";
        activateContext(static_cast<int>(dev_id), *context);
    }
}

void Workers::monitorThread() {
    // Track time for periodic endpoint reclaim (1 Hz heartbeat)
    auto last_reclaim_time = std::chrono::steady_clock::now();

    while (running_) {
        // Periodic endpoint reclaim: runs every 1 second to drain waiting_list_
        // Under failure load, insertions stall but endpoints still need cleanup
        auto current_time = std::chrono::steady_clock::now();
        auto time_since_last_reclaim =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                current_time - last_reclaim_time)
                .count();

        if (time_since_last_reclaim >= 1000) {  // 1 second = 1000 ms
            reclaimEndpoints();
            // Safety net for a recovery event that never reached us.
            resumePausedContexts();
            last_reclaim_time = current_time;
        }

        for (size_t dev_id = 0; dev_id < transport_->context_set_.size();
             ++dev_id) {
            auto& context = transport_->context_set_[dev_id];
            struct epoll_event event;
            if (context->eventFd() < 0) continue;
            int num_events = epoll_wait(context->eventFd(), &event, 1, 100);
            if (num_events < 0) {
                PLOG(ERROR) << "epoll_wait()";
                continue;
            }
            if (num_events == 0) continue;
            if (!(event.events & EPOLLIN)) continue;
            if (event.data.fd == context->nativeContext()->async_fd)
                handleContextEvents(static_cast<int>(dev_id), context);
        }
    }
}

Status Workers::getRouteHint(RouteHint& hint, SegmentID segment_id,
                             uint64_t addr, uint64_t length) {
    auto& segment_manager = transport_->metadata_->segmentManager();
    CHECK_STATUS(segment_manager.withCachedSegment(
        segment_id, hint.pin, [&](SegmentDesc* segment) {
            hint.segment = segment;
            hint.buffer = segment->findBuffer(addr, length);
            if (!hint.buffer)
                return Status::NeedsRefreshCache(
                    "No matched buffer in given address range" LOC_MARK);

            if (hint.segment->type != SegmentType::Memory)
                return Status::NeedsRefreshCache(
                    "Segment type not memory" LOC_MARK);
            return Status::OK();
        }));

    hint.topo = &std::get<MemorySegmentDesc>(hint.segment->detail).topology;
    std::string location = hint.buffer->location;
    if (!hint.buffer->regions.empty()) {
        size_t offset = hint.buffer->addr;
        size_t best_overlap = 0;
        size_t target_start = addr;
        size_t target_end = addr + length;
        for (auto& entry : hint.buffer->regions) {
            size_t region_start = offset;
            size_t region_end = offset + entry.size;
            size_t overlap_start = std::max(region_start, target_start);
            size_t overlap_end = std::min(region_end, target_end);
            size_t overlap = (overlap_end > overlap_start)
                                 ? (overlap_end - overlap_start)
                                 : 0;
            if (overlap > best_overlap) {
                best_overlap = overlap;
                location = entry.location;
            }
            offset += entry.size;
        }
    }
    auto mem_id = hint.topo->getMemId(location);
    if (mem_id < 0) mem_id = hint.topo->getMemId(kWildcardLocation);
    hint.topo_entry = hint.topo->getMemEntry(mem_id);
    hint.location = std::move(location);
    return Status::OK();
}

int Workers::getDeviceRank(const RouteHint& hint, int device_id) {
    for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
        auto& list = hint.topo_entry->device_list[rank];
        for (auto& entry : list)
            if (entry == device_id) return rank;
    }
    return -1;
}

Status Workers::selectOptimalDevice(RouteHint& source, RouteHint& target,
                                    RdmaSlice* slice) {
    auto& worker = worker_context_[tl_wid];
    if (slice->source_dev_id < 0) {
        CHECK_STATUS(device_selector_->allocate(
            slice->length, source.buffer->location, slice->source_dev_id,
            slice->priority, slice->task->device_mask));
        slice->charged_dev = slice->source_dev_id;
    }

    if (slice->source_dev_id < 0)
        return Status::DeviceNotFound(
            "No device could access the slice memory region" LOC_MARK);

    auto& rail = getOrCreateRail(worker.rails, target.segment->machine_id);
    if (!rail.ready() || target.topo != rail.remote())
        rail.load(std::shared_ptr<const Topology>(source.pin, source.topo),
                  std::shared_ptr<const Topology>(target.pin, target.topo),
                  rail_topo_json_, transport_->conf_.get());
    if (slice->target_dev_id < 0) {
        int mapped_dev_id = rail.findBestRemoteDevice(
            slice->source_dev_id, target.topo_entry->numa_node);
        for (size_t rank = 0; rank < Topology::DevicePriorityRanks - 1;
             ++rank) {
            if (rank && always_tier1_) break;
            const auto& list = target.topo_entry->device_list[rank];
            if (list.empty()) continue;
            if (std::find(list.begin(), list.end(), mapped_dev_id) !=
                list.end()) {
                slice->target_dev_id = mapped_dev_id;
                break;
            }
        }
    }

    if (slice->target_dev_id < 0) {
        for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
            const auto& list = target.topo_entry->device_list[rank];
            if (list.empty()) continue;
            size_t start = SimpleRandom::Get().next(list.size());
            slice->target_dev_id = list[start];
            // Prefer a same-NUMA peer NIC; do not fail if none exist.
            if (strictLocalNuma()) {
                for (size_t i = 0; i < list.size(); ++i) {
                    int tdev = list[(start + i) % list.size()];
                    if (!target.topo->isCrossNuma(*target.topo_entry, tdev)) {
                        slice->target_dev_id = tdev;
                        break;
                    }
                }
            }
            break;
        }
    }
    /*
    if (slice->target_dev_id < 0) {
        int mapped_dev_id = rail.findBestRemoteDevice(
            slice->source_dev_id, target.topo_entry->numa_node);
        for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
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
    }
    */

    if (slice->target_dev_id < 0)
        return Status::DeviceNotFound(
            "No device could access the slice memory region" LOC_MARK);

    // Proactively steer away from a NIC/GPU pair already known to be
    // GPUDirect-unreachable (learned from earlier completion errors) so we
    // never post to a dead rail; the fallback path re-selects a reachable one.
    // Reactive learning in asyncPollCq still covers pairs not yet observed.
    bool gdr_excluded = false;
    if (GdrReachability::hasAnyExclusion()) {
        int src_gpu = -1, dst_gpu = -1;
        LocationParser s(source.location), d(target.location);
        if (s.type() == "cuda") src_gpu = s.index();
        if (d.type() == "cuda") dst_gpu = d.index();
        gdr_excluded = gdrPairExcluded(source, target, slice->source_dev_id,
                                       slice->target_dev_id, src_gpu, dst_gpu);
    }

    if (gdr_excluded ||
        !rail.available(slice->source_dev_id, slice->target_dev_id)) {
        LOG(INFO) << "Optimal device pair not available: source_dev_id "
                  << slice->source_dev_id << ", target_dev_id "
                  << slice->target_dev_id;
        return selectFallbackDevice(source, target, slice);
    }

    return Status::OK();
}

bool Workers::strictLocalNuma() const {
    return device_selector_ &&
           device_selector_->getSchedulingParams().strict_local_numa;
}

int Workers::getDeviceByFlatIndex(const RouteHint& hint, size_t flat_idx) {
    for (size_t rank = 0; rank < Topology::DevicePriorityRanks; ++rank) {
        auto& list = hint.topo_entry->device_list[rank];
        if (flat_idx < list.size()) return list[flat_idx];
        flat_idx -= list.size();
    }
    return -1;
}

bool Workers::gdrPairExcluded(const RouteHint& source, const RouteHint& target,
                              int sdev, int tdev, int src_gpu, int dst_gpu) {
    auto& gdr = GdrReachability::instance();
    if (src_gpu >= 0) {
        const auto* lnic = source.topo->getNicEntry(sdev);
        if (lnic && !gdr.localReachable(lnic->name, src_gpu)) return true;
    }
    // Target-GPU reachability applies whether or not the peer is remote: on a
    // same-host transfer the "remote" GPU is still a physical GPU some NICs
    // cannot P2P to. The failure is reported under the (target machine_id, nic,
    // gpu) key either way, so the check is keyed consistently.
    if (dst_gpu >= 0) {
        const auto* rnic = target.topo->getNicEntry(tdev);
        if (rnic && !gdr.remoteReachable(target.segment->machine_id, rnic->name,
                                         dst_gpu))
            return true;
    }
    return false;
}

Status Workers::selectFallbackDevice(RouteHint& source, RouteHint& target,
                                     RdmaSlice* slice) {
    LOG_EVERY_N(INFO, 100) << "fallback device selection for slice " << slice;
    bool same_machine =
        (source.segment->machine_id == target.segment->machine_id);

    // GPUDirect reachability filtering (only when something has been learned).
    bool gdr_learned = GdrReachability::hasAnyExclusion();
    int src_gpu = -1, dst_gpu = -1;
    if (gdr_learned) {
        LocationParser s(source.location), d(target.location);
        if (s.type() == "cuda") src_gpu = s.index();
        if (d.type() == "cuda") dst_gpu = d.index();
    }

    size_t src_total = 0;
    for (size_t srank = 0; srank < Topology::DevicePriorityRanks; ++srank)
        src_total += source.topo_entry->device_list[srank].size();

    size_t dst_total = 0;
    for (size_t trank = 0; trank < Topology::DevicePriorityRanks; ++trank)
        dst_total += target.topo_entry->device_list[trank].size();

    size_t total_combos = src_total * dst_total;
    if (total_combos == 0)
        return Status::DeviceNotFound("No available path" LOC_MARK);

    // Rotate through source/target combinations with wraparound, resuming just
    // past the pair this slice tried last (last_fallback_idx, seeded to -1 so
    // the first fallback starts at flat index 0). This keeps a retry from
    // immediately re-picking the same path -- a non-GDR failure would otherwise
    // burn the whole retry budget on one path before RailMonitor's error
    // threshold excludes it -- while still preferring higher-priority pairs:
    // getDeviceByFlatIndex walks the per-GPU priority-ranked NIC list, so flat
    // index 0 is (source PIX NIC, target PIX NIC), the ideal GPUDirect-capable
    // pair. Rail-down and GDR-excluded pairs are skipped, so the scan converges
    // onto a reachable rail instead of exhausting retries.
    auto& worker = worker_context_[tl_wid];
    RailMonitor* rail_mon =
        same_machine
            ? nullptr
            : &getOrCreateRail(worker.rails, target.segment->machine_id);
    size_t start =
        static_cast<size_t>(slice->last_fallback_idx + 1) % total_combos;
    const uint64_t device_mask = slice->task->device_mask;
    for (size_t k = 0; k < total_combos; ++k) {
        size_t idx = (start + k) % total_combos;
        size_t src_idx = idx / dst_total;
        size_t dst_idx = idx % dst_total;
        int sdev = getDeviceByFlatIndex(source, src_idx);
        int tdev = getDeviceByFlatIndex(target, dst_idx);
        if (sdev < 0 || sdev >= 64 || (device_mask & (1ULL << sdev)) == 0)
            continue;
        if (strictLocalNuma() &&
            source.topo->isCrossNuma(*source.topo_entry, sdev))
            continue;
        bool reachable = same_machine ? (sdev == tdev)  // loopback is safe
                                      : rail_mon->available(sdev, tdev);

        // Skip NICs that cannot GPUDirect-DMA to the source/target GPU.
        if (reachable && gdr_learned &&
            gdrPairExcluded(source, target, sdev, tdev, src_gpu, dst_gpu))
            reachable = false;

        if (reachable) {
            // A retry gets here after the failure path returned the slice's
            // charge, so charge the device this attempt will actually use:
            // otherwise the NIC's inflight bytes miss it and its completion
            // teaches neither bandwidth estimate.
            rechargeSlice(slice, sdev);
            slice->source_dev_id = sdev;
            slice->target_dev_id = tdev;
            // Keys are assigned by generatePostPath() once the device pair is
            // settled.
            slice->last_fallback_idx = static_cast<int>(idx);
            return Status::OK();
        }
    }

    return Status::DeviceNotFound("No available path" LOC_MARK);
}

Status Workers::generatePostPath(RdmaSlice* slice) {
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
    // Keys are NicID-indexed. A peer running an older build publishes a
    // compacted rkey vector, so a NicID from its device_list can point past the
    // end; fail the slice instead of reading out of bounds.
    const auto& lkeys = source.buffer->lkey;
    const auto& rkeys = target.buffer->rkey;
    if (slice->source_dev_id < 0 ||
        (size_t)slice->source_dev_id >= lkeys.size() ||
        slice->target_dev_id < 0 ||
        (size_t)slice->target_dev_id >= rkeys.size())
        return Status::DeviceNotFound(
            "Selected device has no registered memory key" LOC_MARK);
    slice->source_lkey = lkeys[slice->source_dev_id];
    slice->target_rkey = rkeys[slice->target_dev_id];
    // Cache the RailMonitor pointer so asyncPollCq / disableEndpoint can
    // update rail state without a segment lookup or string-keyed map
    // lookup on the hot path.
    slice->rail_monitor = &getOrCreateRail(worker_context_[tl_wid].rails,
                                           target.segment->machine_id);
    // Stash identifiers for GPUDirect reachability learning in asyncPollCq.
    // The name pointers alias stable Topology::NicEntry / segment storage and
    // remain valid for the slice's lifetime.
    {
        LocationParser s(source.location), d(target.location);
        slice->source_gpu_ordinal = (s.type() == "cuda") ? s.index() : -1;
        slice->target_gpu_ordinal = (d.type() == "cuda") ? d.index() : -1;
        const auto* lnic = source.topo->getNicEntry(slice->source_dev_id);
        const auto* rnic = target.topo->getNicEntry(slice->target_dev_id);
        slice->source_nic_name = lnic ? lnic->name.c_str() : nullptr;
        slice->target_nic_name = rnic ? rnic->name.c_str() : nullptr;
        slice->target_machine_id = &target.segment->machine_id;
    }
    if (transport_->params_->log_slice_affinity) {
        const auto* local_nic = source.topo->getNicEntry(slice->source_dev_id);
        const auto* remote_nic = target.topo->getNicEntry(slice->target_dev_id);
        VLOG(1) << "RDMA slice affinity: source_location=" << source.location
                << ", target_location=" << target.location
                << ", local_device_name="
                << (local_nic ? local_nic->name : "<unknown>")
                << ", peer_device_name="
                << (remote_nic ? remote_nic->name : "<unknown>")
                << ", target_id=" << slice->task->request.target_id
                << ", source_addr=" << static_cast<void*>(slice->source_addr)
                << ", dest_addr=" << reinterpret_cast<void*>(slice->target_addr)
                << ", length=" << slice->length
                << ", retry_count=" << slice->retry_count;
    }
    return Status::OK();
}
}  // namespace tent
}  // namespace mooncake
