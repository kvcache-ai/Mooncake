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
#include <chrono>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>

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
struct ArbitrationEntry {
    RdmaSlice* slice;
    double mlu;
    size_t order;
};

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
      accepting_submits_(true),
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
        accepting_submits_.store(true, std::memory_order_release);
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
    accepting_submits_.store(false, std::memory_order_release);
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

Status Workers::quiesce(uint64_t timeout_ns) {
    accepting_submits_.store(false, std::memory_order_release);
    if (!running_.load(std::memory_order_acquire) || !worker_context_ ||
        num_workers_ == 0) {
        return Status::OK();
    }

    for (size_t id = 0; id < num_workers_; ++id) {
        auto& worker = worker_context_[id];
        std::lock_guard<std::mutex> lock(worker.mutex);
        if (worker.in_suspend) worker.cv.notify_all();
    }

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::nanoseconds(timeout_ns);
    while (true) {
        int64_t inflight = 0;
        for (size_t id = 0; id < num_workers_; ++id) {
            inflight += worker_context_[id].inflight_slices.load(
                std::memory_order_acquire);
        }
        if (inflight == 0) return Status::OK();
        if (std::chrono::steady_clock::now() >= deadline) {
            return Status::InternalError("RDMA quiesce timed out with " +
                                         std::to_string(inflight) +
                                         " inflight slices" LOC_MARK);
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}

Status Workers::submit(RdmaSliceList& slice_list, int worker_id) {
    if (!accepting_submits_.load(std::memory_order_acquire)) {
        return Status::InternalError("RDMA transport is quiescing" LOC_MARK);
    }
    if (!running_.load(std::memory_order_acquire) || !worker_context_ ||
        num_workers_ == 0) {
        return Status::InternalError("RDMA workers are not running" LOC_MARK);
    }
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
    // The worker must never block on its own queue (issue #3637): a full
    // queue parks the slice in requeue_overflow and the next tick retries.
    // Either way it stays counted as inflight, which keeps the worker from
    // suspending while a parked flush is pending.
    if (!worker.queues[priority].try_push(slice_list)) {
        worker.requeue_overflow.emplace_back(priority, slice_list);
    }
    worker.inflight_slices.fetch_add(1);
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

bool Workers::cancelUnpostedSlice(WorkerContext& worker, RdmaSlice* slice) {
    if (!slice || !slice->task ||
        !slice->task->cancel_requested.load(std::memory_order_acquire))
        return false;
    if (slice->word == PENDING) {
        releaseSliceQuota(device_selector_.get(), slice);
        updateSliceStatus(slice, CANCELED);
    }
    worker.inflight_slices.fetch_sub(1);
    return true;
}

void Workers::releaseSliceQuota(DeviceSelector* selector, RdmaSlice* slice,
                                double latency) {
    if (!slice || !slice->quota_charged || !selector) return;
    // Release against the device the bytes were actually charged on, and unwind
    // exactly the bytes that were charged, not the current routing NIC or the
    // slice length: a fallback re-route can leave source_dev_id pointing at a
    // different device, and the allocator's per-slice estimate can differ from
    // slice->length.
    selector->release(slice->charged_dev_id, slice->charged_bytes, latency);
    slice->quota_charged = false;
    slice->charged_dev_id = -1;
    slice->charged_bytes = 0;
}

void Workers::chargeSliceQuota(DeviceSelector* selector, RdmaSlice* slice) {
    // Reconcile the inflight charge so it lands on the device the slice will
    // actually post on (slice->source_dev_id) and reflects the slice's real
    // length. This keeps DeviceSelector's per-NIC inflight view both symmetric
    // with releaseSliceQuota and an accurate load signal, across three paths
    // that would otherwise skew local telemetry:
    //   - initial allocate: the aggregated allocator charges a per-slice
    //     estimate (ceil(total/num_slices)); convert it to the exact length so
    //     inflight equals the bytes really put on the wire.
    //   - fallback re-route: the charge was made on the originally selected NIC
    //     but the slice now posts on a different one -> migrate the charge.
    //   - retry: the previous attempt released the quota, so re-enter the
    //     inflight view instead of running uncounted.
    if (!selector || slice->source_dev_id < 0) return;
    if (slice->quota_charged && slice->charged_dev_id == slice->source_dev_id &&
        slice->charged_bytes == slice->length)
        return;  // already charged exactly this slice's length on the right NIC
    if (slice->quota_charged) {
        // Stale device and/or estimate -> unwind exactly what was charged
        // first. Order is deliberate: releasing before charging makes inflight
        // dip slightly below reality for the instant between the two calls,
        // whereas charging first would transiently double-count during a
        // fallback migration. inflight is only a scoring signal (not an
        // admission gate), so a momentary dip merely perturbs one selection,
        // while a double-count would over-penalize the NIC -- the dip is the
        // lesser, intended bias.
        selector->release(slice->charged_dev_id, slice->charged_bytes, 0.0);
        slice->quota_charged = false;
        slice->charged_dev_id = -1;
        slice->charged_bytes = 0;
    }
    // Charge this slice's real length on the routing NIC. Only commit the
    // bookkeeping if the charge actually succeeded, so a failed charge is never
    // released later (which would underflow the counter).
    Status status = selector->chargeDevice(slice->source_dev_id, slice->length);
    if (status.ok()) {
        slice->charged_dev_id = slice->source_dev_id;
        slice->charged_bytes = slice->length;
        slice->quota_charged = true;
    } else {
        // The routing NIC is not tracked by DeviceSelector (a topology/quota
        // mismatch, e.g. a device that never entered devices_). The data
        // transfer can still proceed, so we do not fail the slice, but the
        // slice runs without inflight accounting -- surface it (rate-limited on
        // the hot path) instead of silently under-counting the device's load.
        LOG_EVERY_N(WARNING, 100)
            << "chargeSliceQuota: source_dev_id " << slice->source_dev_id
            << " is not tracked by DeviceSelector; slice " << slice
            << " proceeds without inflight accounting: " << status.ToString();
    }
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
            if (cancelUnpostedSlice(worker, slice)) {
                slice = slice->next;
                continue;
            }
            auto status = generatePostPath(slice);
            if (!status.ok()) {
                LOG(ERROR) << "Failed to generate post path for slice " << slice
                           << ": " << status.ToString();
                releaseSliceQuota(device_selector_.get(), slice);
                updateSliceStatus(slice, slice->task->cancel_requested.load(
                                             std::memory_order_acquire)
                                             ? CANCELED
                                             : FAILED);
                worker.inflight_slices.fetch_sub(1);
            } else if (cancelUnpostedSlice(worker, slice)) {
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
                                        return cancelUnpostedSlice(worker,
                                                                   slice);
                                    }),
                     slices.end());
        if (slices.empty()) continue;
        auto endpoint = getEndpoint(path);
        if (!endpoint) {
            std::vector<RdmaSlice*> clone;
            slices.swap(clone);
            for (auto slice : clone) {
                if (cancelUnpostedSlice(worker, slice)) continue;
                slice->retry_count++;
                if (slice->retry_count >=
                    transport_->params_->workers.max_retry_count) {
                    LOG(WARNING)
                        << "Slice " << slice << " failed: retry count exceeded";
                    disableEndpoint(slice);
                    releaseSliceQuota(device_selector_.get(), slice);
                    updateSliceStatus(slice, FAILED);
                } else {
                    releaseSliceQuota(device_selector_.get(), slice);
                    submitFromTick(worker, slice);
                }
                worker.inflight_slices.fetch_sub(1);
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
        if (deadline_bw_arbitration_ && slices.size() > 1) {
            const uint64_t now_ns = getCurrentTimeInNano();
            const double bw_bps = device_selector_
                                      ? device_selector_->getSchedulingParams()
                                                .default_bandwidth_gbps *
                                            1e9 / 8.0
                                      : 0.0;
            if (bw_bps > 0.0) {
                thread_local std::vector<ArbitrationEntry> scratch;
                scratch.clear();
                scratch.reserve(slices.size());

                for (size_t i = 0; i < slices.size(); ++i) {
                    const RdmaSlice* s = slices[i];
                    ArbFlow flow{0, 0};
                    if (s && s->task) {
                        flow = ArbFlow{s->task->request.deadline_ns, s->length};
                    }
                    scratch.push_back(ArbitrationEntry{
                        slices[i], PredictedMlu(flow, now_ns, bw_bps), i});
                }

                std::sort(
                    scratch.begin(), scratch.end(),
                    [](const ArbitrationEntry& a, const ArbitrationEntry& b) {
                        if (a.mlu > b.mlu) return true;
                        if (a.mlu < b.mlu) return false;
                        return a.order < b.order;
                    });
                for (size_t i = 0; i < scratch.size(); ++i) {
                    slices[i] = scratch[i].slice;
                }
            }
        }

        int num_submitted = endpoint->submitSlices(slices, tl_wid);
        for (int id = 0; id < num_submitted; ++id) {
            auto slice = slices[id];
            if (slice->failed) {
                releaseSliceQuota(device_selector_.get(), slice);
                if (slice->task->cancel_requested.load(
                        std::memory_order_acquire)) {
                    updateSliceStatus(slice, CANCELED);
                    worker.inflight_slices.fetch_sub(1);
                    continue;
                }
                slice->retry_count++;
                if (slice->retry_count >=
                    transport_->params_->workers.max_retry_count) {
                    LOG(WARNING)
                        << "Slice " << slice << " failed: retry count exceeded";
                    disableEndpoint(slice);
                    updateSliceStatus(slice, FAILED);
                } else {
                    submitFromTick(worker, slice);
                }
                worker.inflight_slices.fetch_sub(1);
            } else {
                slice->submit_ts = getCurrentTimeInNano();
                worker.inflight_slice_set.insert(slice);
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

void Workers::asyncPollCq() {
    auto& worker = worker_context_[tl_wid];
    const static size_t kPollCount = 64;
    int num_contexts = (int)transport_->context_set_.size();
    int num_cq_list = transport_->params_->device.num_cq_list;
    int num_slices = 0;

    uint64_t current_ts = getCurrentTimeInNano();
    std::vector<RdmaSlice*> slice_to_remove;
    for (auto& slice : worker.inflight_slice_set) {
        if (slice->word != PENDING) continue;
        if (current_ts - slice->enqueue_ts > slice_timeout_ns_) {
            auto ep = slice->ep_weak_ptr.lock();
            LOG(WARNING) << "Slice " << slice
                         << " failed: transfer timeout (software)";
            // A software timeout is terminal (no retry), so release the
            // inflight charge here or it leaks on charged_dev_id forever. No
            // latency sample: a timeout is not a valid bandwidth observation.
            releaseSliceQuota(device_selector_.get(), slice);
            if (!ep) {
                updateSliceStatus(slice, TIMEOUT);
                slice_to_remove.push_back(slice);
                worker.inflight_slices.fetch_sub(1);
                continue;
            }
            auto num_slices = ep->acknowledge(slice, TIMEOUT);
            disableEndpoint(slice);
            worker.inflight_slices.fetch_sub(num_slices);
            slice_to_remove.push_back(slice);
        }
    }
    for (auto& slice : slice_to_remove) worker.inflight_slice_set.erase(slice);

    for (int index = 0; index < num_contexts; index++) {
        auto& context = transport_->context_set_[index];
        auto cq = context->cq(tl_wid % num_cq_list);
        if (!cq) continue;  // inert context for a non-RDMA or failed NIC
        ibv_wc wc[kPollCount];
        int nr_poll = cq->poll(kPollCount, wc);
        if (nr_poll < 0) continue;
        auto poll_ts = getCurrentTimeInNano();
        for (int i = 0; i < nr_poll; ++i) {
            auto slice = (RdmaSlice*)wc[i].wr_id;
            worker.inflight_slice_set.erase(slice);
            auto ep = slice->ep_weak_ptr.lock();
            double enqueue_lat =
                (slice->submit_ts - slice->enqueue_ts) / 1000.0;
            double inflight_lat = (poll_ts - slice->submit_ts) / 1000.0;
            // EWMA bandwidth must learn only from successful transfers, and
            // only from the current NIC attempt's inflight time -- not the
            // cumulative time since first enqueue, which folds in queueing
            // delay and prior failed attempts on other NICs and would bias the
            // estimate low. A failed/flushed WC or an already-resolved slice
            // contributes no sample (latency 0), so releaseSliceQuota only
            // frees the charge.
            bool ewma_sample =
                ep && slice->word == PENDING && wc[i].status == IBV_WC_SUCCESS;
            double sample_lat_sec =
                ewma_sample ? (poll_ts - slice->submit_ts) / 1e9 : 0.0;
            releaseSliceQuota(device_selector_.get(), slice, sample_lat_sec);
            if (slice->word != PENDING) continue;
            if (!ep) {
                updateSliceStatus(slice, FAILED);
                num_slices++;
                continue;
            }
            if (wc[i].status != IBV_WC_SUCCESS) {
                if (wc[i].status != IBV_WC_WR_FLUSH_ERR) {
                    // TE handles them automatically
                    LOG(INFO) << "Detected error WQE for slice " << slice
                              << " (opcode: " << slice->task->request.opcode
                              << ", source_addr: " << (void*)slice->source_addr
                              << ", dest_addr: " << (void*)slice->target_addr
                              << ", length: " << slice->length
                              << ", local_nic: " << context->name()
                              << "): " << ibv_wc_status_str(wc[i].status);
                }
                // GPUDirect reachability learning: a protection/access error
                // on a GPU buffer means the chosen NIC cannot P2P-DMA to that
                // GPU (the MR was created but the PCIe path is unusable).
                // Record it so selection avoids that NIC and converges onto a
                // reachable rail instead of exhausting retries. The local side
                // (source NIC -> source GPU) surfaces as LOC_PROT; the remote
                // side (target NIC -> target GPU) as REM_ACCESS or, for a
                // remote GDR-read failure, REM_OP (observed on strict fabrics).
                bool local_gdr_err = (wc[i].status == IBV_WC_LOC_PROT_ERR);
                bool remote_gdr_err = (wc[i].status == IBV_WC_REM_ACCESS_ERR ||
                                       wc[i].status == IBV_WC_REM_OP_ERR);
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
                    LOG(WARNING)
                        << "Slice " << slice << " failed: retry count exceeded";
                    num_slices += ep->acknowledge(slice, FAILED);
                    disableEndpoint(slice);
                } else {
                    num_slices += ep->acknowledge(slice, PENDING);
                    disableEndpoint(slice);
                    if (slice->task->cancel_requested.load(
                            std::memory_order_acquire)) {
                        updateSliceStatus(slice, CANCELED);
                    } else {
                        submitFromTick(worker, slice);
                    }
                }
            } else {
                num_slices += ep->acknowledge(slice, COMPLETED);
                // A successful GPU transfer re-admits any learned GDR
                // unreachability for the (GPU, NIC) pair(s) it used, so a
                // transient exclusion (or a recovered path) heals. Skipped
                // entirely until something has actually been excluded.
                if (GdrReachability::hasAnyExclusion()) {
                    auto& gdr = GdrReachability::instance();
                    if (slice->source_gpu_ordinal >= 0 &&
                        slice->source_nic_name)
                        gdr.reportLocalSuccess(slice->source_nic_name,
                                               slice->source_gpu_ordinal);
                    if (slice->target_gpu_ordinal >= 0 &&
                        slice->target_nic_name && slice->target_machine_id)
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
                    rail->markRecovered(slice->source_dev_id,
                                        slice->target_dev_id);
                if (transport_->params_->workers.show_latency_info) {
                    worker.perf.inflight_lat.add(inflight_lat);
                    worker.perf.enqueue_lat.add(enqueue_lat);
                }
            }
        }
    }
    if (num_slices) {
        worker.inflight_slices.fetch_sub(num_slices);
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
        slice->quota_charged = true;
        slice->charged_dev_id = slice->source_dev_id;
        // Single-slice allocate charges exactly slice->length on the chosen
        // device.
        slice->charged_bytes = slice->length;
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
    // The routing NIC is now final for this (re)submit. Reconcile the inflight
    // charge onto it so a fallback re-route or a retry does not leave the
    // original NIC charged (residue) or run uncounted.
    chargeSliceQuota(device_selector_.get(), slice);
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
