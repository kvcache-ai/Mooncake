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

#include "tent_backend.h"
#include "utils.h"
#include "char_util.h"
#include "receiver_credit_metrics.h"
#include "tent/common/types.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/topology.h"
#include "tent/runtime/transport_selector.h"

#include <algorithm>
#include <chrono>
#include <deque>
#include <limits>
#include <unordered_map>
#include <unordered_set>

#include "tent/thirdparty/nlohmann/json.h"

#if defined(USE_CUDA) || defined(USE_SUNRISE)
#include "cuda_alike.h"
#endif

#ifdef USE_HIP
#include <hip/hip_runtime.h>
#endif

namespace mooncake {
namespace tent {

namespace {

constexpr const char* kReceiverCreditDemand = "receiver-credit-demand-v1";
constexpr const char* kReceiverCreditRelease = "receiver-credit-release-v1";
constexpr const char* kReceiverCreditGrant = "receiver-credit-grant-v1";
constexpr useconds_t kReceiverCreditPollIntervalUs = 10;

uint64_t steadyNowUs() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::microseconds>(now).count();
}

struct ReceiverDemand {
    std::string sender_segment;
    uint64_t request_id{0};
    uint64_t bytes{0};
    uint64_t slots{0};
    uint64_t arrival_us{0};
};

struct DelayedReceiverRelease {
    ReceiverDemand demand;
    uint64_t due_us{0};
};

std::string demandKey(const std::string& sender_segment, uint64_t request_id) {
    return sender_segment + "#" + std::to_string(request_id);
}

double p99(std::vector<double> samples) {
    if (samples.empty()) return 0.0;
    std::sort(samples.begin(), samples.end());
    const double rank = 0.99 * static_cast<double>(samples.size() - 1);
    const size_t index = static_cast<size_t>(rank);
    const double fraction = rank - static_cast<double>(index);
    if (index + 1 == samples.size()) return samples[index];
    return samples[index] * (1.0 - fraction) + samples[index + 1] * fraction;
}

}  // namespace

volatile bool g_tent_running = true;
volatile bool g_tent_triggered_sig = false;

void signalHandlerV1(int signum) {
    if (g_tent_triggered_sig) {
        LOG(ERROR) << "Received signal " << signum
                   << " again, forcefully terminating...";
        std::exit(EXIT_FAILURE);
    }
    LOG(INFO) << "Received signal " << signum << ", stopping target server...";
    g_tent_running = false;
    g_tent_triggered_sig = true;
}

std::shared_ptr<Config> loadConfig() {
    auto config = std::make_shared<Config>();
    config->set("local_segment_name", XferBenchConfig::seg_name);
    config->set("metadata_type", XferBenchConfig::metadata_type);
    config->set("metadata_servers", XferBenchConfig::metadata_url_list);
    config->set("rpc_server_port", XferBenchConfig::rpc_server_port);

    // Configure transport types based on xport_type parameter
    if (!XferBenchConfig::xport_type.empty()) {
        // Map of transport names to their config keys (handle name mismatches)
        std::unordered_map<std::string, std::string> transport_map = {
            {"rdma", "rdma"},
            {"tcp", "tcp"},
            {"shm", "shm"},
            {"iouring", "io_uring"},  // Note: iouring -> io_uring
            {"gds", "gds"},
            {"mnnvl", "mnnvl"},
            {"nvlink", "nvlink"},
            {"sunrise_link", "sunrise_link"}};

        // Disable all transports by default
        for (const auto& entry : transport_map) {
            config->set("transports/" + entry.second + "/enable", false);
        }

        // Enable only the specified transport
        auto it = transport_map.find(XferBenchConfig::xport_type);
        if (it != transport_map.end()) {
            config->set("transports/" + it->second + "/enable", true);
        }
    }

    return config;
}

static TransportType getTransportType(const std::string& xport_type) {
    if (xport_type == "rdma") return RDMA;
    if (xport_type == "shm") return SHM;
    if (xport_type == "gds") return GDS;
    if (xport_type == "mnnvl") return MNNVL;
    if (xport_type == "nvlink") return NVLINK;
    if (xport_type == "tcp") return TCP;
    if (xport_type == "iouring") return IOURING;
    if (xport_type == "sunrise_link") return SUNRISE_LINK;
    return UNSPEC;
}

int TENTBenchRunner::allocateBuffers() {
    const auto total_buffer_size = XferBenchConfig::total_buffer_size;
    const auto& seg_type = XferBenchConfig::seg_type;
    const auto& xport_type = XferBenchConfig::xport_type;

    // Resolve device prefix, start index, and buffer count per seg_type
    std::string device_prefix;
    int start_idx = 0, num_buffers = 0;

    if (seg_type == "DRAM") {
        device_prefix = "cpu";
        num_buffers = numa_num_configured_nodes();
#if defined(USE_CUDA) || defined(USE_SUNRISE)
    } else if (seg_type == "VRAM") {
        device_prefix = "cuda";
        int gpu_count = 0;
        auto err = cudaGetDeviceCount(&gpu_count);
        LOG_ASSERT(err == cudaSuccess && gpu_count > 0)
            << "cudaGetDeviceCount failed: " << cudaGetErrorString(err);
        start_idx = 0;
        num_buffers = gpu_count;
        if (XferBenchConfig::local_gpu_id != -1) {
            start_idx = XferBenchConfig::local_gpu_id;
            num_buffers = 1;
            LOG_ASSERT(start_idx >= 0 && start_idx < gpu_count)
                << "local_gpu_id " << start_idx << " out of range [0, "
                << gpu_count << ")";
        }
#elif defined(USE_HIP)
    } else if (seg_type == "VRAM") {
        device_prefix = "rocm";
        int gpu_count = 0;
        hipGetDeviceCount(&gpu_count);
        start_idx = 0;
        num_buffers = gpu_count;
        if (XferBenchConfig::local_gpu_id != -1) {
            start_idx = XferBenchConfig::local_gpu_id;
            num_buffers = 1;
            LOG_ASSERT(start_idx >= 0 && start_idx < gpu_count)
                << "local_gpu_id " << start_idx << " out of range [0, "
                << gpu_count << ")";
        }
#endif
    } else {
        LOG(ERROR) << "Unknown seg_type: " << seg_type;
        return -1;
    }

    pinned_buffer_list_.resize(num_buffers, nullptr);
    uint64_t alloc_ns = 0, reg_ns = 0;
    for (int i = 0; i < num_buffers; ++i) {
        auto location = device_prefix + ":" + std::to_string(start_idx + i);
        MemoryOptions options;
        if (!xport_type.empty()) {
            options.type = getTransportType(xport_type);
            options.location = location;
        }

        auto t0 = getCurrentTimeInNano();
        if (!xport_type.empty()) {
            options.location = location;
            CHECK_FAIL(engine_->allocateLocalMemory(
                &pinned_buffer_list_[i], total_buffer_size, options));
        } else {
            CHECK_FAIL(engine_->allocateLocalMemory(
                &pinned_buffer_list_[i], total_buffer_size, location));
        }
        auto t1 = getCurrentTimeInNano();

#ifdef USE_SUNRISE
        if (seg_type == "VRAM") {
            auto err = cudaSetDevice(start_idx + i);
            CHECK_FAIL(err == cudaSuccess ? Status::OK()
                                          : Status::InternalError(
                                                "Failed to set Sunrise device "
                                                "before registerLocalMemory"));
        }
#endif
        CHECK_FAIL(engine_->registerLocalMemory(pinned_buffer_list_[i],
                                                total_buffer_size, options));
        auto t2 = getCurrentTimeInNano();

        alloc_ns += (t1 - t0);
        reg_ns += (t2 - t1);
    }

    LOG(INFO) << "Allocated " << total_buffer_size * num_buffers << " bytes "
              << seg_type << " buffers in " << alloc_ns / 1e6
              << " ms, registered in " << reg_ns / 1e6 << " ms";
    return 0;
}

int TENTBenchRunner::freeBuffers() {
    auto total_buffer_size = XferBenchConfig::total_buffer_size;
    for (size_t i = 0; i < pinned_buffer_list_.size(); ++i) {
        CHECK_FAIL(engine_->unregisterLocalMemory(pinned_buffer_list_[i],
                                                  total_buffer_size));
        CHECK_FAIL(engine_->freeLocalMemory(pinned_buffer_list_[i]));
    }
    pinned_buffer_list_.clear();
    return 0;
}

TENTBenchRunner::TENTBenchRunner() {
    signal(SIGINT, signalHandlerV1);
    signal(SIGTERM, signalHandlerV1);
    engine_ = std::make_unique<TransferEngine>(loadConfig());
    transport_hint_ = TransportSelector::parseTransportType(
        XferBenchConfig::tent_transport_hint);
    allocateBuffers();
}

TENTBenchRunner::~TENTBenchRunner() { freeBuffers(); }

int TENTBenchRunner::runTarget() {
    const auto& mode = XferBenchConfig::receiver_credit_mode;
    if (mode == "disabled") {
        while (g_tent_running) sleep(1);
        return 0;
    }

    ReceiverCapacityTracker tracker(XferBenchConfig::receiver_capacity_bytes,
                                    XferBenchConfig::receiver_capacity_slots);
    std::unordered_map<std::string, ReceiverDemand> active;
    std::unordered_set<std::string> seen;
    std::deque<ReceiverDemand> pending;
    std::deque<DelayedReceiverRelease> delayed;
    std::unordered_map<std::string, SegmentID> sender_handles;
    std::vector<double> completion_latency_us;
    uint64_t offered = 0;
    uint64_t completed = 0;
    uint64_t completed_bytes = 0;
    uint64_t data_errors = 0;
    uint64_t first_demand_us = 0;
    uint64_t last_completion_us = 0;

    auto sendGrant = [&](const ReceiverDemand& demand) -> bool {
        auto handle = sender_handles.find(demand.sender_segment);
        if (handle == sender_handles.end()) {
            SegmentID sender_handle = 0;
            auto status =
                engine_->openSegment(sender_handle, demand.sender_segment);
            if (!status.ok()) {
                LOG(ERROR) << "Failed to open receiver-credit sender segment "
                           << demand.sender_segment << ": "
                           << status.ToString();
                return false;
            }
            handle =
                sender_handles.emplace(demand.sender_segment, sender_handle)
                    .first;
        }
        nlohmann::json payload = {{"schema_version", 1},
                                  {"request_id", demand.request_id},
                                  {"slots", demand.slots}};
        auto status = engine_->sendNotification(
            handle->second, Notification{kReceiverCreditGrant, payload.dump()});
        if (!status.ok()) {
            LOG(ERROR) << "Failed to send receiver-credit grant: "
                       << status.ToString();
            return false;
        }
        return true;
    };

    auto admit = [&](const ReceiverDemand& demand, bool enforce) -> bool {
        if (!tracker.tryReserve(demand.bytes, demand.slots, enforce)) {
            return false;
        }
        active.emplace(demandKey(demand.sender_segment, demand.request_id),
                       demand);
        if (enforce && !sendGrant(demand)) {
            ++data_errors;
            active.erase(demandKey(demand.sender_segment, demand.request_id));
            if (!tracker.release(demand.bytes, demand.slots)) ++data_errors;
            return false;
        }
        return true;
    };

    auto grantPending = [&]() {
        while (!pending.empty()) {
            const auto demand = pending.front();
            if (!admit(demand, true)) break;
            pending.pop_front();
        }
    };

    auto processDueReleases = [&]() {
        const uint64_t now_us = steadyNowUs();
        bool released = false;
        while (!delayed.empty() && delayed.front().due_us <= now_us) {
            auto release = std::move(delayed.front());
            delayed.pop_front();
            if (!tracker.release(release.demand.bytes,
                                 release.demand.slots)) {
                ++data_errors;
            } else {
                completed += release.demand.slots;
                completed_bytes += release.demand.bytes;
                last_completion_us = now_us;
                completion_latency_us.push_back(
                    static_cast<double>(now_us - release.demand.arrival_us));
            }
            released = true;
        }
        if (released && mode == "credit") grantPending();
    };

    auto processNotifications = [&]() {
        std::vector<Notification> notifications;
        auto status = engine_->receiveNotification(notifications);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to receive receiver-credit notification: "
                       << status.ToString();
            ++data_errors;
            return;
        }
        for (const auto& notification : notifications) {
            if (notification.name != kReceiverCreditDemand &&
                notification.name != kReceiverCreditRelease) {
                continue;
            }
            try {
                const auto payload = nlohmann::json::parse(notification.msg);
                if (payload.value("schema_version", 0) != 1) {
                    ++data_errors;
                    continue;
                }
                const auto sender = payload.value("sender_segment", "");
                const auto request_id = payload.value("request_id", 0ull);
                const auto key = demandKey(sender, request_id);
                if (notification.name == kReceiverCreditDemand) {
                    ReceiverDemand demand{
                        sender, request_id, payload.value("bytes", 0ull),
                        payload.value("slots", 0ull), steadyNowUs()};
                    if (sender.empty() || request_id == 0 ||
                        demand.bytes == 0 || demand.slots == 0 ||
                        payload.value("mode", "") != mode ||
                        !seen.emplace(key).second) {
                        ++data_errors;
                        continue;
                    }
                    if (first_demand_us == 0)
                        first_demand_us = demand.arrival_us;
                    offered += demand.slots;
                    if (mode == "fixed") {
                        if (!admit(demand, false)) ++data_errors;
                    } else if (!admit(demand, true)) {
                        pending.push_back(std::move(demand));
                    }
                } else if (notification.name == kReceiverCreditRelease) {
                    auto active_it = active.find(key);
                    const auto release_bytes = payload.value("bytes", 0ull);
                    const auto release_slots = payload.value("slots", 0ull);
                    if (active_it == active.end() || release_bytes == 0 ||
                        release_slots == 0 ||
                        release_bytes > active_it->second.bytes ||
                        release_slots > active_it->second.slots) {
                        ++data_errors;
                        continue;
                    }
                    delayed.push_back(DelayedReceiverRelease{
                        ReceiverDemand{active_it->second.sender_segment,
                                       active_it->second.request_id,
                                       release_bytes, release_slots,
                                       active_it->second.arrival_us},
                        steadyNowUs() +
                            XferBenchConfig::receiver_consumer_delay_us});
                    active_it->second.bytes -= release_bytes;
                    active_it->second.slots -= release_slots;
                    if ((active_it->second.bytes == 0) !=
                        (active_it->second.slots == 0)) {
                        ++data_errors;
                    }
                    if (active_it->second.bytes == 0 &&
                        active_it->second.slots == 0) {
                        active.erase(active_it);
                    }
                }
            } catch (const std::exception& error) {
                LOG(ERROR) << "Malformed receiver-credit notification: "
                           << error.what();
                ++data_errors;
            }
        }
    };

    while (g_tent_running) {
        processNotifications();
        processDueReleases();
        usleep(kReceiverCreditPollIntervalUs);
    }

    const uint64_t drain_deadline_us =
        steadyNowUs() +
        std::max<uint64_t>(5000000,
                           XferBenchConfig::receiver_consumer_delay_us * 2);
    while ((!active.empty() || !pending.empty() || !delayed.empty()) &&
           steadyNowUs() < drain_deadline_us) {
        processNotifications();
        processDueReleases();
        usleep(kReceiverCreditPollIntervalUs);
    }
    if (!active.empty() || !pending.empty() || !delayed.empty()) {
        data_errors += active.size() + pending.size() + delayed.size();
    }

    const double elapsed_us =
        last_completion_us > first_demand_us
            ? static_cast<double>(last_completion_us - first_demand_us)
            : 0.0;
    ReceiverCreditRunReport report;
    report.run_id = XferBenchConfig::receiver_credit_run_id;
    report.mode = mode;
    report.condition = XferBenchConfig::receiver_credit_condition;
    report.sender_count = XferBenchConfig::receiver_credit_sender_count;
    report.repetition = XferBenchConfig::receiver_credit_repetition;
    report.offered = offered;
    report.completed = completed;
    report.data_errors = data_errors;
    report.throughput_gbps =
        elapsed_us > 0.0 ? completed_bytes / elapsed_us / 1000.0 : 0.0;
    report.p99_us = p99(completion_latency_us);
    report.oracle_throughput_gbps =
        XferBenchConfig::receiver_credit_oracle_throughput_gbps;
    report.receiver = tracker.snapshot();

    LOG(INFO) << "RECEIVER_CREDIT_RESULT mode=" << report.mode
              << " senders=" << report.sender_count
              << " offered=" << report.offered
              << " completed=" << report.completed << " capacity_violations="
              << report.receiver.capacity_violation_total
              << " peak_bytes=" << report.receiver.peak_bytes
              << " peak_slots=" << report.receiver.peak_slots
              << " throughput_gbps=" << report.throughput_gbps
              << " p99_us=" << report.p99_us
              << " data_errors=" << report.data_errors;
    if (!XferBenchConfig::receiver_credit_output_jsonl.empty()) {
        std::string error;
        if (!appendReceiverCreditRunJsonl(
                XferBenchConfig::receiver_credit_output_jsonl, report,
                &error)) {
            LOG(ERROR) << error;
            return -1;
        }
    }
    return 0;
}

int TENTBenchRunner::beginReceiverCreditTransfer(uint64_t* request_id,
                                                 uint64_t bytes) {
    const bool lease_mode = XferBenchConfig::receiver_credit_mode == "credit";
    if (lease_mode) {
        if (receiver_credit_bytes_per_transfer_ == 0) {
            receiver_credit_bytes_per_transfer_ = bytes;
        } else if (receiver_credit_bytes_per_transfer_ != bytes) {
            LOG(ERROR) << "Receiver-credit transfer size changed within a run";
            return -1;
        }
        if (pollReceiverCreditGrants(false) != 0) return -1;
        if (receiver_credit_leases_.empty()) {
            if (receiver_credit_pending_leases_.empty()) {
                const uint64_t remaining =
                    XferBenchConfig::receiver_credit_operations -
                    receiver_credit_requested_;
                const uint64_t slots = std::min(
                    XferBenchConfig::receiver_credit_grant_batch, remaining);
                if (requestReceiverCreditLease(bytes, slots) != 0) return -1;
            }
            if (pollReceiverCreditGrants(true) != 0) return -1;
        }
        auto& lease = receiver_credit_leases_.front();
        *request_id = lease.id;
        --lease.remaining;
        --receiver_credit_available_;
        ++receiver_credit_consumed_;
        if (lease.remaining == 0) receiver_credit_leases_.pop_front();

        const uint64_t low_watermark = std::max<uint64_t>(
            1, XferBenchConfig::receiver_credit_grant_batch / 2);
        if (receiver_credit_available_ <= low_watermark &&
            receiver_credit_pending_leases_.empty() &&
            receiver_credit_requested_ <
                XferBenchConfig::receiver_credit_operations) {
            const uint64_t remaining =
                XferBenchConfig::receiver_credit_operations -
                receiver_credit_requested_;
            const uint64_t slots = std::min(
                XferBenchConfig::receiver_credit_grant_batch, remaining);
            if (requestReceiverCreditLease(bytes, slots) != 0) return -1;
        }
        return 0;
    }

    nlohmann::json payload = {
        {"schema_version", 1},
        {"sender_segment", engine_->getSegmentName()},
        {"request_id", *request_id},
        {"bytes", bytes},
        {"slots", 1},
        {"mode", XferBenchConfig::receiver_credit_mode},
    };
    auto status = engine_->sendNotification(
        handle_, Notification{kReceiverCreditDemand, payload.dump()});
    if (!status.ok()) {
        LOG(ERROR) << "Failed to send receiver-credit demand: "
                   << status.ToString();
        return -1;
    }
    return 0;
}

int TENTBenchRunner::requestReceiverCreditLease(uint64_t bytes,
                                                uint64_t slots) {
    if (slots == 0 || bytes > std::numeric_limits<uint64_t>::max() / slots) {
        LOG(ERROR) << "Receiver-credit lease size is invalid";
        return -1;
    }
    const uint64_t request_id = ++receiver_credit_request_id_;
    nlohmann::json payload = {
        {"schema_version", 1},
        {"sender_segment", engine_->getSegmentName()},
        {"request_id", request_id},
        {"bytes", bytes * slots},
        {"slots", slots},
        {"mode", XferBenchConfig::receiver_credit_mode},
    };
    auto status = engine_->sendNotification(
        handle_, Notification{kReceiverCreditDemand, payload.dump()});
    if (!status.ok()) {
        LOG(ERROR) << "Failed to send receiver-credit lease demand: "
                   << status.ToString();
        return -1;
    }
    receiver_credit_pending_leases_.emplace(request_id, slots);
    receiver_credit_requested_ += slots;
    return 0;
}

int TENTBenchRunner::pollReceiverCreditGrants(bool wait_for_available) {
    const uint64_t deadline_us =
        steadyNowUs() +
        XferBenchConfig::receiver_credit_grant_timeout_ms * 1000;
    do {
        std::vector<Notification> notifications;
        auto status = engine_->receiveNotification(notifications);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to receive receiver-credit grant: "
                       << status.ToString();
            return -1;
        }
        for (const auto& notification : notifications) {
            if (notification.name != kReceiverCreditGrant) continue;
            try {
                const auto grant = nlohmann::json::parse(notification.msg);
                const auto request_id = grant.value("request_id", 0ull);
                const auto pending =
                    receiver_credit_pending_leases_.find(request_id);
                if (grant.value("schema_version", 0) != 1 ||
                    pending == receiver_credit_pending_leases_.end() ||
                    grant.value("slots", 0ull) != pending->second) {
                    LOG(ERROR) << "Unexpected receiver-credit grant";
                    return -1;
                }
                receiver_credit_leases_.push_back(
                    ReceiverCreditLease{request_id, pending->second});
                receiver_credit_available_ += pending->second;
                receiver_credit_pending_leases_.erase(pending);
            } catch (const std::exception& error) {
                LOG(ERROR) << "Malformed receiver-credit grant: "
                           << error.what();
                return -1;
            }
        }
        if (!wait_for_available || !receiver_credit_leases_.empty()) return 0;
        usleep(kReceiverCreditPollIntervalUs);
    } while (steadyNowUs() < deadline_us);

    LOG(ERROR) << "Timed out waiting for receiver-credit lease";
    return -1;
}

int TENTBenchRunner::finishReceiverCreditTransfer(uint64_t request_id,
                                                  uint64_t bytes) {
    uint64_t release_bytes = bytes;
    uint64_t release_slots = 1;
    nlohmann::json payload = {{"schema_version", 1},
                              {"sender_segment", engine_->getSegmentName()},
                              {"request_id", request_id},
                              {"bytes", release_bytes},
                              {"slots", release_slots}};
    auto status = engine_->sendNotification(
        handle_, Notification{kReceiverCreditRelease, payload.dump()});
    if (!status.ok()) {
        LOG(ERROR) << "Failed to send receiver-credit release: "
                   << status.ToString();
        return -1;
    }
    return 0;
}

int TENTBenchRunner::startInitiator(int num_threads) {
    CHECK_FAIL(engine_->openSegment(handle_, XferBenchConfig::target_seg_name));
    info_.buffers.clear();
    CHECK_FAIL(engine_->getSegmentInfo(handle_, info_));
    std::sort(info_.buffers.begin(), info_.buffers.end(),
              [](const SegmentInfo::Buffer& a, const SegmentInfo::Buffer& b) {
                  return a.location < b.location;
              });
    threads_.resize(num_threads);
    current_task_.resize(threads_.size());
    g_tent_running = true;
    for (size_t i = 0; i < threads_.size(); ++i)
        threads_[i] = std::thread(&TENTBenchRunner::runner, this, i);
    return 0;
}

int TENTBenchRunner::stopInitiator() {
    {
        std::unique_lock<std::mutex> lk(mtx_);
        g_tent_running = false;
        cv_task_.notify_all();
        cv_done_.notify_all();
    }
    for (auto& thread : threads_) {
        thread.join();
    }
    return 0;
}

static inline int getNumaNodeFromPciDevice(const std::string& pci_bdf) {
    std::string sysfs_path = "/sys/bus/pci/devices/" + pci_bdf + "/numa_node";
    std::ifstream numa_file(sysfs_path);
    if (!numa_file.is_open()) return -1;
    int numa_node = -1;
    numa_file >> numa_node;
    if (numa_file.fail()) return -1;
    return numa_node;
}

#if defined(USE_CUDA) || defined(USE_SUNRISE)
static inline int getGpuDeviceNumaID(int gpu_id) {
    char pci_bus_id[20];
    auto err = cudaDeviceGetPCIBusId(pci_bus_id, sizeof(pci_bus_id), gpu_id);
    if (err != cudaSuccess) {
        LOG(WARNING) << "cudaDeviceGetPCIBusId: " << cudaGetErrorString(err);
        return 0;
    }
    for (char* ch = pci_bus_id; (*ch = to_lower(*ch)); ch++);
    return getNumaNodeFromPciDevice(pci_bus_id);
}
#elif defined(USE_HIP)
static inline int getGpuDeviceNumaID(int gpu_id) {
    hipDeviceProp_t prop;
    if (hipGetDeviceProperties(&prop, gpu_id) != hipSuccess) return 0;
    char pci_bus_id[20];
    snprintf(pci_bus_id, sizeof(pci_bus_id), "%04x:%02x:%02x.0",
             prop.pciDomainID, prop.pciBusID, prop.pciDeviceID);
    return getNumaNodeFromPciDevice(pci_bus_id);
}
#else
static inline int getGpuDeviceNumaID(int gpu_id) { return 0; }
#endif

void TENTBenchRunner::pinThread(int thread_id) {
#ifdef USE_SUNRISE
    if (XferBenchConfig::seg_type == "VRAM" && !pinned_buffer_list_.empty()) {
        int base_gpu = std::max(0, XferBenchConfig::local_gpu_id);
        int device_id =
            base_gpu +
            (thread_id % static_cast<int>(pinned_buffer_list_.size()));
        auto err = cudaSetDevice(device_id);
        LOG_ASSERT(err == cudaSuccess)
            << "cudaSetDevice failed before getLocation: "
            << cudaGetErrorString(err) << " device_id=" << device_id;
        bindToSocket(getGpuDeviceNumaID(device_id));
        return;
    }
#endif
    uint64_t addr =
        (uint64_t)pinned_buffer_list_[thread_id % pinned_buffer_list_.size()];
    auto result = Platform::getLoader().getLocation((void*)addr, 1);
    LocationParser location(result[0].location);
    if (location.type() == "cpu") {
        auto socket_id = location.index();
        bindToSocket(socket_id);
    } else if (location.type() == "cuda" || location.type() == "rocm") {
        auto device_id = location.index();
        auto socket_id = getGpuDeviceNumaID(device_id);
        bindToSocket(socket_id);
    }
}

int TENTBenchRunner::runner(int thread_id) {
    while (g_tent_running) {
        std::function<int(int)> task;
        {
            std::unique_lock<std::mutex> lk(mtx_);
            cv_task_.wait(lk, [&] {
                return !g_tent_running || current_task_[thread_id];
            });
            if (!g_tent_running) break;
            std::swap(task, current_task_[thread_id]);
        }
        if (task) task(thread_id);
        {
            std::unique_lock<std::mutex> lk(mtx_);
            if (--pending_ == 0) cv_done_.notify_all();
        }
    }
    return 0;
}

int TENTBenchRunner::runInitiatorTasks(
    const std::function<int(int /* thread_id */)>& func) {
    std::unique_lock<std::mutex> lk(mtx_);
    for (size_t id = 0; id < current_task_.size(); ++id)
        current_task_[id] = func;
    pending_ = (int)threads_.size();
    cv_task_.notify_all();
    cv_done_.wait(lk, [&] { return !g_tent_running || pending_ == 0; });
    return g_tent_running ? 0 : -1;
}

double TENTBenchRunner::runSingleTransfer(uint64_t local_addr,
                                          uint64_t target_addr,
                                          uint64_t block_size,
                                          uint64_t batch_size, OpCode opcode) {
    XferBenchTimer timer;
    uint64_t receiver_credit_request_id = 0;
    const uint64_t receiver_credit_bytes = block_size * batch_size;
    if (XferBenchConfig::receiver_credit_mode != "disabled") {
        receiver_credit_request_id = ++receiver_credit_request_id_;
        if (beginReceiverCreditTransfer(&receiver_credit_request_id,
                                        receiver_credit_bytes) != 0) {
            exit(EXIT_FAILURE);
        }
    }
    auto batch_id = engine_->allocateBatch(batch_size);
    std::vector<Request> requests;
    for (uint64_t i = 0; i < batch_size; ++i) {
        Request entry;
        entry.opcode = opcode == READ ? Request::READ : Request::WRITE;
        entry.length = block_size;
        entry.source = (void*)(local_addr + block_size * i);
        entry.target_id = handle_;
        entry.target_offset = target_addr + block_size * i;
        entry.transport_hint = transport_hint_;
        requests.emplace_back(entry);
    }
    if (XferBenchConfig::notifi) {
        // Use target_addr as msg for verification by peer
        Notification notifi{"benchmark", std::to_string(target_addr)};
        CHECK_FAIL(engine_->submitTransfer(batch_id, requests, notifi));
    } else {
        CHECK_FAIL(engine_->submitTransfer(batch_id, requests));
    }
    while (true) {
        TransferStatus overall_status;
        CHECK_FAIL(engine_->getTransferStatus(batch_id, overall_status));
        if (overall_status.s == TransferStatusEnum::COMPLETED) {
            break;
        } else if (overall_status.s == TransferStatusEnum::FAILED) {
            LOG(ERROR) << "Failed transfer detected";
            exit(EXIT_FAILURE);
        }
    }
    auto duration = timer.lap_us();
    CHECK_FAIL(engine_->freeBatch(batch_id));
    if (XferBenchConfig::receiver_credit_mode != "disabled" &&
        finishReceiverCreditTransfer(receiver_credit_request_id,
                                     receiver_credit_bytes) != 0) {
        exit(EXIT_FAILURE);
    }
    return duration;
}

}  // namespace tent
}  // namespace mooncake
