// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/ub_transport.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>
#include <mutex>
#include <new>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <glog/logging.h>

#include "tent/common/utils/string_builder.h"
#include "tent/runtime/segment.h"
#include "tent/thirdparty/nlohmann/json.h"
#include "tent/transport/ub/buffers.h"
#include "tent/transport/ub/context.h"
#include "tent/transport/ub/endpoint.h"
#include "tent/transport/ub/endpoint_store.h"
#include "tent/transport/ub/params.h"
#include "tent/transport/ub/quota.h"
#include "tent/transport/ub/rail_monitor.h"
#include "tent/transport/ub/slice.h"
#include "tent/transport/ub/urma_adapter.h"
#include "tent/transport/ub/workers.h"

namespace mooncake::tent {
namespace {

bool filterAllows(const std::vector<std::string>& filter,
                  const ub::DeviceInfo& device) {
    if (filter.empty()) return true;
    return std::find(filter.begin(), filter.end(), device.topology_name) !=
               filter.end() ||
           std::find(filter.begin(), filter.end(), device.native_device_name) !=
               filter.end();
}

uint64_t generationSeed() {
    const auto wall = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    const auto steady = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
    const uint64_t seed =
        (wall ^ (steady << 19) ^ (steady >> 5)) & 0x7fffffffffffffffULL;
    return seed == 0 ? 1 : seed;
}

std::string encodeDeviceMetadata(Topology::NicID topology_id,
                                 const ub::DeviceInfo& device) {
    const auto& caps = device.capabilities;
    return nlohmann::json{{"schema_version", 1},
                          {"topology_id", topology_id},
                          {"native_device_name", device.native_device_name},
                          {"native_device_path", device.native_device_path},
                          {"eid_index", device.eid_index},
                          {"eid", device.eid},
                          {"active", device.active},
                          {"capabilities",
                           {{"max_jfc", caps.max_jfc},
                            {"max_jfc_depth", caps.max_jfc_depth},
                            {"max_jfr_depth", caps.max_jfr_depth},
                            {"max_jetty", caps.max_jetty},
                            {"max_jetty_depth", caps.max_jetty_depth},
                            {"max_send_sge", caps.max_send_sge},
                            {"max_remote_sge", caps.max_remote_sge},
                            {"max_message_size", caps.max_message_size},
                            {"max_read_size", caps.max_read_size},
                            {"max_write_size", caps.max_write_size},
                            {"feature_flags", caps.feature_flags},
                            {"transport_modes", caps.transport_modes}}}}
        .dump();
}

}  // namespace

struct UbTransport::Impl {
    explicit Impl(std::shared_ptr<ub::UrmaAdapter> injected)
        : adapter(injected ? std::move(injected)
                           : ub::createDefaultUrmaAdapter()) {}

    Status install(const std::string& segment_name,
                   std::shared_ptr<ControlService> control,
                   std::shared_ptr<Topology> topology,
                   std::shared_ptr<Config> config) {
        std::lock_guard<std::mutex> lock(lifecycle_mutex);
        if (installed.load(std::memory_order_acquire)) {
            return Status::InvalidArgument(
                "UB transport has already been installed" LOC_MARK);
        }
        if (shutting_down.load(std::memory_order_acquire) || workers ||
            adapter_initialized) {
            return Status::InvalidArgument(
                "A previous UB uninstall has not drained yet" LOC_MARK);
        }
        if (segment_name.empty() || !control || !topology) {
            return Status::InvalidArgument(
                "UB install requires segment, control service and "
                "topology" LOC_MARK);
        }
        if (!adapter || !adapter->available()) {
            return Status::DeviceNotFound(
                "A real or injected URMA adapter is unavailable" LOC_MARK);
        }
        if (!config) config = std::make_shared<Config>();
        ub::UbParams parsed;
        CHECK_STATUS(ub::UbParams::FromConfig(*config, parsed));
        if (!parsed.enable) {
            return Status::InvalidArgument(
                "UB transport is disabled by configuration" LOC_MARK);
        }
        if (parsed.enable_notifications) {
            return Status::NotImplemented(
                "UB notifications are not supported by protocol version "
                "1" LOC_MARK);
        }

        shutting_down.store(false, std::memory_order_release);
        local_segment_name = segment_name;
        metadata = std::move(control);
        local_topology = std::move(topology);
        conf = std::move(config);
        params = parsed;

        auto status = adapter->initialize();
        if (!status.ok()) return failInstall(status);
        adapter_initialized = true;

        std::vector<ub::DeviceInfo> discovered;
        status = adapter->discoverDevices(discovered);
        if (!status.ok()) return failInstall(status);
        std::unordered_map<std::string, ub::DeviceInfo> by_topology_name;
        std::unordered_map<Topology::NicID, uint32_t> jfc_depth_by_device;
        for (auto& device : discovered) {
            by_topology_name.emplace(device.topology_name, std::move(device));
        }

        for (size_t id = 0; id < local_topology->getNicCount(); ++id) {
            const auto* nic = local_topology->getNicEntry(static_cast<int>(id));
            if (!nic || nic->type != Topology::NIC_UB) continue;
            auto found = by_topology_name.find(nic->name);
            if (found == by_topology_name.end() || !found->second.active) {
                continue;
            }
            if (!filterAllows(params.device_filter, found->second)) continue;

            ub::JfcOptions jfc_options;
            const auto& device_caps = found->second.capabilities;
            if (device_caps.max_jfc_depth != 0) {
                jfc_options.depth =
                    std::min(jfc_options.depth, device_caps.max_jfc_depth);
                jfc_options.receiver_depth = std::min(
                    jfc_options.receiver_depth, device_caps.max_jfc_depth);
            }
            if (device_caps.max_jfr_depth != 0) {
                jfc_options.receiver_depth = std::min(
                    jfc_options.receiver_depth, device_caps.max_jfr_depth);
            }
            auto context = std::make_shared<ub::UbContext>(
                static_cast<Topology::NicID>(id), found->second, adapter);
            status = context->initialize(params.jfc_per_context, jfc_options);
            if (!status.ok()) {
                LOG(WARNING) << "Disable UB device " << nic->name << ": "
                             << status.ToString();
                continue;
            }
            context_by_topology_id.emplace(static_cast<int>(id), context);
            context_by_topology_name.emplace(nic->name, context);
            jfc_depth_by_device.emplace(static_cast<Topology::NicID>(id),
                                        jfc_options.depth);
            contexts.push_back(std::move(context));
        }
        if (contexts.empty()) {
            return failInstall(Status::DeviceNotFound(
                "No UB context initialized successfully" LOC_MARK));
        }

        size_t safe_slice_size = params.slice_size;
        ub::JettyOptions jetty_options;
        for (const auto& context : contexts) {
            const auto& device_caps = context->deviceInfo().capabilities;
            auto clamp_slice = [&safe_slice_size](uint64_t limit) {
                if (limit != 0) {
                    safe_slice_size = static_cast<size_t>(
                        std::min<uint64_t>(safe_slice_size, limit));
                }
            };
            clamp_slice(device_caps.max_message_size);
            clamp_slice(device_caps.max_read_size);
            clamp_slice(device_caps.max_write_size);
            if (device_caps.max_jetty_depth != 0) {
                jetty_options.depth =
                    std::min(jetty_options.depth, device_caps.max_jetty_depth);
            }
            if (device_caps.max_send_sge != 0) {
                jetty_options.max_sge = static_cast<uint8_t>(std::min<uint32_t>(
                    jetty_options.max_sge, device_caps.max_send_sge));
            }
            if (device_caps.max_remote_sge != 0) {
                jetty_options.max_sge = static_cast<uint8_t>(std::min<uint32_t>(
                    jetty_options.max_sge, device_caps.max_remote_sge));
            }
        }
        if (safe_slice_size == 0 || jetty_options.depth == 0 ||
            jetty_options.max_sge == 0) {
            return failInstall(Status::InvalidArgument(
                "UB device capabilities cannot support the configured data "
                "path" LOC_MARK));
        }
        if (safe_slice_size != params.slice_size) {
            LOG(WARNING) << "Clamp UB slice_size from " << params.slice_size
                         << " to device limit " << safe_slice_size;
            params.slice_size = safe_slice_size;
        }

        buffers = std::make_unique<ub::UbBufferManager>(adapter, contexts);
        endpoints = std::make_unique<ub::EndpointStore>(
            adapter, params.max_endpoints, params.jetty_per_endpoint,
            jetty_options);
        ub::RailMonitorConfig rail_config;
        rail_config.cooldown_ns =
            static_cast<uint64_t>(params.endpoint_cooldown_ms) * 1'000'000ULL;
        rails = std::make_unique<ub::RailMonitor>(rail_config);
        auto saturatedProduct = [](uint64_t lhs, uint64_t rhs) {
            if (lhs != 0 && rhs > std::numeric_limits<uint64_t>::max() / lhs) {
                return std::numeric_limits<uint64_t>::max();
            }
            return lhs * rhs;
        };
        const uint64_t path_wrs =
            saturatedProduct(jetty_options.depth, params.jetty_per_endpoint);
        ub::QuotaLimits path_limits{
            saturatedProduct(path_wrs, params.slice_size), path_wrs};
        quota = std::make_unique<ub::QuotaManager>(path_limits, path_limits);
        for (const auto& context : contexts) {
            const auto depth = jfc_depth_by_device.at(context->topologyId());
            const uint64_t device_wrs =
                saturatedProduct(depth, context->jfcs().size());
            (void)quota->setDeviceLimits(
                context->topologyId(),
                ub::QuotaLimits{saturatedProduct(device_wrs, params.slice_size),
                                device_wrs});
        }

        status = publishLocalDevices();
        if (!status.ok()) return failInstall(status);

        metadata->setBootstrapUbCallback(
            [this](const UbBootstrapDesc& request, UbBootstrapDesc& response) {
                return onBootstrap(request, response);
            });
        callback_installed = true;

        workers = std::make_unique<ub::UbWorkers>(
            adapter, contexts, local_topology, &metadata->segmentManager(),
            buffers.get(), rails.get(), quota.get(), params,
            [this](const ub::EndpointResolveRequest& request,
                   std::shared_ptr<ub::UbEndpoint>& endpoint) {
                return resolveEndpoint(request, endpoint);
            },
            [this](const std::shared_ptr<ub::UbEndpoint>& endpoint) {
                if (endpoints) (void)endpoints->retire(endpoint);
            });
        status = workers->start();
        if (!status.ok()) return failInstall(status);

        installed.store(true, std::memory_order_release);
        return Status::OK();
    }

    Status uninstall() {
        std::lock_guard<std::mutex> lock(lifecycle_mutex);
        return shutdownUnlocked();
    }

    Status failInstall(Status failure) {
        (void)shutdownUnlocked();
        return failure;
    }

    Status shutdownUnlocked() {
        installed.store(false, std::memory_order_release);
        shutting_down.store(true, std::memory_order_release);
        Status first_error = Status::OK();
        auto remember = [&first_error](const Status& status) {
            if (first_error.ok() && !status.ok()) first_error = status;
        };

        // Callback replacement waits for a currently executing UB bootstrap
        // handler, fencing all control-plane access before resources retire.
        if (callback_installed && metadata) {
            metadata->setBootstrapUbCallback({});
            callback_installed = false;
        }
        if (workers) {
            auto status = workers->stop();
            if (!status.ok()) {
                // A failed native drain fence is not permission to destroy
                // memory registrations, JFCs, Contexts, or the adapter. Keep
                // the complete ownership graph alive so uninstall can be
                // retried after a late completion/provider recovery.
                return status;
            }
            workers.reset();
        }
        if (endpoints) {
            auto status = endpoints->clear();
            if (!status.ok()) return status;
            endpoints.reset();
        }
        if (buffers) {
            auto status = buffers->clear();
            if (!status.ok()) return status;
            buffers.reset();
        }
        for (auto it = contexts.rbegin(); it != contexts.rend(); ++it) {
            if (*it) remember((*it)->shutdown());
        }
        contexts.clear();
        context_by_topology_id.clear();
        context_by_topology_name.clear();
        rails.reset();
        quota.reset();
        if (adapter_initialized && adapter) {
            remember(adapter->shutdown());
            adapter_initialized = false;
        }
        metadata.reset();
        local_topology.reset();
        conf.reset();
        local_segment_name.clear();
        shutting_down.store(false, std::memory_order_release);
        return first_error;
    }

    Status publishLocalDevices() {
        auto& manager = metadata->segmentManager();
        CHECK_STATUS(
            manager.updateLocal([this](SegmentDesc& segment) -> Status {
                if (segment.type != SegmentType::Memory) {
                    return Status::InvalidMetadataType(
                        "Local segment is not memory-backed" LOC_MARK);
                }
                auto& detail = std::get<MemorySegmentDesc>(segment.detail);
                detail.transport_attrs[TransportType::UB] = nlohmann::json{
                    {"schema_version", 1},
                    {"protocol", "urma"},
                    {"notifications", false}}.dump();
                std::unordered_set<std::string> existing;
                for (const auto& device : detail.devices) {
                    existing.insert(device.name);
                }
                for (const auto& context : contexts) {
                    if (!context || !context->active()) continue;
                    if (existing.insert(context->deviceInfo().topology_name)
                            .second) {
                        DeviceDesc device;
                        device.name = context->deviceInfo().topology_name;
                        device.lid = 0;
                        device.gid.clear();
                        device.transport_attrs[TransportType::UB] =
                            encodeDeviceMetadata(context->topologyId(),
                                                 context->deviceInfo());
                        detail.devices.push_back(std::move(device));
                    }
                }
                return Status::OK();
            }));
        return manager.synchronizeLocal();
    }

    Status resolveEndpoint(const ub::EndpointResolveRequest& request,
                           std::shared_ptr<ub::UbEndpoint>& endpoint) {
        endpoint.reset();
        if (shutting_down.load(std::memory_order_acquire) || !endpoints ||
            !request.local_context || !request.remote_segment) {
            return Status::InvalidArgument(
                "UB transport is shutting down or route is invalid" LOC_MARK);
        }
        const auto* remote_nic =
            request.remote_segment->getMemory().topology.getNicEntry(
                request.remote_topology_id);
        if (!remote_nic || remote_nic->type != Topology::NIC_UB) {
            return Status::DeviceNotFound(
                "Remote segment does not advertise a UB topology "
                "device" LOC_MARK);
        }
        const std::string peer_path =
            MakeNicPath(request.remote_segment->name, remote_nic->name);
        ub::UbEndpointKey key{request.local_context->topologyId(),
                              request.remote_segment_id,
                              request.remote_topology_id, peer_path};
        CHECK_STATUS(
            endpoints->getOrCreate(key, request.local_context, endpoint));
        if (endpoint->ready()) return Status::OK();

        const std::string local_path =
            MakeNicPath(local_segment_name,
                        request.local_context->deviceInfo().topology_name);
        UbBootstrapDesc bootstrap;
        auto status = endpoint->makeBootstrapDesc(
            local_segment_name, local_path, peer_path,
            request.segment_generation, bootstrap);
        if (!status.ok()) {
            (void)endpoints->retire(endpoint);
            endpoint.reset();
            return status;
        }
        UbBootstrapDesc response;
        status = ControlClient::bootstrapUb(
            request.remote_segment->rpc_server_addr, bootstrap, response);
        if (status.ok()) status = endpoint->bind(response);
        if (!status.ok()) {
            (void)endpoints->retire(endpoint);
            endpoint.reset();
            return status;
        }
        return Status::OK();
    }

    int onBootstrap(const UbBootstrapDesc& request, UbBootstrapDesc& response) {
        if (shutting_down.load(std::memory_order_acquire) || !endpoints) {
            response.reply_msg = "UB transport is shutting down";
            return -1;
        }
        const std::string local_name =
            getNicNameFromNicPath(request.peer_nic_path);
        auto context_it = context_by_topology_name.find(local_name);
        if (local_name.empty() ||
            context_it == context_by_topology_name.end()) {
            response.reply_msg = "UB bootstrap selected an unknown local NIC";
            return -1;
        }
        if (request.local_device_id < 0 || request.local_eid.empty() ||
            request.jetty_ids.empty() || request.endpoint_generation == 0) {
            response.reply_msg = "UB bootstrap request is incomplete";
            return -1;
        }

        ub::UbEndpointKey key{context_it->second->topologyId(),
                              LOCAL_SEGMENT_ID, request.local_device_id,
                              request.local_nic_path};
        std::shared_ptr<ub::UbEndpoint> endpoint;
        auto status = endpoints->getOrCreate(key, context_it->second, endpoint);
        if (status.ok()) status = endpoint->bind(request);
        if (status.ok()) {
            status = endpoint->makeBootstrapDesc(
                local_segment_name, request.peer_nic_path,
                request.local_nic_path, currentSegmentGeneration(), response);
        }
        if (!status.ok()) {
            if (endpoint) (void)endpoints->retire(endpoint);
            response = UbBootstrapDesc{};
            response.reply_msg = status.ToString();
            return -1;
        }
        return 0;
    }

    uint64_t currentSegmentGeneration() const {
        // Buffer generations are carried in buffer metadata. The bootstrap
        // field is a peer restart/refresh hint and must still be nonzero even
        // before the first user buffer is registered.
        return segment_generation.load(std::memory_order_relaxed);
    }

    mutable std::mutex lifecycle_mutex;
    std::shared_ptr<ub::UrmaAdapter> adapter;
    bool adapter_initialized{false};
    bool callback_installed{false};
    std::atomic<bool> installed{false};
    std::atomic<bool> shutting_down{false};
    std::atomic<uint64_t> segment_generation{generationSeed()};
    ub::UbParams params;
    std::string local_segment_name;
    std::shared_ptr<ControlService> metadata;
    std::shared_ptr<Topology> local_topology;
    std::shared_ptr<Config> conf;
    std::vector<ub::UbContextPtr> contexts;
    std::unordered_map<Topology::NicID, ub::UbContextPtr>
        context_by_topology_id;
    std::unordered_map<std::string, ub::UbContextPtr> context_by_topology_name;
    std::unique_ptr<ub::UbBufferManager> buffers;
    std::unique_ptr<ub::EndpointStore> endpoints;
    std::unique_ptr<ub::RailMonitor> rails;
    std::unique_ptr<ub::QuotaManager> quota;
    std::unique_ptr<ub::UbWorkers> workers;
};

UbTransport::UbTransport(std::shared_ptr<ub::UrmaAdapter> adapter)
    : impl_(std::make_unique<Impl>(std::move(adapter))) {}

UbTransport::~UbTransport() {
    auto status = uninstall();
    if (!status.ok()) {
        // There is no caller left to retry an explicit uninstall. Leaking the
        // still-live ownership graph is the only safe failure mode: destroying
        // joinable pollers or registered memory after a failed device fence
        // would terminate the process or permit DMA-after-free. The OS/provider
        // reclaims these resources at process exit.
        LOG(ERROR) << "Preserve undrained UB resources during destruction: "
                   << status.ToString();
        (void)impl_.release();
    }
}

Status UbTransport::install(std::string& local_segment_name,
                            std::shared_ptr<ControlService> metadata,
                            std::shared_ptr<Topology> local_topology,
                            std::shared_ptr<Config> conf) {
    auto status = impl_->install(local_segment_name, std::move(metadata),
                                 std::move(local_topology), std::move(conf));
    if (status.ok()) {
        caps = Capabilities{};
        caps.dram_to_dram = true;
    }
    return status;
}

Status UbTransport::uninstall() {
    auto status = impl_->uninstall();
    caps = Capabilities{};
    return status;
}

Status UbTransport::allocateSubBatch(SubBatchRef& batch, size_t max_size) {
    batch = nullptr;
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->installed.load(std::memory_order_acquire)) {
        return Status::InvalidArgument(
            "UB transport is not installed" LOC_MARK);
    }
    auto* ub_batch = new (std::nothrow) UbSubBatch();
    if (!ub_batch) {
        return Status::InternalError(
            "Unable to allocate UB sub-batch" LOC_MARK);
    }
    ub_batch->max_size = max_size;
    ub_batch->task_list.reserve(max_size);
    batch = ub_batch;
    return Status::OK();
}

Status UbTransport::freeSubBatch(SubBatchRef& batch) {
    auto* ub_batch = dynamic_cast<UbSubBatch*>(batch);
    if (!ub_batch) {
        return Status::InvalidArgument("Invalid UB sub-batch" LOC_MARK);
    }
    ub_batch->task_list.clear();
    delete ub_batch;
    batch = nullptr;
    return Status::OK();
}

Status UbTransport::submitTransferTasks(
    SubBatchRef batch, const std::vector<Request>& request_list) {
    auto* ub_batch = dynamic_cast<UbSubBatch*>(batch);
    if (!ub_batch) {
        return Status::InvalidArgument("Invalid UB sub-batch" LOC_MARK);
    }
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->installed.load(std::memory_order_acquire) || !impl_->workers) {
        return Status::InvalidArgument(
            "UB transport is not installed" LOC_MARK);
    }
    if (request_list.size() > ub_batch->max_size - ub_batch->task_list.size()) {
        return Status::TooManyRequests("Exceed UB batch capacity" LOC_MARK);
    }
    for (const auto& request : request_list) {
        const auto local_address = reinterpret_cast<uintptr_t>(request.source);
        if (!request.source || request.length == 0 ||
            request.length >
                std::numeric_limits<uint64_t>::max() - request.target_offset ||
            request.length >
                std::numeric_limits<uintptr_t>::max() - local_address) {
            return Status::InvalidArgument(
                "UB request range is empty or overflows" LOC_MARK);
        }
    }

    const auto notify_progress = ub_batch->notify_progress;
    const auto progress_batch_id = ub_batch->progress_batch_id;
    std::vector<ub::UbTask::Ptr> new_tasks;
    new_tasks.reserve(request_list.size());
    for (const auto& request : request_list) {
        auto task = ub::UbTask::create(
            request,
            [notify_progress, progress_batch_id](const TransferStatus&) {
                if (notify_progress) notify_progress(progress_batch_id);
            });
        size_t offset = 0;
        while (offset < request.length) {
            const size_t length =
                std::min(impl_->params.slice_size, request.length - offset);
            ub::UbSliceSpec spec;
            spec.local_address = static_cast<char*>(request.source) + offset;
            spec.remote_address = request.target_offset + offset;
            spec.length = length;
            spec.request_offset = offset;
            spec.max_retries = impl_->params.max_retries;
            if (!task->addSlice(spec)) {
                return Status::InternalError(
                    "Unable to construct UB slice" LOC_MARK);
            }
            offset += length;
        }
        (void)task->seal();
        new_tasks.push_back(std::move(task));
    }

    for (auto& task : new_tasks) {
        ub_batch->task_list.push_back(task);
        auto status = impl_->workers->submit(task, ub_batch->device_mask);
        if (!status.ok()) {
            for (auto& queued : new_tasks) {
                if (queued) queued->requestCancellation();
            }
            return status;
        }
    }
    return Status::OK();
}

Status UbTransport::getTransferStatus(SubBatchRef batch, int task_id,
                                      TransferStatus& status) {
    auto* ub_batch = dynamic_cast<UbSubBatch*>(batch);
    if (!ub_batch || task_id < 0 ||
        static_cast<size_t>(task_id) >= ub_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid UB task ID" LOC_MARK);
    }
    status = ub_batch->task_list[task_id]->transferStatus();
    return Status::OK();
}

Status UbTransport::cancelTransferTask(SubBatchRef batch, int task_id) {
    auto* ub_batch = dynamic_cast<UbSubBatch*>(batch);
    if (!ub_batch || task_id < 0 ||
        static_cast<size_t>(task_id) >= ub_batch->task_list.size()) {
        return Status::InvalidArgument("Invalid UB task ID" LOC_MARK);
    }
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->workers) {
        return Status::InvalidArgument("UB workers are not running" LOC_MARK);
    }
    return impl_->workers->cancel(ub_batch->task_list[task_id]);
}

Status UbTransport::addMemoryBuffer(BufferDesc& desc,
                                    const MemoryOptions& options) {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->buffers) {
        return Status::InvalidArgument(
            "UB transport is not installed" LOC_MARK);
    }
    auto status = impl_->buffers->addBuffer(desc, options);
    if (status.ok()) {
        impl_->segment_generation.fetch_add(1, std::memory_order_relaxed);
    }
    return status;
}

Status UbTransport::addMemoryBuffer(std::vector<BufferDesc>& desc_list,
                                    const MemoryOptions& options) {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->buffers) {
        return Status::InvalidArgument(
            "UB transport is not installed" LOC_MARK);
    }
    auto status = impl_->buffers->addBuffers(desc_list, options);
    if (status.ok()) {
        impl_->segment_generation.fetch_add(1, std::memory_order_relaxed);
    }
    return status;
}

Status UbTransport::removeMemoryBuffer(BufferDesc& desc) {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->buffers) return Status::OK();
    auto status = impl_->buffers->removeBuffer(desc);
    if (status.ok()) {
        impl_->segment_generation.fetch_add(1, std::memory_order_relaxed);
    }
    return status;
}

bool UbTransport::warmupMemory(void* addr, size_t length) {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!addr || length == 0 || !impl_->adapter || impl_->contexts.empty()) {
        return false;
    }
    ub::LocalSegmentPtr segment;
    ub::SegmentOptions options;
    auto status = impl_->adapter->registerLocalSegment(
        impl_->contexts.front()->handle(), reinterpret_cast<uint64_t>(addr),
        length, options, segment);
    if (!status.ok()) return false;
    return impl_->adapter->unregisterLocalSegment(segment).ok();
}

double UbTransport::getEstimatedBandwidth() const {
    std::lock_guard<std::mutex> lock(impl_->lifecycle_mutex);
    if (!impl_->params.enable_bandwidth_estimation || !impl_->rails)
        return -1.0;
    return impl_->rails->aggregateBandwidth();
}

}  // namespace mooncake::tent
