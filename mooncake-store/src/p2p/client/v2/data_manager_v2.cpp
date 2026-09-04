#include "p2p/client/v2/data_manager_v2.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <span>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <glog/logging.h>

#include "p2p/client/p2p_client_metric.h"
#include "utils.h"

namespace mooncake::v2 {
namespace {

constexpr size_t kForEachKeyBatchSize = 512;

size_t ParseByteSize(const Json::Value& value) {
    if (value.isString()) {
        return static_cast<size_t>(string_to_byte_size(value.asString()));
    }
    return static_cast<size_t>(value.asUInt64());
}

tl::expected<size_t, ErrorCode> TotalSliceSize(
    const std::vector<Slice>& slices) {
    size_t total = 0;
    for (const auto& slice : slices) {
        if (slice.size == 0) continue;
        if (slice.ptr == nullptr) {
            LOG(ERROR) << "slice has a null pointer with a non-zero size";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (total > std::numeric_limits<size_t>::max() - slice.size) {
            LOG(ERROR) << "slice sizes overflow";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        total += slice.size;
    }
    return total;
}

/**
 * @brief Where a storage tier keeps its backing file when the config does not
 *        say.
 *
 * The name carries a v2 prefix and the tiler's UUID on purpose: V2 manages
 * this space as a plain extent allocator while V1's StorageTier evicts whole
 * buckets underneath it, so the two must never end up on the same file even if
 * an operator points them at one directory.
 */
tl::expected<std::string, ErrorCode> DeriveStorageFilePath(
    const UUID& tiler_id) {
    const char* root = std::getenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH");
    if (root == nullptr || *root == '\0') {
        LOG(ERROR) << "A storage tier needs either a 'file_path' in its "
                      "configuration or MOONCAKE_OFFLOAD_FILE_STORAGE_PATH";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return std::string(root) + "/mooncake_v2_tier_" +
           std::to_string(tiler_id.first) + "_" +
           std::to_string(tiler_id.second) + ".data";
}

}  // namespace

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

tl::expected<DataManagerV2Config, ErrorCode> ParseDataManagerV2Config(
    const Json::Value& tier_config, const LocalTransferConfig& local_transfer,
    const KeyLeaseConfig& key_lease) {
    DataManagerV2Config config;
    config.local_transfer = local_transfer;
    config.key_lease = key_lease;

    if (!tier_config.isMember("tiers") || !tier_config["tiers"].isArray() ||
        tier_config["tiers"].empty()) {
        LOG(ERROR) << "DataManagerV2 config has no 'tiers' array";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    for (const auto& entry : tier_config["tiers"]) {
        const std::string type =
            entry.isMember("type") ? entry["type"].asString() : "";
        if (type == "ASCEND_NPU" || type == "ASCEND") {
            // Refusing loudly. Skipping the tier would leave the operator
            // believing data lives on the NPU while it silently landed in
            // DRAM.
            LOG(ERROR) << "DataManagerV2 does not support tier type '" << type
                       << "'. Keep data_manager_version=v1 for this "
                          "deployment.";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        TilerConfig tiler;
        // Each tiers[] entry is one independent logical tiler with its own
        // UUID and Master segment; entries are never merged by type.
        tiler.logical.tiler_id = generate_uuid();
        tiler.logical.priority =
            entry.isMember("priority") ? entry["priority"].asInt() : 0;
        if (entry.isMember("tags") && entry["tags"].isArray()) {
            for (const auto& tag : entry["tags"]) {
                tiler.logical.tags.push_back(tag.asString());
            }
        }

        const size_t capacity =
            entry.isMember("capacity") ? ParseByteSize(entry["capacity"]) : 0;
        if (capacity == 0) {
            LOG(ERROR) << "tier '" << type << "' has no capacity";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        if (type == "DRAM") {
            tiler.logical.memory_type = MemoryType::DRAM;
            DramArenaConfig arena;
            arena.capacity_bytes = capacity;
            if (entry.isMember("numa_node") && !entry["numa_node"].isNull()) {
                arena.numa_node = entry["numa_node"].asInt();
            }
            DramBlockPoolConfig pool;
            pool.arenas.push_back(arena);
            tiler.pool = pool;
        } else if (type == "STORAGE" || type == "NVME" || type == "SSD") {
            tiler.logical.memory_type = MemoryType::NVME;
            SSDDeviceConfig device;
            device.capacity_bytes = capacity;
            if (entry.isMember("alignment")) {
                device.alignment =
                    static_cast<size_t>(entry["alignment"].asUInt64());
            }
            if (entry.isMember("fsync_on_commit")) {
                device.fsync_on_commit = entry["fsync_on_commit"].asBool();
            }
            if (entry.isMember("file_path")) {
                device.file_path = entry["file_path"].asString();
            } else {
                auto derived = DeriveStorageFilePath(tiler.logical.tiler_id);
                if (!derived) return tl::make_unexpected(derived.error());
                device.file_path = *derived;
            }
            SSDBlockPoolConfig pool;
            pool.devices.push_back(device);
            tiler.pool = pool;
        } else {
            LOG(ERROR) << "Unknown tier type '" << type << "'";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        if (entry.isMember("allocation_failure")) {
            const auto& node = entry["allocation_failure"];
            AllocationFailurePolicyConfig policy;
            if (node.isMember("try_evict")) {
                policy.try_evict = node["try_evict"].asBool();
            }
            if (node.isMember("max_evict_rounds")) {
                policy.max_evict_rounds = node["max_evict_rounds"].asUInt();
            }
            if (node.isMember("evict_timeout_ms")) {
                policy.evict_timeout = std::chrono::milliseconds(
                    node["evict_timeout_ms"].asUInt());
            }
            if (node.isMember("reclaim_margin_bytes")) {
                policy.reclaim_margin_bytes =
                    ParseByteSize(node["reclaim_margin_bytes"]);
            }
            auto valid = ValidateAllocationFailurePolicy(policy);
            if (!valid) return tl::make_unexpected(valid.error());
            tiler.allocation_failure_override = policy;
        }

        config.tilers.push_back(std::move(tiler));
    }

    // One ordering for every tier. A per-tier override is deliberately not
    // offered yet: nothing in the design asks for it, and an unused override
    // is a configuration surface that has to be kept working for no reason.
    EvictionIndexConfig default_eviction;

    if (tier_config.isMember("v2")) {
        const auto& v2 = tier_config["v2"];
        if (v2.isMember("block_index")) {
            const auto& node = v2["block_index"];
            if (node.isMember("shard_count")) {
                config.block_index.shard_count = node["shard_count"].asUInt64();
            }
            if (node.isMember("max_load_factor")) {
                config.block_index.max_load_factor =
                    node["max_load_factor"].asFloat();
            }
        }
        if (v2.isMember("block_registry") &&
            v2["block_registry"].isMember("shard_count")) {
            config.registry.shard_count =
                v2["block_registry"]["shard_count"].asUInt64();
        }
        if (v2.isMember("events")) {
            const auto& node = v2["events"];
            if (node.isMember("shard_count")) {
                config.events.shard_count = node["shard_count"].asUInt64();
            }
            if (node.isMember("event_queue_capacity")) {
                config.events.event_queue_capacity =
                    node["event_queue_capacity"].asUInt64();
            }
            if (node.isMember("movement_queue_capacity")) {
                // The command queue moved to the migration engine, where it
                // became one queue per route. The old name still configures
                // the total, so an existing tier file keeps meaning what it
                // meant.
                config.migration.max_queued_requests =
                    node["movement_queue_capacity"].asUInt64();
            }
            if (node.isMember("movement_worker_count")) {
                config.movement_worker_count =
                    node["movement_worker_count"].asUInt64();
            }
        }
        if (v2.isMember("placement_policy")) {
            // The monolithic policy is gone; the JSON block keeps its name and
            // its keys so an existing tier file still means what it meant,
            // and each key now lands where that decision actually lives.
            const auto& node = v2["placement_policy"];
            if (node.isMember("offload_high_watermark")) {
                config.movement.offload_high_watermark =
                    node["offload_high_watermark"].asDouble();
            }
            if (node.isMember("onboard_min_frequency")) {
                // Reads only now: the old counter was bumped on commit too.
                config.movement.onboard_min_read_heat =
                    node["onboard_min_frequency"].asDouble();
            }
            // The band thresholds and the sketch belong to the per-tier
            // eviction ordering, which has its own block.
            if (node.isMember("sketch_capacity")) {
                default_eviction.sketch_capacity =
                    node["sketch_capacity"].asUInt64();
            }
            if (node.isMember("band_warm_threshold")) {
                default_eviction.band_warm_threshold =
                    node["band_warm_threshold"].asUInt64();
            }
            if (node.isMember("band_hot_threshold")) {
                default_eviction.band_hot_threshold =
                    node["band_hot_threshold"].asUInt64();
            }
            if (node.isMember("band_veryhot_threshold")) {
                default_eviction.band_veryhot_threshold =
                    node["band_veryhot_threshold"].asUInt64();
            }
        }
        if (v2.isMember("copier")) {
            const auto& node = v2["copier"];
            if (node.isMember("staging_buffer_bytes")) {
                config.copier.staging_buffer_bytes =
                    ParseByteSize(node["staging_buffer_bytes"]);
            }
            if (node.isMember("copy_timeout_ms")) {
                config.copier.copy_timeout = std::chrono::milliseconds(
                    node["copy_timeout_ms"].asInt64());
            }
        }
        if (v2.isMember("eviction_index")) {
            const auto& node = v2["eviction_index"];
            if (node.isMember("type")) {
                default_eviction.type = node["type"].asString();
            }
            if (node.isMember("max_victim_candidates")) {
                default_eviction.max_victim_candidates =
                    node["max_victim_candidates"].asUInt64();
            }
            if (node.isMember("sketch_capacity")) {
                default_eviction.sketch_capacity =
                    node["sketch_capacity"].asUInt64();
            }
        }
        if (v2.isMember("max_registration_retry")) {
            config.max_registration_retry =
                v2["max_registration_retry"].asUInt();
        }
        if (v2.isMember("stop_drain_timeout_ms")) {
            config.stop_drain_timeout = std::chrono::milliseconds(
                v2["stop_drain_timeout_ms"].asUInt64());
        }
        if (v2.isMember("lease_shard_count")) {
            config.lease_shard_count = v2["lease_shard_count"].asUInt64();
        }
        if (v2.isMember("hot_key_snapshot_limit")) {
            config.frequency_tracker.max_snapshot_keys =
                v2["hot_key_snapshot_limit"].asUInt64();
        }
        if (v2.isMember("allocation_failure")) {
            const auto& node = v2["allocation_failure"];
            if (node.isMember("try_evict")) {
                config.allocation_failure.try_evict =
                    node["try_evict"].asBool();
            }
            if (node.isMember("max_evict_rounds")) {
                config.allocation_failure.max_evict_rounds =
                    node["max_evict_rounds"].asUInt();
            }
            if (node.isMember("evict_timeout_ms")) {
                config.allocation_failure.evict_timeout =
                    std::chrono::milliseconds(
                        node["evict_timeout_ms"].asUInt());
            }
            if (node.isMember("reclaim_margin_bytes")) {
                config.allocation_failure.reclaim_margin_bytes =
                    ParseByteSize(node["reclaim_margin_bytes"]);
            }
        }
    }

    for (auto& tiler : config.tilers) tiler.eviction = default_eviction;

    auto valid = ValidateDataManagerV2Config(config);
    if (!valid) return tl::make_unexpected(valid.error());
    return config;
}

tl::expected<void, ErrorCode> ValidateDataManagerV2Config(
    const DataManagerV2Config& config) {
    if (config.registry.shard_count == 0) {
        LOG(ERROR) << "block_registry.shard_count must be greater than zero";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto index_valid = ValidateBlockIndexConfig(config.block_index);
    if (!index_valid) return index_valid;
    auto failure_valid =
        ValidateAllocationFailurePolicy(config.allocation_failure);
    if (!failure_valid) return failure_valid;
    auto events_valid = ValidateEventCenterConfig(config.events);
    if (!events_valid) return events_valid;
    auto movement_valid = ValidateMovementConsumerConfig(config.movement);
    if (!movement_valid) return movement_valid;
    auto placement_valid =
        ValidateTierPlacementPolicyConfig(config.tier_placement);
    if (!placement_valid) return placement_valid;
    auto copier_valid = ValidateCopierConfig(config.copier);
    if (!copier_valid) return copier_valid;
    auto tracker_valid = ValidateMovementTrackerConfig(config.movement_tracker);
    if (!tracker_valid) return tracker_valid;
    auto migration_valid = ValidateMigrationSchedulerConfig(config.migration);
    if (!migration_valid) return migration_valid;
    auto frequency_valid =
        ValidateFrequencyTrackerConfig(config.frequency_tracker);
    if (!frequency_valid) return frequency_valid;
    for (const auto& tiler : config.tilers) {
        auto eviction_valid = ValidateEvictionIndexConfig(tiler.eviction);
        if (!eviction_valid) return eviction_valid;
    }
    if (config.max_registration_retry == 0) {
        LOG(ERROR) << "max_registration_retry must be greater than zero";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.lease_shard_count == 0 || config.movement_worker_count == 0) {
        LOG(ERROR) << "lease_shard_count and movement_worker_count must be "
                      "greater than zero";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.stop_drain_timeout <= std::chrono::milliseconds::zero()) {
        LOG(ERROR) << "stop_drain_timeout_ms must be greater than zero";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

thread_local bool DataManagerV2::reclaiming_ = false;

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

DataManagerV2::DataManagerV2(const DataManagerV2Config& config,
                             std::shared_ptr<TransferEngine> transfer_engine,
                             MetadataCallbacks callbacks,
                             std::shared_ptr<TierMetric> tier_metric,
                             std::shared_ptr<KeyRetentionMetric> key_retention,
                             std::shared_ptr<Clock> clock)
    : config_(config),
      clock_(std::move(clock)),
      block_registry_(config.registry),
      transfer_engine_(std::move(transfer_engine)),
      metadata_callbacks_(std::move(callbacks)),
      tier_metric_(std::move(tier_metric)),
      key_retention_metric_(std::move(key_retention)) {
    CHECK(clock_ != nullptr) << "DataManagerV2 requires a Clock";
    rectify_callback_ = metadata_callbacks_.rectify_route;
    leases_ = std::make_unique<LeaseManager>(config_.key_lease, clock_,
                                             config_.lease_shard_count);
    // Shares the injected clock: heat decays with time, so a test that cannot
    // move the clock cannot test decay at all.
    frequency_tracker_ =
        std::make_shared<FrequencyTracker>(config_.frequency_tracker, clock_);

    // GetCoroExecutor() must never return null, including after Stop(), so the
    // pool is created up front and only stopped (never released) on shutdown.
    unsigned coro_threads =
        static_cast<unsigned>(config_.local_transfer.te_async_poll_worker_num);
    if (coro_threads == 0) coro_threads = std::thread::hardware_concurrency();
    if (coro_threads == 0) coro_threads = 1;
    coro_executor_pool_ =
        std::make_shared<coro_io::io_context_pool>(coro_threads);
    std::thread([pool = coro_executor_pool_]() { pool->run(); }).detach();

    // A dedicated pool for TE completion polling, so a poll loop never sits on
    // an RPC thread. Zero keeps the wait on the caller's thread.
    if (config_.local_transfer.te_async_poll_worker_num > 0) {
        te_wait_pool_ =
            std::make_shared<coro_io::io_context_pool>(static_cast<unsigned>(
                config_.local_transfer.te_async_poll_worker_num));
        std::thread([pool = te_wait_pool_]() { pool->run(); }).detach();
    }

    // The coordinator first: the copy engine takes it so a TE-mode deployment
    // gets a TransferEngine copier instead of silently falling back to a
    // bounce buffer, which is what V2 did before.
    transfer_coordinator_ =
        std::make_unique<TransferCoordinator>(transfer_engine_, te_wait_pool_);
    local_copy_engine_ = std::make_unique<LocalCopyEngine>(
        config_.local_transfer, transfer_coordinator_.get(), config_.copier,
        clock_);
}

DataManagerV2::~DataManagerV2() {
    Stop();
    Destroy();
}

tl::expected<void, ErrorCode> DataManagerV2::Init() {
    if (initialized_) return {};
    if (config_.tilers.empty()) {
        LOG(ERROR) << "DataManagerV2 requires at least one tiler";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    // Again here, not only in the parser. A configuration built in C++ rather
    // than read from JSON reached this point completely unchecked, so a zero
    // shard count became a division by zero at run time instead of a refusal
    // at start-up.
    auto valid = ValidateDataManagerV2Config(config_);
    if (!valid) return valid;

    std::shared_ptr<TransferEngine> pool_engine =
        config_.register_tiers_with_transfer_engine ? transfer_engine_
                                                    : nullptr;
    for (const auto& tiler_config : config_.tilers) {
        auto pool = CreateBlockPool(tiler_config.pool, pool_engine);
        if (!pool) {
            LOG(ERROR) << "Failed to create the block pool for tiler "
                       << tiler_config.logical.tiler_id
                       << ", error=" << toString(pool.error());
            return tl::make_unexpected(pool.error());
        }
        tilers_.by_priority.push_back(std::make_unique<TilerManager>(
            tiler_config.logical, config_.block_index, std::move(pool.value()),
            block_registry_, EventPublisher(), tiler_config.eviction));
    }
    tilers_.Rebuild();

    std::vector<UUID> tiler_ids;
    tiler_ids.reserve(tilers_.Size());
    for (const auto& tiler : tilers_.by_priority) {
        tiler_ids.push_back(tiler->Id());
    }
    allocation_failure_metrics_ =
        std::make_unique<AllocationFailureMetrics>(tiler_ids);

    auto events_valid = ValidateEventCenterConfig(config_.events);
    if (!events_valid) return tl::make_unexpected(events_valid.error());

    // The topology as a graph rather than as a sorted list. Nothing derives
    // an edge from priority any more; FromPriorityChain states the classic
    // chain explicitly, which is what a configuration with no explicit
    // topology means.
    std::vector<TierNode> nodes;
    nodes.reserve(tilers_.Size());
    for (const auto& tiler : tilers_.by_priority) {
        TierNode node;
        node.tiler_id = tiler->Id();
        node.priority = tiler->Priority();
        node.capacity = tiler->Capacity();
        node.addressable = tiler->IsTeAddressable();
        node.domain = tiler->Capabilities().direct_cpu_access
                          ? CopyDomain::kHostMemory
                          : CopyDomain::kFileOrBlock;
        nodes.push_back(node);
    }
    auto graph = TierGraph::FromPriorityChain(std::move(nodes));
    if (!graph) return tl::make_unexpected(graph.error());
    tier_graph_ = std::make_shared<const TierGraph>(std::move(graph.value()));

    auto placement =
        CreateTierPlacementPolicy(config_.tier_placement, tier_graph_);
    if (!placement) return tl::make_unexpected(placement.error());
    tier_placement_ = std::move(placement.value());

    // Order matters here. The migration engine is the movement sink, the
    // tracker is what makes a command unique, and the consumer needs both --
    // so both exist before the consumer, and the consumer before the center it
    // registers with.
    movement_tracker_ =
        std::make_unique<MovementTracker>(config_.movement_tracker, clock_);
    migration_engine_ = std::make_unique<MigrationEngine>(
        &tilers_, &block_registry_, local_copy_engine_.get(),
        &metadata_callbacks_,
        [this](
            const UUID& tiler_id, size_t size, size_t alignment,
            AllocationSource source) -> tl::expected<MutableBlock, ErrorCode> {
            TilerManager* tiler = tilers_.Find(tiler_id);
            if (tiler == nullptr) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            return AllocateWithPolicy(*tiler, size, alignment, source);
        },
        clock_, config_.migration);

    event_center_ = std::make_shared<EventCenter>(config_.events);

    MovementConsumerDeps deps;
    deps.tilers = &tilers_;
    deps.registry = &block_registry_;
    deps.placement = tier_placement_.get();
    deps.frequency = frequency_tracker_.get();
    deps.movement = movement_tracker_.get();
    deps.sink = migration_engine_.get();
    deps.clock = clock_;

    auto offload = CreateOffloadConsumer(config_.movement, deps);
    if (!offload) return tl::make_unexpected(offload.error());
    offload_consumer_ = std::move(offload.value());
    auto onboard = CreateOnboardConsumer(config_.movement, deps);
    if (!onboard) return tl::make_unexpected(onboard.error());
    onboard_consumer_ = std::move(onboard.value());

    // Registered before Start, which is when the consumer set freezes.
    for (EventConsumer* consumer :
         {offload_consumer_.get(), onboard_consumer_.get()}) {
        auto registered = event_center_->RegisterConsumer(consumer);
        if (!registered) return tl::make_unexpected(registered.error());
    }
    event_center_->Start();
    // Attached only now: the sink depends on the policy, which needed the full
    // topology above. Nothing has served a request yet.
    for (const auto& tiler : tilers_.by_priority) {
        tiler->SetEventPublisher(event_center_->Publisher());
    }

    evict_engine_ = std::make_unique<EvictEngine>(
        &tilers_, &block_registry_, &metadata_callbacks_, clock_,
        [this](const std::string& key) {
            // Same rule Delete applies: once the last replica is gone the key
            // stops being tracked, or the tracker grows for the process
            // lifetime and reports keys that no longer exist.
            frequency_tracker_->Remove(key);
        });

    const size_t workers = std::max<size_t>(1, config_.movement_worker_count);
    movement_workers_.reserve(workers);
    for (size_t i = 0; i < workers; ++i) {
        movement_workers_.emplace_back([this] { MovementWorkerMain(); });
    }

    if (tier_metric_) {
        for (const auto& tiler : tilers_.by_priority) {
            const TierView view = tiler->GetView();
            // Borrowed pointer: the tilers are members of this object and the
            // metric sink is injected from outside and outlives it, so the
            // callback cannot outlive what it reads.
            TilerManager* borrowed = tiler.get();
            tier_metric_->RegisterTier(
                view.id, view.GetName(), view.type, view.priority,
                view.capacity,
                [borrowed]() -> size_t { return borrowed->Usage(); });
        }
    }

    initialized_ = true;
    LOG(INFO) << "DataManagerV2 initialized with " << tilers_.Size()
              << " logical tilers, registry_shards="
              << block_registry_.ShardCount()
              << ", index_shards=" << config_.block_index.shard_count;
    return {};
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

AllocationFailurePolicyConfig DataManagerV2::AllocationPolicyFor(
    const UUID& tiler_id) const {
    for (const auto& tiler : config_.tilers) {
        if (tiler.logical.tiler_id == tiler_id &&
            tiler.allocation_failure_override.has_value()) {
            // A per-tiler override replaces the global policy wholesale; there
            // is deliberately no field-level merge, which would make the
            // effective policy hard to reason about.
            return *tiler.allocation_failure_override;
        }
    }
    return config_.allocation_failure;
}

std::vector<TilerManager*> DataManagerV2::CandidateTilers(
    AllocationSource source) const {
    switch (source) {
        case AllocationSource::kMigration:
            // The only source allowed to target a slow tier: that is what
            // keeps slow-tier capacity an offload destination rather than a
            // write fallback.
            return tilers_.All();
        case AllocationSource::kPut:
        case AllocationSource::kPreWrite:
        case AllocationSource::kWriteRemoteData:
        case AllocationSource::kOnboard:
        case AllocationSource::kCount:
            break;
    }
    // Everything else is a request-path source and may only land on a tiler
    // that can expose an address, i.e. never on a slow tier.
    return tilers_.TeAddressable();
}

tl::expected<MutableBlock, ErrorCode> DataManagerV2::AllocateWithPolicy(
    TilerManager& tiler, size_t size_bytes, size_t alignment,
    AllocationSource source) {
    auto allocated = tiler.Allocate(size_bytes, alignment);
    if (allocated) return allocated;
    if (allocated.error() != ErrorCode::NO_AVAILABLE_HANDLE) {
        // Only exhaustion may trigger reclaim: IO, parameter, shutdown and
        // internal errors are returned untouched.
        return allocated;
    }

    // Counted whether or not eviction is allowed, so the metric answers "how
    // often did we run out" independently of what the policy did about it.
    // The matching fact event was already published by TilerManager::Allocate.
    auto& counters = allocation_failure_metrics_->For(tiler.Id(), source);
    counters.failures.fetch_add(1, std::memory_order_relaxed);

    const AllocationFailurePolicyConfig policy =
        AllocationPolicyFor(tiler.Id());
    if (!policy.try_evict || evict_engine_ == nullptr) {
        counters.evict_disabled.fetch_add(1, std::memory_order_relaxed);
        return allocated;
    }

    size_t target_bytes = size_bytes;
    if (policy.reclaim_margin_bytes >
        std::numeric_limits<size_t>::max() - target_bytes) {
        LOG(ERROR) << "reclaim_margin_bytes overflows the allocation size";
        return allocated;
    }
    target_bytes += policy.reclaim_margin_bytes;

    // Reclaiming a sole replica demotes it, which allocates on the slower
    // tier -- and that allocation would otherwise start its own reclaim, with
    // its own fresh deadline, on this same thread. One level is a bounded
    // cost; nesting is not, so the second level simply reports exhaustion.
    if (reclaiming_) {
        counters.evict_disabled.fetch_add(1, std::memory_order_relaxed);
        return allocated;
    }
    struct ReclaimScope {
        bool& flag;
        explicit ReclaimScope(bool& f) : flag(f) { flag = true; }
        ~ReclaimScope() { flag = false; }
    } reclaim_scope(reclaiming_);

    // One absolute deadline for the whole loop, read from the injected clock
    // so a test can drive the timeout without sleeping.
    const auto deadline = clock_->Now() + policy.evict_timeout;
    tl::expected<MutableBlock, ErrorCode> last = std::move(allocated);

    for (uint32_t round = 0; round < policy.max_evict_rounds; ++round) {
        if (clock_->Now() >= deadline) {
            counters.evict_timed_out.fetch_add(1, std::memory_order_relaxed);
            break;
        }
        counters.evict_attempted.fetch_add(1, std::memory_order_relaxed);

        ReclaimRequest request;
        request.tiler_id = tiler.Id();
        request.source = source;
        request.allocation_size = size_bytes;
        request.reclaim_target_bytes = target_bytes;
        request.round = round;
        request.deadline = deadline;

        auto reclaimed = evict_engine_->ReclaimOneRound(request);
        if (!reclaimed) {
            // A reclaim problem must not mask the allocation error the caller
            // actually needs to see.
            break;
        }

        // Retried regardless of what the round reported: a concurrent release
        // or an allocator extent merge can make this succeed even when the
        // round reclaimed nothing, and a round that claims to have hit its
        // target can still leave the allocation failing. The allocator is the
        // only authority here.
        last = tiler.Allocate(size_bytes, alignment);
        if (last) {
            counters.retry_succeeded.fetch_add(1, std::memory_order_relaxed);
            return last;
        }
        if (last.error() != ErrorCode::NO_AVAILABLE_HANDLE) {
            return last;  // a shutdown or a real error: surface it as-is
        }
        if (reclaimed->deadline_reached) {
            counters.evict_timed_out.fetch_add(1, std::memory_order_relaxed);
            break;
        }
    }

    counters.retry_failed.fetch_add(1, std::memory_order_relaxed);
    // Never another tier: a full fast tier is NO_AVAILABLE_HANDLE, not a
    // silent write to slower storage (section 6.1.1).
    return last;
}

std::optional<DataManagerV2::ReplicaSite> DataManagerV2::FindReplica(
    const BlockRegistrationHandle& registration,
    std::optional<UUID> tier_id) const {
    if (!registration) return std::nullopt;

    if (tier_id.has_value()) {
        TilerManager* tiler = tilers_.Find(*tier_id);
        if (tiler == nullptr) return std::nullopt;
        auto matched = tiler->Match(registration);
        if (!matched) return std::nullopt;
        return ReplicaSite{tiler, std::move(matched.value())};
    }

    // Priority order. The registry's presence markers are only a hint, so the
    // authoritative answer always comes from a TilerManager lookup.
    for (const auto& tiler : tilers_.by_priority) {
        auto matched = tiler->Match(registration);
        if (matched) {
            return ReplicaSite{tiler.get(), std::move(matched.value())};
        }
    }
    return std::nullopt;
}

tl::expected<size_t, ErrorCode> DataManagerV2::LookupReplicaSize(
    std::string_view key) const {
    auto registration = block_registry_.Match(key);
    if (!registration.has_value()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto site = FindReplica(*registration, std::nullopt);
    if (!site.has_value()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    return site->block.Size();
}

void DataManagerV2::NotifyAddReplica(const std::string& key,
                                     const UUID& tier_id, size_t size) {
    if (!metadata_callbacks_.add_replica) return;
    auto result = metadata_callbacks_.add_replica(key, tier_id, size);
    if (!result) {
        LOG(WARNING) << "add-replica callback failed, key=" << key
                     << ", error=" << toString(result.error());
    }
}

void DataManagerV2::NotifyRemoveReplica(const std::string& key,
                                        const UUID& tier_id) {
    if (!metadata_callbacks_.remove_replica) return;
    auto result = metadata_callbacks_.remove_replica(key, tier_id);
    if (!result) {
        LOG(WARNING) << "remove-replica callback failed, key=" << key
                     << ", error=" << toString(result.error());
    }
}

tl::expected<void, ErrorCode> DataManagerV2::CommitBlock(const std::string& key,
                                                         TilerManager& tiler,
                                                         CompletedBlock&& block,
                                                         size_t size_bytes) {
    CompletedBlock owned = std::move(block);

    for (uint32_t attempt = 0; attempt < config_.max_registration_retry;
         ++attempt) {
        auto registration = block_registry_.Register(key);
        if (!registration) {
            return tl::make_unexpected(registration.error());
        }
        const BlockRegistrationHandle handle = registration.value();
        BlockId block_id;

        {
            auto guard = handle.LockMutation();
            if (guard.IsRetired()) {
                // A concurrent Delete retired this identity between Register
                // and the guard; take a fresh one and try again.
                continue;
            }
            // Exact duplicate check across every tiler, under the guard.
            if (FindReplica(handle, std::nullopt).has_value()) {
                return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
            }
            auto registered = tiler.RegisterWithHandle(std::move(owned), handle,
                                                       /*defer_notify=*/true);
            if (!registered) {
                return tl::make_unexpected(registered.error());
            }
            block_id = registered->Id();
        }
        // Everything below runs with the mutation guard released. Publishing
        // under it would push into an event queue -- and, on a full queue, run
        // a whole inline policy update -- while the key is locked.
        //
        // The tracker is updated BEFORE the event is published, and the order
        // is load-bearing: a consumer reacting to the commit queries the
        // tracker, and the other order lets it observe a key that has no
        // record yet -- a race it has no way to detect or retry.
        //
        // V1 commits with record_access=true, so a freshly written key starts
        // out warm rather than invisible to hot-key recovery. It goes in as a
        // commit rather than a read: it must count towards hot-key reporting,
        // and it must NOT count towards "this key is in demand on a slow
        // tier", which is what the onboard decision reads.
        frequency_tracker_->OnCommit(handle.Id(), key);
        tiler.NotifyRegistered(handle, block_id, size_bytes);
        if (tier_metric_) tier_metric_->OnReplicaAdded(tiler.Id());
        if (key_retention_metric_) {
            key_retention_metric_->OnKeyCreated(
                std::chrono::steady_clock::now());
        }
        // A commit of a key rectify recently asked Master to drop means that
        // miss was a false positive.
        NoteCommitForRectifyWitness(key);
        NotifyAddReplica(key, tiler.Id(), size_bytes);
        return {};
    }

    registration_retry_exhausted_.fetch_add(1, std::memory_order_relaxed);
    LOG(ERROR) << "Gave up registering key=" << key << " after "
               << config_.max_registration_retry
               << " attempts (concurrent delete churn)";
    return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
}

tl::expected<TilerManager*, ErrorCode> DataManagerV2::ResolveForwardWriteTiler(
    std::optional<UUID> tier_id) const {
    if (tier_id.has_value()) {
        TilerManager* tiler = tilers_.Find(*tier_id);
        if (tiler == nullptr) {
            LOG(ERROR) << "Unknown tier id requested for a forward write";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (!tiler->IsTeAddressable()) {
            // Deliberate divergence from V1, which silently redirected such a
            // request to DRAM. The caller is going to hand this address to a
            // peer, so answering with a different tier's memory than it asked
            // for is worse than refusing.
            LOG(ERROR) << "Requested tier " << *tier_id
                       << " is not TE-addressable; refusing to publish an "
                          "address for it";
            return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
        }
        return tiler;
    }

    auto candidates = CandidateTilers(AllocationSource::kPreWrite);
    if (candidates.empty()) {
        LOG(ERROR) << "No TE-addressable tier is configured for forward writes";
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    return candidates.front();
}

tl::expected<RemoteBufferDesc, ErrorCode> DataManagerV2::MakeRemoteBufferDesc(
    const std::optional<TransferAddress>& address) const {
    // A block that exposes no address must never yield a descriptor. V1's bug
    // was exactly this: it produced addr == 0 and the failure only surfaced on
    // the requester side, or worse, an unregistered staging address slipped
    // through every check and reached RDMA.
    if (!address.has_value() || address->addr == 0 || address->size == 0) {
        LOG(ERROR) << "Refusing to publish a buffer with no usable address";
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    RemoteBufferDesc desc;
    desc.segment_endpoint = config_.local_transfer.te_endpoint;
    desc.addr = address->addr;
    desc.size = address->size;
    return desc;
}

void DataManagerV2::MovementWorkerMain() {
    // The engine owns the queues, the batching and the settle; this loop only
    // lends it a thread. The threads stay here because they call into the
    // engine, so member declaration order is what guarantees they are joined
    // before it is destroyed.
    // RunOnce returns 0 only once the engine has stopped, so this is the
    // whole loop.
    while (migration_engine_->RunOnce() > 0) {
    }
}

// ---------------------------------------------------------------------------
// Local IO
// ---------------------------------------------------------------------------

tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode> DataManagerV2::Put(
    std::string_view key, std::vector<Slice>& slices) {
    if (key.empty() || slices.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto total = TotalSliceSize(slices);
    if (!total) return tl::make_unexpected(total.error());
    if (*total == 0) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    auto guard = lifecycle_->Acquire();
    if (!guard) return tl::make_unexpected(guard.error());

    // Claim the key before allocating anything: N concurrent writers of the
    // same key then produce exactly one allocation (section 5.9).
    const auto deadline = clock_->Now() + leases_->LeaseDuration();
    auto token = leases_->ReservePendingWrite(key, deadline);
    if (!token) return tl::make_unexpected(token.error());

    const std::string owned_key(key);
    auto release_claim = [this, owned_key, write_token = *token]() {
        (void)leases_->TakePendingWrite(owned_key, write_token);
    };

    // Reject a duplicate before spending an allocation on it.
    if (auto existing = block_registry_.Match(owned_key);
        existing.has_value() &&
        FindReplica(*existing, std::nullopt).has_value()) {
        release_claim();
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }

    auto candidates = CandidateTilers(AllocationSource::kPut);
    if (candidates.empty()) {
        release_claim();
        LOG(ERROR) << "Put has no TE-addressable tiler to place data on";
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    TilerManager& target = *candidates.front();

    auto block = AllocateWithPolicy(target, *total, /*alignment=*/0,
                                    AllocationSource::kPut);
    if (!block) {
        release_claim();
        return tl::make_unexpected(block.error());
    }

    auto attached = leases_->AttachPendingWriteTransaction(
        owned_key, *token, target.Id(),
        std::variant<MutableBlock, CompletedBlock>(std::move(block.value())));
    if (!attached) {
        release_claim();
        return tl::make_unexpected(attached.error());
    }

    // The copy and the commit happen inside Wait(), on the caller's thread, so
    // the task owns the key and carries the lifecycle guard.
    auto task = [this, owned_key, write_token = *token, slices, tiler = &target,
                 lifecycle_guard = std::move(guard.value())]() mutable
        -> tl::expected<void, ErrorCode> {
        if (!lifecycle_guard.IsUsable()) {
            // Nothing below may touch the captured `this`. Stop() bounds its
            // wait and then cancels, so reaching here means the manager may
            // already be gone -- the guard keeps the gate alive, not the
            // manager. The abandoned write claim needs no cleanup: it carries
            // a deadline and expires on its own.
            return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
        }

        // The claim is released only once the commit is done, on every exit
        // path. Releasing it earlier would open a window in which a concurrent
        // Put of the same key sees neither a claim nor a committed replica and
        // allocates a second full-size block for an object that is about to
        // exist. V1 has the same ordering: it erases the pending record after
        // Commit, not before.
        struct ClaimGuard {
            LeaseManager* leases;
            const std::string* key;
            UUID token;
            ~ClaimGuard() { (void)leases->TakePendingWrite(*key, token); }
        } claim{leases_.get(), &owned_key, write_token};

        auto detached =
            leases_->DetachPendingWriteTransaction(owned_key, write_token);
        if (!detached) return tl::make_unexpected(detached.error());
        if (!std::holds_alternative<MutableBlock>(detached->transaction)) {
            return tl::make_unexpected(ErrorCode::INVALID_WRITE);
        }
        MutableBlock mutable_block =
            std::move(std::get<MutableBlock>(detached->transaction));
        const size_t size_bytes = mutable_block.Size();

        auto written =
            local_copy_engine_->WriteFromSlices(slices, mutable_block);
        if (!written) return tl::make_unexpected(written.error());

        auto completed = std::move(mutable_block).Complete(owned_key);
        if (!completed) return tl::make_unexpected(completed.error());

        return CommitBlock(owned_key, *tiler, std::move(completed.value()),
                           size_bytes);
    };
    return CallableTaskHandle<void>::Create(std::move(task));
}

tl::expected<ReadTaskHandle, ErrorCode> DataManagerV2::Get(
    std::string_view key, const std::vector<Slice>& slices) {
    if (key.empty() || slices.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto total = TotalSliceSize(slices);
    if (!total) return tl::make_unexpected(total.error());

    auto guard = lifecycle_->Acquire();
    if (!guard) return tl::make_unexpected(guard.error());

    auto registration = block_registry_.Match(key);
    if (!registration.has_value()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto site = FindReplica(*registration, std::nullopt);
    if (!site.has_value()) {
        // A live registration with no exact replica is not existence.
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    const size_t source_size = site->block.Size();
    if (*total < source_size) {
        LOG(ERROR) << "Get destination is too small for key=" << key
                   << ", required=" << source_size << ", provided=" << *total;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Statistics and the access fact happen outside every index shard lock.
    // The fact is what the placement policy runs on: without it its LRU only
    // ever sees commit order, so eviction would ignore reads entirely and a
    // hot key on a slow tier could never be onboarded.
    frequency_tracker_->RecordAccess(registration->Id(), key);
    site->block.RecordAccess(
        access_tick_.fetch_add(1, std::memory_order_relaxed));
    site->tiler->NotifyAccess(*registration, site->block);

    ReadTaskHandle result;
    result.data_size = static_cast<int64_t>(source_size);
    result.is_local = true;
    // The snapshot travels with the task: a concurrent Delete may detach the
    // entry, but this read still completes against live memory.
    result.task_handle = CallableTaskHandle<void>::Create(
        [this, block = std::move(site->block), slices,
         lifecycle_guard = std::move(
             guard.value())]() mutable -> tl::expected<void, ErrorCode> {
            if (!lifecycle_guard.IsUsable()) {
                return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
            }
            return local_copy_engine_->ReadToSlices(block, slices);
        });
    return result;
}

tl::expected<ReadTaskHandle, ErrorCode> DataManagerV2::Get(
    std::string_view key, std::shared_ptr<ClientBufferAllocator> allocator) {
    if (key.empty() || !allocator) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Non-recording: the delegated Get below records this read once.
    auto size = LookupReplicaSize(key);
    if (!size) return tl::make_unexpected(size.error());

    auto buffer = allocator->allocate(*size);
    if (!buffer) {
        LOG(ERROR) << "Failed to allocate a read buffer for key=" << key;
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    auto read_buf = std::make_shared<BufferHandle>(std::move(*buffer));
    const std::vector<Slice> slices = {{read_buf->ptr(), *size}};

    auto result = Get(key, slices);
    if (result) {
        result->read_buf = std::move(read_buf);
    }
    return result;
}

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

tl::expected<std::pair<UUID, uint64_t>, ErrorCode> DataManagerV2::Query(
    std::string_view key) {
    if (key.empty()) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    auto guard = lifecycle_->Acquire();
    if (!guard) return tl::make_unexpected(guard.error());

    auto registration = block_registry_.Match(key);
    if (!registration.has_value()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto site = FindReplica(*registration, std::nullopt);
    if (!site.has_value()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    // A successful Query counts as an access; a miss does not.
    frequency_tracker_->RecordAccess(registration->Id(), key);
    return std::make_pair(site->tiler->Id(),
                          static_cast<uint64_t>(site->block.Size()));
}

tl::expected<size_t, ErrorCode> DataManagerV2::QueryObjectSize(
    std::string_view key) {
    if (key.empty()) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    auto guard = lifecycle_->Acquire();
    if (!guard) return tl::make_unexpected(guard.error());

    auto size = LookupReplicaSize(key);
    if (size) {
        // V1 reaches this through TieredBackend::Get(record_access=true), so a
        // successful size query counts as an access there too.
        auto registration = block_registry_.Match(key);
        if (registration.has_value()) {
            frequency_tracker_->RecordAccess(registration->Id(), key);
        }
    }
    return size;
}

bool DataManagerV2::Exist(std::string_view key,
                          std::optional<UUID> tier_id) const {
    if (key.empty()) return false;
    auto registration = block_registry_.Match(key);
    if (!registration.has_value()) return false;
    // Exact, never satisfied from a presence hint, and it must not disturb the
    // access statistics.
    return FindReplica(*registration, tier_id).has_value();
}

std::vector<UUID> DataManagerV2::GetReplicaTierIds(std::string_view key) const {
    std::vector<UUID> out;
    if (key.empty()) return out;
    auto registration = block_registry_.Match(key);
    if (!registration.has_value()) return out;

    for (const auto& tiler : tilers_.by_priority) {
        if (tiler->Match(*registration)) out.push_back(tiler->Id());
    }
    return out;
}

tl::expected<void, ErrorCode> DataManagerV2::Delete(std::string_view key,
                                                    std::optional<UUID> tier_id,
                                                    bool notify_master) {
    if (key.empty()) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    auto guard = lifecycle_->Acquire();
    if (!guard) return tl::make_unexpected(guard.error());

    auto registration = block_registry_.Match(key);
    if (!registration.has_value()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    const BlockRegistrationHandle handle = *registration;

    struct DeferredNotify {
        TilerManager* tiler;
        BlockId block_id;
        size_t size_bytes;
    };
    std::vector<UUID> removed;
    std::vector<DeferredNotify> deferred;
    {
        auto mutation = handle.LockMutation();
        if (mutation.IsRetired()) {
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }

        std::vector<TilerManager*> targets;
        if (tier_id.has_value()) {
            TilerManager* tiler = tilers_.Find(*tier_id);
            if (tiler == nullptr) {
                // Naming a tier that does not exist is a different mistake
                // from naming a key that does not exist, and V1 distinguishes
                // them. A caller deleting one specific replica needs to know
                // which of the two happened.
                return tl::make_unexpected(ErrorCode::TIER_NOT_FOUND);
            }
            targets.push_back(tiler);
        } else {
            targets = tilers_.All();
        }

        // Decide, under the guard, what this call actually removes and what
        // would survive it.
        std::vector<TilerManager*> holding;
        size_t surviving = 0;
        for (const auto& tiler : tilers_.by_priority) {
            if (!tiler->Match(handle)) continue;
            const bool targeted = std::find(targets.begin(), targets.end(),
                                            tiler.get()) != targets.end();
            if (targeted) {
                holding.push_back(tiler.get());
            } else {
                ++surviving;
            }
        }
        if (holding.empty()) {
            // Nothing to remove. Crucially, do not retire here: a live
            // registration with no replica belongs to an in-flight Put, and
            // retiring it would make that Put fail or re-register.
            //
            // "The key exists but not on that tier" is TIER_NOT_FOUND, as in
            // V1; only an unqualified delete of an absent key is
            // OBJECT_NOT_FOUND.
            return tl::make_unexpected(tier_id.has_value()
                                           ? ErrorCode::TIER_NOT_FOUND
                                           : ErrorCode::OBJECT_NOT_FOUND);
        }
        if (surviving == 0) {
            // Last replica goes: retire first, so a new Get misses at once and
            // a new Put for the same key gets a fresh identity.
            handle.Retire(mutation);
        }

        for (TilerManager* tiler : holding) {
            // One tiler at a time; two index shards are never held together.
            // The notification is deferred: it publishes an event, which must
            // not happen while this key's mutation guard is held.
            auto matched = tiler->Match(handle);
            const BlockId block_id = matched ? matched->Id() : BlockId{};
            const size_t size_bytes = matched ? matched->Size() : 0;
            matched = tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            if (tiler->Delete(handle, std::nullopt, /*defer_notify=*/true)) {
                removed.push_back(tiler->Id());
                deferred.push_back({tiler, block_id, size_bytes});
            }
        }
    }
    for (const auto& note : deferred) {
        note.tiler->NotifyDeleted(handle, note.block_id, note.size_bytes);
        if (tier_metric_) tier_metric_->OnReplicaRemoved(note.tiler->Id());
    }

    if (removed.empty()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    if (GetReplicaTierIds(key).empty()) {
        frequency_tracker_->Remove(key);
    }
    if (notify_master) {
        const std::string owned_key(key);
        for (const auto& tiler : removed) {
            NotifyRemoveReplica(owned_key, tiler);
        }
    }
    return {};
}

tl::expected<long, ErrorCode> DataManagerV2::RemoveAll() {
    auto guard = lifecycle_->Acquire();
    if (!guard) return tl::make_unexpected(guard.error());

    // Distinct keys, counted once even when replicated across tilers.
    std::unordered_set<std::string, StringHash, std::equal_to<>> keys;
    std::vector<std::pair<std::string, UUID>> removed;
    std::unordered_map<RegistrationId, BlockRegistrationHandle,
                       RegistrationIdHash>
        registrations;
    for (const auto& tiler : tilers_.by_priority) {
        // Drain works shard by shard and releases the detached entries here,
        // outside every lock.
        auto detached = tiler->DrainAll();
        for (const auto& entry : detached) {
            keys.insert(entry->block.key);
            removed.emplace_back(entry->block.key, tiler->Id());
            registrations.emplace(entry->block.registration.Id(),
                                  entry->block.registration);
        }
        detached.clear();
    }

    // Dropping every replica must retire every registration, exactly as Delete
    // does when it removes the last one. Skipping it would leave an identity
    // alive for as long as some reader still holds a detached snapshot, and a
    // later Put of the same key would reuse that identity -- putting the old
    // detached block and the new one under one registration, which invariant
    // 7.4.7 forbids.
    std::vector<BlockRegistrationHandle> retired;
    for (const auto& [id, handle] : registrations) {
        auto mutation = handle.LockMutation();
        if (mutation.IsRetired()) continue;
        // Re-checked under the guard: a Put racing the drain may already have
        // committed a fresh replica under this identity, and retiring it would
        // make that brand-new object invisible.
        if (FindReplica(handle, std::nullopt).has_value()) continue;
        handle.Retire(mutation);
        retired.push_back(handle);
    }
    // Presence markers are touched only after every mutation guard and index
    // shard lock has been released (section 7.3 constraint 2).
    for (const auto& handle : retired) {
        for (const auto& tiler : tilers_.by_priority) {
            handle.MarkAbsent(tiler->Id());
        }
    }

    // Nothing to forget separately: TilerManager::DrainAll clears that tier's
    // eviction index in the same call that empties its BlockIndex, so the two
    // cannot disagree about what the drain removed.

    for (const auto& [key, tier] : removed) {
        frequency_tracker_->Remove(key);
        NotifyRemoveReplica(key, tier);
    }
    return static_cast<long>(keys.size());
}

std::vector<TierView> DataManagerV2::GetTierViews() const {
    std::vector<TierView> views;
    views.reserve(tilers_.Size());
    for (const auto& tiler : tilers_.by_priority) {
        views.push_back(tiler->GetView());
    }
    return views;
}

void DataManagerV2::ForEachKeyBatch(
    const std::function<bool(std::vector<ReplicaLocation>&&)>& callback) const {
    if (!callback) return;

    // Per-replica, not per-key: HARecoveryManager resyncs metadata per
    // (key, tier_id) and GetLocalKeyCount sums batch sizes, so a per-key
    // granularity would change both.
    std::vector<ReplicaLocation> batch;
    for (const auto& tiler : tilers_.by_priority) {
        const size_t shard_count = tiler->ShardCount();
        for (size_t shard = 0; shard < shard_count; ++shard) {
            // One shard snapshot at a time: the whole index is never locked.
            auto entries = tiler->SnapshotShard(shard);
            for (const auto& entry : entries) {
                batch.push_back(ReplicaLocation{entry->block.key, tiler->Id(),
                                                entry->block.size_bytes});
                if (batch.size() >= kForEachKeyBatchSize) {
                    if (!callback(std::move(batch))) return;
                    batch.clear();
                }
            }
        }
    }
    if (!batch.empty()) {
        (void)callback(std::move(batch));
    }
}

AccessStats DataManagerV2::GetHotKeyStats(
    std::optional<size_t> hot_key_num) const {
    return frequency_tracker_->Snapshot(hot_key_num);
}

// ---------------------------------------------------------------------------
// Forward protocol: PreWrite / WriteCommit / WriteRevoke
// ---------------------------------------------------------------------------

tl::expected<PreWriteResponse, ErrorCode> DataManagerV2::PreWrite(
    std::string_view key, size_t size_bytes, std::optional<UUID> tier_id) {
    if (key.empty() || size_bytes == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto guard = lifecycle_->Acquire();
    if (!guard) return tl::make_unexpected(guard.error());

    // Resolve the target before claiming: a request naming an unusable tier is
    // rejected without disturbing the key.
    auto target = ResolveForwardWriteTiler(tier_id);
    if (!target) return tl::make_unexpected(target.error());

    // Claim first, allocate second: concurrent writers of one key cost exactly
    // one allocation between them.
    const auto deadline = clock_->Now() + leases_->LeaseDuration();
    auto token = leases_->ReservePendingWrite(key, deadline);
    if (!token) return tl::make_unexpected(token.error());

    const std::string owned_key(key);
    auto release_claim = [&] {
        (void)leases_->TakePendingWrite(owned_key, *token);
    };

    if (auto existing = block_registry_.Match(owned_key);
        existing.has_value() &&
        FindReplica(*existing, std::nullopt).has_value()) {
        release_claim();
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }

    auto block = AllocateWithPolicy(**target, size_bytes, /*alignment=*/0,
                                    AllocationSource::kPreWrite);
    if (!block) {
        release_claim();
        return tl::make_unexpected(block.error());
    }

    auto desc = MakeRemoteBufferDesc(block->GetTransferAddress());
    if (!desc) {
        // The block is dropped here, unlocked, by MutableBlock's destructor.
        release_claim();
        return tl::make_unexpected(desc.error());
    }

    auto attached = leases_->AttachPendingWriteTransaction(
        owned_key, *token, (*target)->Id(),
        std::variant<MutableBlock, CompletedBlock>(std::move(block.value())));
    if (!attached) {
        release_claim();
        return tl::make_unexpected(attached.error());
    }

    PreWriteResponse response;
    response.remote_buffer = std::move(desc.value());
    response.write_operation_id = *token;
    return response;
}

tl::expected<void, ErrorCode> DataManagerV2::WriteCommit(
    std::string_view key, const UUID& write_operation_id) {
    if (key.empty() || IsZeroUUID(write_operation_id)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto guard = lifecycle_->Acquire();
    if (!guard) return tl::make_unexpected(guard.error());

    const std::string owned_key(key);

    // The block comes out of the claim, but the claim itself stays until this
    // function returns: releasing it first would let a concurrent writer of
    // the same key allocate for an object that is about to exist.
    auto detached =
        leases_->DetachPendingWriteTransaction(owned_key, write_operation_id);
    if (!detached) return tl::make_unexpected(detached.error());

    struct ClaimGuard {
        LeaseManager* leases;
        const std::string* key;
        UUID token;
        ~ClaimGuard() { (void)leases->TakePendingWrite(*key, token); }
    } claim{leases_.get(), &owned_key, write_operation_id};

    TilerManager* tiler = tilers_.Find(detached->tiler_id);
    if (tiler == nullptr) {
        LOG(ERROR) << "WriteCommit: the tier the claim was placed on is gone, "
                      "key="
                   << key;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    CompletedBlock completed;
    if (std::holds_alternative<CompletedBlock>(detached->transaction)) {
        completed = std::move(std::get<CompletedBlock>(detached->transaction));
    } else {
        auto finished = std::move(std::get<MutableBlock>(detached->transaction))
                            .Complete(owned_key);
        if (!finished) return tl::make_unexpected(finished.error());
        completed = std::move(finished.value());
    }
    const size_t size_bytes = completed.Size();
    return CommitBlock(owned_key, *tiler, std::move(completed), size_bytes);
}

tl::expected<void, ErrorCode> DataManagerV2::WriteRevoke(
    std::string_view key, const UUID& write_operation_id) {
    if (key.empty() || IsZeroUUID(write_operation_id)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto taken = leases_->TakePendingWrite(key, write_operation_id);
    if (taken) {
        // The record (and with it the untouched allocation) dies here, outside
        // every lock.
        return {};
    }
    if (taken.error() == ErrorCode::OBJECT_NOT_FOUND) {
        // Idempotent, matching V1: revoking a claim that is already gone is
        // what a peer does after a failed forward transfer it never made.
        return {};
    }
    return tl::make_unexpected(taken.error());
}

// ---------------------------------------------------------------------------
// Forward protocol: PinKey / UnPinKey
// ---------------------------------------------------------------------------

tl::expected<PinKeyResponse, ErrorCode> DataManagerV2::PinKey(
    std::string_view key, std::optional<UUID> tier_id) {
    if (key.empty()) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    auto guard = lifecycle_->Acquire();
    if (!guard) return tl::make_unexpected(guard.error());

    auto registration = block_registry_.Match(key);
    if (!registration.has_value()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    // The medium check happens exactly once, before any lease exists, and it
    // applies to a first pin and a repeat pin alike. V1 checked only on the
    // repeat path, so a first pin of a slow-tier replica returned OK with an
    // unusable address and left a real lease behind.
    auto exact = FindReplica(*registration, tier_id);
    if (!exact.has_value()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    if (!exact->tiler->IsTeAddressable()) {
        LOG(ERROR) << "PinKey: the replica of key=" << key << " on tier "
                   << exact->tiler->Id()
                   << " is not TE-addressable; no lease is created";
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }

    auto desc = MakeRemoteBufferDesc(exact->block.GetTransferAddress());
    if (!desc) return tl::make_unexpected(desc.error());

    // A pin publishes the block's address to a peer that is about to read it,
    // so it is a read in every sense that matters here. Leaving it out made a
    // forward-read workload invisible to both the heat tracker and the tier's
    // ordering: a key served entirely through PinKey looked untouched and was
    // evicted ahead of keys nobody had asked for.
    frequency_tracker_->RecordAccess(registration->Id(), key);
    exact->tiler->NotifyAccess(*registration, exact->block);

    const auto deadline = clock_->Now() + leases_->LeaseDuration();
    // The lease takes the snapshot, which is the only thing keeping the
    // published address valid until UnPin or expiry.
    auto pinned = leases_->Pin(std::move(exact->block), deadline);
    if (!pinned) return tl::make_unexpected(pinned.error());

    PinKeyResponse response;
    response.remote_buffer = std::move(desc.value());
    response.read_operation_id = pinned->read_token;
    return response;
}

tl::expected<void, ErrorCode> DataManagerV2::UnPinKey(
    std::string_view key, const UUID& read_operation_id) {
    if (key.empty() || IsZeroUUID(read_operation_id)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return leases_->Unpin(key, read_operation_id);
}

// ---------------------------------------------------------------------------
// Remote IO
// ---------------------------------------------------------------------------

tl::expected<DataManagerV2::RemoteReadPlan, ErrorCode>
DataManagerV2::PrepareRemoteRead(std::string_view key) {
    auto guard = lifecycle_->Acquire();
    if (!guard) return tl::make_unexpected(guard.error());

    auto registration = block_registry_.Match(key);
    if (!registration.has_value()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto site = FindReplica(*registration, std::nullopt);
    if (!site.has_value()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    RemoteReadPlan plan;
    plan.guard = std::move(guard.value());
    plan.source = std::move(site->block);
    plan.size = plan.source.Size();

    frequency_tracker_->RecordAccess(registration->Id(), key);
    site->tiler->NotifyAccess(*registration, plan.source);

    if (auto address = plan.source.GetTransferAddress(); address.has_value()) {
        plan.base = reinterpret_cast<void*>(address->addr);
        return plan;
    }

    // The source has no address of its own -- a cold key that now lives only
    // on a slow tier. Stage it through a registered DRAM block instead of
    // failing: the requester asked for the object, not for a particular
    // medium. The staging block travels with the plan so it outlives the
    // transfer.
    auto candidates = CandidateTilers(AllocationSource::kOnboard);
    if (candidates.empty()) {
        LOG(ERROR) << "No addressable tier is available to stage key=" << key;
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    auto staging =
        AllocateWithPolicy(*candidates.front(), plan.size,
                           /*alignment=*/0, AllocationSource::kOnboard);
    if (!staging) return tl::make_unexpected(staging.error());

    auto copied = local_copy_engine_->Copy(plan.source, staging.value());
    if (!copied) return tl::make_unexpected(copied.error());

    auto address = staging->GetTransferAddress();
    if (!address.has_value()) {
        LOG(ERROR) << "Staging block for key=" << key << " has no address";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    plan.base = reinterpret_cast<void*>(address->addr);
    plan.staging = std::move(staging.value());
    return plan;
}

async_simple::coro::Lazy<tl::expected<void, ErrorCode>>
DataManagerV2::ReadRemoteDataAsync(
    std::string_view key, const std::vector<RemoteBufferDesc>& dest_buffers) {
    auto validated = TransferCoordinator::ValidateRemoteBuffers(dest_buffers);
    if (!validated) {
        co_return tl::make_unexpected(validated.error());
    }

    auto prepared = PrepareRemoteRead(key);
    if (!prepared) {
        co_return tl::make_unexpected(prepared.error());
    }
    // Held across the suspend so the source snapshot, any staging block and
    // the lifecycle guard all outlive the transfer.
    RemoteReadPlan plan = std::move(prepared.value());

    auto result = co_await AwaitExpectedFuture(
        transfer_coordinator_->TransferAsync(plan.base, plan.size, dest_buffers,
                                             Transport::TransferRequest::WRITE),
        GetCoroExecutor());
    co_return result;
}

async_simple::coro::Lazy<tl::expected<UUID, ErrorCode>>
DataManagerV2::WriteRemoteDataAsync(
    std::string_view key, const std::vector<RemoteBufferDesc>& src_buffers,
    std::optional<UUID> tier_id) {
    auto validated = TransferCoordinator::ValidateRemoteBuffers(src_buffers);
    if (!validated) {
        co_return tl::make_unexpected(validated.error());
    }

    size_t total = 0;
    for (const auto& buffer : src_buffers) total += buffer.size;
    if (total == 0) {
        co_return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // The target is always an addressable tier, so unlike V1 there is no
    // "receive into staging, then copy down" branch, and the tier this
    // returns is always the one that ends up holding the object. A full DRAM
    // reports NO_AVAILABLE_HANDLE rather than spilling to a slow tier.
    auto target = ResolveForwardWriteTiler(tier_id);
    if (!target) {
        co_return tl::make_unexpected(target.error());
    }
    const UUID target_id = (*target)->Id();

    // Reuses the forward protocol: PreWrite claims the key and allocates,
    // WriteCommit publishes, WriteRevoke rolls back. That keeps one
    // implementation of the claim ordering instead of two.
    auto reserved = PreWrite(key, total, target_id);
    if (!reserved) {
        co_return tl::make_unexpected(reserved.error());
    }
    const UUID write_token = reserved->write_operation_id;
    void* const base = reinterpret_cast<void*>(reserved->remote_buffer.addr);

    auto transferred = co_await AwaitExpectedFuture(
        transfer_coordinator_->TransferAsync(base, total, src_buffers,
                                             Transport::TransferRequest::READ),
        GetCoroExecutor());
    if (!transferred) {
        // Partial or failed transfer must not publish anything.
        (void)WriteRevoke(key, write_token);
        co_return tl::make_unexpected(transferred.error());
    }

    auto committed = WriteCommit(key, write_token);
    if (!committed) {
        co_return tl::make_unexpected(committed.error());
    }
    co_return target_id;
}

async_simple::Future<tl::expected<void, ErrorCode>>
DataManagerV2::TransferDataAsync(
    void* local_transfer_base, size_t total_size,
    const std::vector<RemoteBufferDesc>& peer_buffers,
    Transport::TransferRequest::OpCode opcode) {
    return transfer_coordinator_->TransferAsync(local_transfer_base, total_size,
                                                peer_buffers, opcode);
}

// ---------------------------------------------------------------------------
// Route rectification
// ---------------------------------------------------------------------------

void DataManagerV2::RectifyReadRoute(std::string_view key,
                                     std::optional<UUID> tier_id) {
    if (key.empty()) return;
    if (Exist(key, tier_id)) return;

    // Best effort by construction: the miss check and the callback are not
    // atomic with respect to a concurrent Put, so a false positive is possible
    // and accepted. Holding a metadata lock across a Master RPC costs more
    // (section 7.3).
    RectifyRouteCallback callback;
    {
        std::lock_guard<std::mutex> lock(rectify_mu_);
        callback = rectify_callback_;
    }
    if (callback) {
        NoteRectifyWitness(key);
        callback(key, tier_id);
    }
}

void DataManagerV2::NoteRectifyWitness(std::string_view key) const {
    // A single atomic store into a fixed slot: no allocation, no lock, and no
    // growth. Collisions only make the estimate noisier, never unbounded.
    const size_t slot = StringHash{}(key) % kRectifyWitnessSlots;
    rectify_witness_[slot].store(StringHash{}(key), std::memory_order_relaxed);
}

void DataManagerV2::NoteCommitForRectifyWitness(std::string_view key) const {
    const size_t fingerprint = StringHash{}(key);
    const size_t slot = fingerprint % kRectifyWitnessSlots;
    // compare_exchange, so one rectify is only ever blamed once.
    uint64_t expected = static_cast<uint64_t>(fingerprint);
    if (rectify_witness_[slot].compare_exchange_strong(
            expected, 0, std::memory_order_relaxed)) {
        rectify_false_positive_suspected_.fetch_add(1,
                                                    std::memory_order_relaxed);
    }
}

uint64_t DataManagerV2::HotKeyTruncationCount() const {
    return frequency_tracker_ ? frequency_tracker_->TruncatedSnapshotCount()
                              : 0;
}

DataManagerV2Metrics DataManagerV2::Metrics() const {
    DataManagerV2Metrics metrics;
    if (event_center_) {
        const EventCenterMetrics events = event_center_->Metrics();
        metrics.lifecycle_event_inline_applied =
            events.lifecycle_event_inline_applied;
    }
    if (migration_engine_) {
        metrics.movement_commands_dropped =
            migration_engine_->Stats().submissions_rejected;
    }
    if (evict_engine_) {
        metrics.reclaim_destroyed_sole_replica =
            evict_engine_->Stats().victims_sole_replica;
    }
    metrics.registration_retry_exhausted =
        registration_retry_exhausted_.load(std::memory_order_relaxed);
    metrics.stop_drain_timeout_hit =
        stop_drain_timeout_hit_.load(std::memory_order_relaxed);
    metrics.rectify_false_positive_suspected =
        rectify_false_positive_suspected_.load(std::memory_order_relaxed);
    return metrics;
}

void DataManagerV2::SetRectifyCallback(RectifyRouteCallback fn) {
    std::lock_guard<std::mutex> lock(rectify_mu_);
    rectify_callback_ = std::move(fn);
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

void DataManagerV2::Stop() {
    if (!lifecycle_->BeginStop()) {
        return;  // idempotent
    }

    // Bounded: a caller may hold a TaskHandle and never call Wait(), so an
    // unbounded wait here would hang shutdown forever. On timeout the gate
    // switches to Cancel and outstanding handles fail with SHUTTING_DOWN.
    if (!lifecycle_->WaitForNoInflight(config_.stop_drain_timeout)) {
        stop_drain_timeout_hit_.fetch_add(1, std::memory_order_relaxed);
        LOG(WARNING) << "DataManagerV2::Stop timed out after "
                     << config_.stop_drain_timeout.count()
                     << "ms with in-flight work; cancelling";
        lifecycle_->BeginCancel();
    }

    if (leases_) leases_->StopAndDrain();
    // Before the pools: an outstanding wait completes its promise here, so no
    // awaiter is left hanging by the shutdown.
    if (transfer_coordinator_) transfer_coordinator_->Stop();

    // The event center first, so no new command can be proposed; then the
    // migration engine, which is what makes RunOnce() return zero and lets the
    // workers exit. They are joined here, before the engines they call into
    // are torn down.
    if (event_center_) event_center_->Stop(EventCenterStopMode::kDrain);
    if (movement_tracker_) movement_tracker_->Stop();
    if (migration_engine_) migration_engine_->Stop();
    for (auto& worker : movement_workers_) {
        if (worker.joinable()) worker.join();
    }
    movement_workers_.clear();

    for (const auto& tiler : tilers_.by_priority) {
        tiler->Stop();
    }
    if (te_wait_pool_) te_wait_pool_->stop();
    if (coro_executor_pool_) coro_executor_pool_->stop();
    lifecycle_->MarkStopped();
}

void DataManagerV2::Destroy() {
    if (!lifecycle_->MarkDestroyed()) {
        return;  // idempotent: never notify or free twice
    }

    for (const auto& tiler : tilers_.by_priority) {
        // Detached here and released here; anything a reader still holds stays
        // alive through the shared pool state until that reader is done.
        auto detached = tiler->DrainAll();
        if (metadata_callbacks_.segment_sync) {
            Segment segment;
            const TierView view = tiler->GetView();
            segment.id = view.id;
            segment.name = view.GetName();
            auto result = metadata_callbacks_.segment_sync(segment, false);
            if (!result) {
                LOG(WARNING)
                    << "segment unmount callback failed for tier " << view.id;
            }
        }
        detached.clear();
    }
}

void DataManagerV2::DrainForTest() {
    if (leases_) leases_->ScanExpiredNow();
    if (!event_center_) return;

    // Apply every published fact, then wait for the commands it produced to
    // settle. Without the second half a test could compare state while a
    // migration was still running.
    for (int attempt = 0; attempt < 1000; ++attempt) {
        event_center_->DrainForTest();
        const EventCenterMetrics metrics = event_center_->Metrics();
        const size_t queued =
            migration_engine_ ? migration_engine_->QueuedCount() : 0;
        if (queued == 0 && metrics.event_queue_depth == 0) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    LOG(WARNING) << "DrainForTest gave up waiting for the movement queue";
}

// ---------------------------------------------------------------------------
// Not yet implemented in this phase
// ---------------------------------------------------------------------------

async_simple::Executor* DataManagerV2::GetCoroExecutor() const {
    return coro_executor_pool_ ? coro_executor_pool_->get_executor() : nullptr;
}

}  // namespace mooncake::v2
