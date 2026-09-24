#include "ssd_prefetcher.h"

#include <algorithm>
#include <chrono>
#include <optional>
#include <thread>
#include <unordered_map>
#include <utility>

#include <glog/logging.h>

#include "client_buffer.h"
#include "client_service.h"
#include "file_storage.h"
#include "pyclient.h"
#include "replica.h"

namespace mooncake {

namespace {

// Metadata queries are batched in chunks so one exist probe over many keys
// costs ceil(n / 128) RPCs, and register+promote starts per chunk
// (pipelined) instead of after all metadata is back.
constexpr size_t kPrefetchMetadataChunkSize = 128;

struct SsdPrefetchRoute {
    int64_t local_disk_size{0};
    std::string holder_endpoint;
};

// A key is prefetchable when it has a COMPLETE LOCAL_DISK replica (data
// readable) and no MEMORY replica at all (a PROCESSING memory replica means
// someone is already promoting/writing it).
std::optional<SsdPrefetchRoute> ClassifySsdPrefetchRoute(
    const std::vector<Replica::Descriptor>& replicas) {
    bool has_memory = false;
    SsdPrefetchRoute route;
    bool has_local_disk = false;
    for (const auto& replica : replicas) {
        if (replica.is_memory_replica()) {
            has_memory = true;
            break;
        }
        if (replica.is_local_disk_replica() &&
            replica.status == ReplicaStatus::COMPLETE) {
            has_local_disk = true;
            route.local_disk_size =
                static_cast<int64_t>(calculate_total_size(replica));
            route.holder_endpoint =
                replica.get_local_disk_descriptor().transport_endpoint;
        }
    }
    if (has_memory || !has_local_disk || route.local_disk_size <= 0) {
        return std::nullopt;
    }
    return route;
}

// Register a promotion task per key on the master (this node is the holder),
// then run the shared promotion execution chain. PROMOTION_ALREADY_EXISTS is
// the normal "already resident / already in flight" outcome: skip quietly.
void RegisterAndPromote(Client& client, FileStorage& file_storage,
                        const std::shared_ptr<PrefetchThrottle>& throttle,
                        const std::vector<std::string>& keys,
                        const std::vector<int64_t>& sizes) {
    std::vector<std::string> promote_keys;
    std::vector<int64_t> promote_sizes;
    promote_keys.reserve(keys.size());
    promote_sizes.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        auto register_result = client.RegisterPrefetchTask(keys[i]);
        if (!register_result) {
            if (register_result.error() !=
                ErrorCode::PROMOTION_ALREADY_EXISTS) {
                VLOG(1) << "SSD prefetch: RegisterPrefetchTask failed for key="
                        << keys[i] << ", error=" << register_result.error();
                if (throttle) {
                    throttle->markFailed(keys[i]);
                }
            }
            continue;
        }
        promote_keys.push_back(keys[i]);
        promote_sizes.push_back(sizes[i]);
        if (throttle) {
            throttle->markInFlight(keys[i]);
        }
    }
    if (promote_keys.empty()) {
        return;
    }

    bool dram_pressure = false;
    auto on_key_done = [throttle](const std::string& key, bool success) {
        if (!throttle) {
            return;
        }
        if (success) {
            throttle->markCompleted(key);
        } else {
            throttle->markFailed(key);
        }
    };
    auto prefetch_res = file_storage.PrefetchKeys(promote_keys, promote_sizes,
                                                  &dram_pressure, on_key_done);
    if (!prefetch_res) {
        LOG(WARNING) << "SSD prefetch: PrefetchKeys failed, error="
                     << prefetch_res.error();
    } else {
        VLOG(1) << "PrefetchKeys completed keys=" << promote_keys.size();
    }
    if (dram_pressure && throttle) {
        // DRAM saturated: back off so eviction/offload can reclaim memory
        // instead of competing with promotion.
        throttle->enterCooldown();
        LOG(INFO) << "SSD prefetch: DRAM saturated, backing off "
                     "(ssd_prefetch_cooldown_sec)";
    }
}

}  // namespace

void SsdPrefetcher::Init(std::weak_ptr<Client> client,
                         std::weak_ptr<FileStorage> file_storage,
                         std::weak_ptr<ClientRequester> client_requester,
                         std::string local_rpc_addr, int64_t get_wait_ms) {
    client_ = std::move(client);
    file_storage_ = std::move(file_storage);
    client_requester_ = std::move(client_requester);
    local_rpc_addr_ = std::move(local_rpc_addr);
    get_wait_ms_ = get_wait_ms;
    prefetch_pool_ = std::make_unique<ThreadPool>(kPrefetchThreadPoolSize);
    initialized_.store(true);
    LOG(INFO) << "SSD prefetch runtime: pool_size=" << kPrefetchThreadPoolSize
              << ", ssd_get_wait_ms=" << get_wait_ms_;
}

void SsdPrefetcher::SubmitJob(std::function<void()> job) {
    if (!prefetch_pool_) {
        VLOG(1) << "SSD prefetch: pool unavailable, dropping job";
        return;
    }
    try {
        prefetch_pool_->enqueue(std::move(job));
    } catch (const std::exception& e) {
        // Pool stopped (shutdown in progress): drop the best-effort job.
        VLOG(1) << "SSD prefetch: pool enqueue failed (" << e.what()
                << "), dropping job";
    }
}

void SsdPrefetcher::TriggerPrefetch(const std::vector<std::string>& keys,
                                    bool ignore_cooldown) {
    if (!initialized_.load() || keys.empty()) {
        return;
    }
    auto throttle = throttle_;
    if (!ignore_cooldown && throttle->inCooldown()) {
        VLOG(1) << "SSD prefetch: skipped (memory-pressure cooldown)";
        return;
    }
    auto client = client_.lock();
    auto file_storage = file_storage_.lock();
    auto client_requester = client_requester_.lock();
    if (!client) {
        return;
    }

    // Coarse dedup at trigger time, for local AND remote-holder keys alike:
    // keys seen within the TTL never reach the pool or the network. The
    // exist path only knows "exists", not the replica tier, so keys that
    // turn out DRAM-resident still occupy a TTL slot; that is the accepted
    // price for keeping hot probes RPC-free. The async job re-classifies
    // precisely and marks those keys kAlreadyResident.
    auto reserved = throttle->reserve(keys);
    if (reserved.empty()) {
        return;
    }

    const std::string local_rpc_addr = local_rpc_addr_;
    SubmitJob([client = std::move(client),
               file_storage = std::move(file_storage),
               client_requester = std::move(client_requester), throttle,
               local_rpc_addr, keys = std::move(reserved)]() {
        std::unordered_map<std::string, std::vector<std::string>> remote_keys;
        std::unordered_map<std::string, std::vector<int64_t>> remote_sizes;

        for (size_t offset = 0; offset < keys.size();
             offset += kPrefetchMetadataChunkSize) {
            const size_t end =
                std::min(offset + kPrefetchMetadataChunkSize, keys.size());
            std::vector<std::string> chunk(keys.begin() + offset,
                                           keys.begin() + end);
            auto batch_results = client->BatchQueryReadOnly(chunk);
            if (batch_results.size() != chunk.size()) {
                LOG(WARNING)
                    << "SSD prefetch: BatchQueryReadOnly size "
                       "mismatch, expected "
                    << chunk.size() << ", got " << batch_results.size();
                continue;
            }

            std::vector<std::string> local_keys;
            std::vector<int64_t> local_sizes;
            for (size_t i = 0; i < chunk.size(); ++i) {
                if (!batch_results[i]) {
                    VLOG(1)
                        << "SSD prefetch: metadata query failed for key="
                        << chunk[i] << ", error=" << batch_results[i].error();
                    throttle->markFailed(chunk[i]);
                    continue;
                }
                auto route =
                    ClassifySsdPrefetchRoute(batch_results[i].value().replicas);
                if (!route) {
                    throttle->markAlreadyResident(chunk[i]);
                    continue;
                }
                if (route->holder_endpoint.empty() ||
                    route->holder_endpoint == local_rpc_addr) {
                    // Only the process that actually has the SSD object may
                    // RegisterPrefetchTask. EngineCore (segment=0) otherwise
                    // leaves a PROCESSING MEMORY replica that get() waits on
                    // for the full per-key budget.
                    std::optional<int64_t> local_size;
                    if (file_storage) {
                        local_size =
                            file_storage->LookupLocalObjectSize(chunk[i]);
                    }
                    if (local_size && *local_size > 0) {
                        local_keys.push_back(chunk[i]);
                        local_sizes.push_back(*local_size);
                    } else {
                        throttle->markFailed(chunk[i]);
                    }
                } else {
                    remote_keys[route->holder_endpoint].push_back(chunk[i]);
                    remote_sizes[route->holder_endpoint].push_back(
                        route->local_disk_size);
                    // The holder tracks in-flight state in its own process;
                    // mark delegated so the requester neither waits on the
                    // key nor re-delegates it before the dedup TTL expires.
                    throttle->markDelegated(chunk[i]);
                }
            }

            if (file_storage && !local_keys.empty()) {
                RegisterAndPromote(*client, *file_storage, throttle, local_keys,
                                   local_sizes);
            }
        }

        // Remote branch: delegate each holder's keys via RPC. The holder
        // registers the promotion task with its own client_id, so the
        // master's holder check passes. Best-effort: old peers reject the
        // RPC and the keys simply stay SSD-only.
        if (client_requester) {
            for (auto& [endpoint, group_keys] : remote_keys) {
                VLOG(1) << "SSD prefetch: delegating " << group_keys.size()
                        << " key(s) to remote holder " << endpoint;
                client_requester->prefetch_offload_object(
                    endpoint, group_keys, remote_sizes[endpoint]);
            }
        }
    });
}

void SsdPrefetcher::RunLocalPrefetch(const std::vector<std::string>& keys,
                                     const std::vector<int64_t>& sizes) {
    if (!initialized_.load() || keys.empty()) {
        return;
    }
    auto throttle = throttle_;
    if (throttle->inCooldown()) {
        VLOG(1) << "SSD prefetch: skipped (memory-pressure cooldown)";
        return;
    }
    auto client = client_.lock();
    auto file_storage = file_storage_.lock();
    if (!client || !file_storage) {
        return;
    }

    // sizes from the remote caller are a hint only; the local object map is
    // authoritative (LookupLocalObjectSize inside the job).
    (void)sizes;
    auto reserved = throttle->reserve(keys);
    if (reserved.empty()) {
        return;
    }
    SubmitJob([client = std::move(client),
               file_storage = std::move(file_storage), throttle,
               keys = std::move(reserved)]() {
        std::vector<std::string> local_keys;
        std::vector<int64_t> local_sizes;
        local_keys.reserve(keys.size());
        local_sizes.reserve(keys.size());
        for (const auto& key : keys) {
            auto local_size = file_storage->LookupLocalObjectSize(key);
            if (!local_size || *local_size <= 0) {
                VLOG(1) << "SSD prefetch: skip remote key=" << key
                        << " (not in local object map)";
                throttle->markFailed(key);
                continue;
            }
            local_keys.push_back(key);
            local_sizes.push_back(*local_size);
        }
        RegisterAndPromote(*client, *file_storage, throttle, local_keys,
                           local_sizes);
    });
}

std::optional<QueryResult> SsdPrefetcher::WaitIfPromotionInFlight(
    const std::string& key, int64_t budget_ms) {
    if (budget_ms <= 0 || !initialized_.load()) {
        return std::nullopt;
    }
    auto client = client_.lock();
    if (!client) {
        return std::nullopt;
    }
    auto throttle = throttle_;

    // Returns the refreshed QueryResult iff a COMPLETE MEMORY replica shows.
    auto requery_memory = [&]() -> std::optional<QueryResult> {
        auto qr = client->QueryReadOnly(key);
        if (!qr) {
            return std::nullopt;
        }
        const bool has_memory =
            std::any_of(qr->replicas.begin(), qr->replicas.end(),
                        [](const Replica::Descriptor& replica) {
                            return replica.is_memory_replica() &&
                                   replica.status == ReplicaStatus::COMPLETE;
                        });
        if (!has_memory) {
            return std::nullopt;
        }
        return std::optional<QueryResult>(*qr);
    };

    if (throttle->triggeredAt(key) >= 0) {
        // Only wait for a promotion that has actually started. kTriggered
        // means the pool job is still queued; waiting it burns the batch
        // budget and is what produced the 70s+ p99 in v2.
        const auto st = throttle->stateOf(key);
        if (st == PrefetchThrottle::State::kCompleted) {
            return requery_memory();
        }
        if (st == PrefetchThrottle::State::kInFlight) {
            throttle->waitForCompletion(key, budget_ms, /*poll_ms=*/1);
            return requery_memory();
        }
        return requery_memory();
    }

    // Another process may be promoting. One read-only re-query: only a
    // PROCESSING MEMORY replica is evidence of an in-flight promotion worth
    // waiting for; otherwise fall through to the SSD read at once.
    auto qr = client->QueryReadOnly(key);
    if (!qr) {
        return std::nullopt;
    }
    const bool promotion_in_flight =
        std::any_of(qr->replicas.begin(), qr->replicas.end(),
                    [](const Replica::Descriptor& replica) {
                        return replica.is_memory_replica() &&
                               replica.status == ReplicaStatus::PROCESSING;
                    });
    if (!promotion_in_flight) {
        return std::nullopt;
    }

    // Bounded poll: at most one re-query per 2ms slice, early exit as soon
    // as the promoted replica is readable.
    const int64_t deadline = PrefetchThrottle::NowMs() + budget_ms;
    while (PrefetchThrottle::NowMs() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        if (auto refreshed = requery_memory()) {
            return refreshed;
        }
    }
    return std::nullopt;
}

}  // namespace mooncake
