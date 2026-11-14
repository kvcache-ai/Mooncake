// SSD prefetcher: best-effort SSD -> DRAM promotion triggered out-of-band by
// exist probes (ExistOptions.prefetch_to_memory). See
// docs/source/design/store/ssd-prefetch.md.
//
// Lifetime: RealClient owns the SsdPrefetcher and outlives the components it
// points at, but those components are shared_ptrs assigned during setup; to
// avoid any member-ordering assumptions, the prefetcher holds weak_ptrs
// injected via Init() and locks them per use. Async jobs capture shared_ptrs
// they locked at submission time, so a tearing-down client simply makes jobs
// no-op.
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <latch>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "prefetch_throttle.h"
#include "thread_pool.h"
#include "types.h"

namespace mooncake {

class Client;
class FileStorage;
class ClientRequester;
struct QueryResult;

class SsdPrefetcher {
   public:
    SsdPrefetcher() = default;
    ~SsdPrefetcher() = default;

    SsdPrefetcher(const SsdPrefetcher&) = delete;
    SsdPrefetcher& operator=(const SsdPrefetcher&) = delete;

    // Inject dependencies once setup has constructed them, and start the
    // bounded worker pool.
    void Init(std::weak_ptr<Client> client,
              std::weak_ptr<FileStorage> file_storage,
              std::weak_ptr<ClientRequester> client_requester,
              std::string local_rpc_addr);

    bool initialized() const { return initialized_.load(); }

    std::shared_ptr<PrefetchThrottle> throttle() const { return throttle_; }

    // Best-effort trigger from the exist path. Synchronous part is a throttle
    // dedup + a pool enqueue only: no RPC on the caller's thread. All keys
    // (local and remote holders alike) pass the dedup window first, so hot
    // probes cannot cause metadata-query or delegation-RPC storms.
    // ignore_cooldown is used only by the get-side demand kick: it bypasses
    // this client's throttle backoff (never a master-side gate), so a get
    // headed for SSD can retry a promotion that an earlier saturated window
    // dropped. The dedup TTL still bounds the rate either way.
    void TriggerPrefetch(const std::vector<std::string>& keys,
                         bool ignore_cooldown = false);

    // Holder side of a remote prefetch request (prefetch_offload_object RPC):
    // dedup, size the staging from authoritative local metadata, then run
    // register + promote for keys whose LOCAL_DISK replica this node holds.
    void RunLocalPrefetch(const std::vector<std::string>& keys,
                          const std::vector<int64_t>& sizes,
                          const std::string& tenant_id);

    // Stop accepting new work, then drain and join the pool. Must run before
    // client teardown releases resources the jobs reference: jobs capture
    // shared_ptrs locked at submission, so a job blocked on its metadata RPC
    // keeps Client alive past client_.reset(), and segment memory must not be
    // released while such a job is still running. Idempotent; after Shutdown,
    // TriggerPrefetch no-ops and SubmitJob drops.
    void Shutdown();

    // Test-only hook (prefetch_teardown_test): pool jobs count down
    // `arrived` as they start, then wait on `release`. Never set in
    // production.
    struct JobGateForTest {
        std::latch arrived{1};
        std::latch release{1};
    };
    void SetJobGateForTest(std::shared_ptr<JobGateForTest> gate) {
        job_gate_for_test_ = std::move(gate);
    }

   private:
    // Submit to the bounded pool; drop the job when the pool is unavailable
    // (shutting down). Never falls back to a detached thread: an unbounded
    // thread-per-probe storm is exactly what the pool exists to prevent.
    void SubmitJob(std::function<void()> job);

    std::shared_ptr<PrefetchThrottle> throttle_ =
        std::make_shared<PrefetchThrottle>();

    std::weak_ptr<Client> client_;
    std::weak_ptr<FileStorage> file_storage_;
    std::weak_ptr<ClientRequester> client_requester_;
    std::string local_rpc_addr_;

    // Serializes the pool check+enqueue against Shutdown's pool handoff.
    std::mutex submit_mutex_;
    std::shared_ptr<JobGateForTest> job_gate_for_test_;

    // Fixed-size worker pool; bounds concurrent SSD reads / DRAM allocations.
    static constexpr size_t kPrefetchThreadPoolSize = 4;
    std::unique_ptr<ThreadPool> prefetch_pool_;
    std::atomic<bool> initialized_{false};
};

}  // namespace mooncake
