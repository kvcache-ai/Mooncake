#pragma once

#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <chrono>
#include <csignal>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "pyclient.h"
#include "client_service.h"
#include "client_buffer.hpp"
#include "mutex.h"
#include "utils.h"
#include "rpc_types.h"
#include "thread_pool.h"
#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/coro_io/coro_io.hpp>
#include <async_simple/coro/Lazy.h>

namespace mooncake {

class RealClient;

// Global resource tracker to handle cleanup on abnormal termination
class ResourceTracker {
   public:
    // Get the singleton instance
    static ResourceTracker &getInstance();

    // Register a DistributedObjectStore instance for cleanup
    void registerInstance(const std::shared_ptr<PyClient> &instance);

   private:
    ResourceTracker();
    ~ResourceTracker();

    // Prevent copying
    ResourceTracker(const ResourceTracker &) = delete;
    ResourceTracker &operator=(const ResourceTracker &) = delete;

    // Cleanup all registered resources
    void cleanupAllResources();

    // Signal handler function
    static void signalHandler(int signal);

    // Exit handler function
    static void exitHandler();

    Mutex mutex_;
    std::vector<std::weak_ptr<PyClient>> instances_ GUARDED_BY(mutex_);

    // Ensure cleanup runs at most once
    std::atomic<bool> cleaned_{false};

    // Dedicated signal handling thread
    void startSignalThread();
    std::once_flag signal_once_{};
    std::jthread signal_thread_{};  // joins on destruction
};

// Throttle state for best-effort SSD prefetch. Two mechanisms:
//   1. Memory-pressure cooldown: when promotion fails because DRAM is
//      saturated (NO_AVAILABLE_HANDLE), open a short cooldown window during
//      which prefetch is a no-op, so prefetch (which *adds* to DRAM) stops
//      competing with eviction/offload (which *frees* DRAM) on the holder.
//   2. Per-key in-flight dedup / rate-limit: suppress duplicate prefetch
//      triggers for the same key from concurrent exist/get probes within a
//      TTL window, cutting the RPC/thread storm that starves offload.
// Held via shared_ptr so detached prefetch threads can use it safely without
// capturing a raw RealClient* that may outlive the work.
class PrefetchThrottle {
   public:
    enum class State : uint8_t {
        kTriggered = 0,
        kInFlight = 1,
        kCompleted = 2,
        kFailed = 3,
        kAlreadyResident = 4,
    };

    struct Entry {
        int64_t trigger_ms{-1};
        int64_t completed_ms{-1};
        State state{State::kTriggered};
        // Set when RegisterPrefetchTask + PrefetchKeys path runs for this key.
        bool promote_attempted{false};
    };

    static int64_t NowMs() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    // Reconfigure from mooncake.json (values come in as seconds, stored as ms).
    // Non-positive values keep the current setting. A 0 cooldown disables the
    // memory-pressure backoff; a 0 dedup TTL disables per-key rate-limiting.
    void configure(int64_t cooldown_sec, int64_t dedup_ttl_sec) {
        if (cooldown_sec >= 0) {
            cooldown_ms_.store(cooldown_sec * 1000, std::memory_order_relaxed);
        }
        if (dedup_ttl_sec >= 0) {
            dedup_ttl_ms_.store(dedup_ttl_sec * 1000,
                                std::memory_order_relaxed);
        }
    }

    bool inCooldown() const {
        return NowMs() < cooldown_until_ms_.load(std::memory_order_relaxed);
    }

    // Open the memory-pressure backoff window. No-op if cooldown is disabled.
    void enterCooldown() {
        const int64_t cooldown_ms =
            cooldown_ms_.load(std::memory_order_relaxed);
        if (cooldown_ms <= 0) {
            return;
        }
        cooldown_until_ms_.store(NowMs() + cooldown_ms,
                                 std::memory_order_relaxed);
    }

    // Returns the subset of keys not seen within the dedup TTL, registering
    // them as triggered synchronously (before the async prefetch job runs).
    std::vector<std::string> reserve(const std::vector<std::string> &keys) {
        const int64_t ttl_ms = dedup_ttl_ms_.load(std::memory_order_relaxed);
        const int64_t now = NowMs();
        std::vector<std::string> out;
        out.reserve(keys.size());
        std::lock_guard<std::mutex> lock(mutex_);
        if (ttl_ms <= 0) {
            for (const auto &key : keys) {
                entries_[key] = Entry{.trigger_ms = now,
                                      .completed_ms = -1,
                                      .state = State::kTriggered};
                out.push_back(key);
            }
            return out;
        }
        for (auto it = entries_.begin(); it != entries_.end();) {
            const int64_t last_ms = it->second.completed_ms >= 0
                                        ? it->second.completed_ms
                                        : it->second.trigger_ms;
            if (now - last_ms > ttl_ms) {
                it = entries_.erase(it);
            } else {
                ++it;
            }
        }
        for (const auto &key : keys) {
            if (entries_.find(key) != entries_.end()) {
                continue;
            }
            entries_[key] = Entry{.trigger_ms = now,
                                  .completed_ms = -1,
                                  .state = State::kTriggered};
            out.push_back(key);
        }
        return out;
    }

    void markInFlight(const std::string &key) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(key);
        if (it == entries_.end()) {
            return;
        }
        it->second.state = State::kInFlight;
        it->second.promote_attempted = true;
    }

    void markCompleted(const std::string &key) {
        const int64_t now = NowMs();
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(key);
        if (it == entries_.end()) {
            entries_[key] = Entry{.trigger_ms = now,
                                  .completed_ms = now,
                                  .state = State::kCompleted};
            return;
        }
        it->second.state = State::kCompleted;
        it->second.completed_ms = now;
    }

    void markFailed(const std::string &key) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(key);
        if (it == entries_.end()) {
            return;
        }
        it->second.state = State::kFailed;
    }

    // Async prefetch decided the key is not SSD-only (e.g. MEMORY already
    // present). Clears in-flight semantics without treating as promotion done.
    void markAlreadyResident(const std::string &key) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(key);
        if (it == entries_.end()) {
            return;
        }
        it->second.state = State::kAlreadyResident;
        it->second.promote_attempted = false;
    }

    bool promoteAttempted(const std::string &key) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(key);
        if (it == entries_.end()) {
            return false;
        }
        return it->second.promote_attempted;
    }

    int64_t triggeredAt(const std::string &key) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(key);
        if (it == entries_.end()) {
            return -1;
        }
        return it->second.trigger_ms;
    }

    int64_t completedAt(const std::string &key) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(key);
        if (it == entries_.end() || it->second.state != State::kCompleted) {
            return -1;
        }
        return it->second.completed_ms;
    }

    State stateOf(const std::string &key) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(key);
        if (it == entries_.end()) {
            return State::kFailed;
        }
        return it->second.state;
    }

    // Poll until promotion completes or the budget expires. Returns true only
    // when the key reaches kCompleted before the deadline.
    bool waitForCompletion(const std::string &key, int64_t max_wait_ms,
                           int64_t poll_ms = 1) const {
        if (max_wait_ms <= 0) {
            return false;
        }
        const int64_t deadline = NowMs() + max_wait_ms;
        const int64_t step_ms = std::max<int64_t>(poll_ms, 1);
        while (NowMs() < deadline) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                auto it = entries_.find(key);
                if (it == entries_.end()) {
                    return false;
                }
                if (it->second.state == State::kCompleted) {
                    return true;
                }
                if (it->second.state == State::kFailed) {
                    return false;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(step_ms));
        }
        return stateOf(key) == State::kCompleted;
    }

   private:
    std::atomic<int64_t> cooldown_until_ms_{0};
    std::atomic<int64_t> cooldown_ms_{DEFAULT_SSD_PREFETCH_COOLDOWN_SEC * 1000};
    std::atomic<int64_t> dedup_ttl_ms_{DEFAULT_SSD_PREFETCH_DEDUP_TTL_SEC *
                                       1000};
    mutable std::mutex mutex_;
    std::unordered_map<std::string, Entry> entries_;
};

class RealClient : public PyClient {
   public:
    RealClient();
    ~RealClient();

    // Factory to create shared instances and auto-register to ResourceTracker
    static std::shared_ptr<RealClient> create();

    int setup_real(
        const std::string &local_hostname, const std::string &metadata_server,
        size_t global_segment_size = 1024 * 1024 * 16,
        size_t local_buffer_size = 1024 * 1024 * 16,
        const std::string &protocol = "tcp",
        const std::string &rdma_devices = "",
        const std::string &master_server_addr = "127.0.0.1:50051",
        const std::shared_ptr<TransferEngine> &transfer_engine = nullptr,
        const std::string &ipc_socket_path = "",
        bool enable_ssd_offload = false,
        const std::string &ssd_offload_path = "",
        const std::string &tenant_id = "default",
        int64_t ssd_prefetch_cooldown_sec = DEFAULT_SSD_PREFETCH_COOLDOWN_SEC,
        int64_t ssd_prefetch_dedup_ttl_sec =
            DEFAULT_SSD_PREFETCH_DEDUP_TTL_SEC);

    int setup_dummy(size_t mem_pool_size, size_t local_buffer_size,
                    const std::string &server_address,
                    const std::string &ipc_socket_path) {
        // Real client does not support dummy setup
        return -1;
    };

    int initAll(const std::string &protocol, const std::string &device_name,
                size_t mount_segment_size = 1024 * 1024 * 16);  // Default 16MB

    uint64_t alloc_from_mem_pool(size_t size) { return 0; };

    int put(const std::string &key, std::span<const char> value,
            const ReplicateConfig &config = ReplicateConfig{});

    int register_buffer(void *buffer, size_t size);

    int unregister_buffer(void *buffer);

    struct WritableBufferRegion {
        void *base{nullptr};
        size_t size{0};
        size_t offset{0};
    };

    std::optional<WritableBufferRegion> resolve_writable_buffer_region(
        void *buffer) const;

    /**
     * @brief Get object data directly into a pre-allocated buffer
     * @param key Key of the object to get
     * @param buffer Pointer to a writable Store buffer, either explicitly
     * registered with register_buffer() or inside the setup-time local buffer
     * @param size Size of the buffer
     * @return Number of bytes read on success, negative value on error
     * @note The buffer address must resolve to Store-managed registered memory
     * for zero-copy operations
     */
    int64_t get_into(const std::string &key, void *buffer, size_t size);

    std::vector<std::vector<std::vector<int64_t>>> get_into_ranges(
        const std::vector<void *> &buffers,
        const std::vector<std::vector<std::string>> &all_keys,
        const std::vector<std::vector<std::vector<size_t>>> &all_dst_offsets,
        const std::vector<std::vector<std::vector<size_t>>> &all_src_offsets,
        const std::vector<std::vector<std::vector<size_t>>> &all_sizes,
        const QueryResultCache *query_result_cache = nullptr) override;

    /**
     * @brief Batch query object placement/lease metadata for later read reuse
     * @param keys Vector of keys to query
     * @return Vector of query results in the same order as keys
     */
    std::vector<tl::expected<QueryResult, ErrorCode>> batch_query(
        const std::vector<std::string> &keys) override;

    std::vector<int64_t> batch_get_into(const std::vector<std::string> &keys,
                                        const std::vector<void *> &buffers,
                                        const std::vector<size_t> &sizes);

    /**
     * @brief Get object data directly into pre-allocated buffers for multiple
     * keys
     * @param keys Vector of keys of the objects to get
     * @param all_buffers Vector of vectors of pointers to the pre-allocated
     * buffers
     * @param all_sizes Vector of vectors of sizes of the buffers
     * @return Vector of integers, where each element is the number of bytes
     * read on success, or a negative value on error
     * @note The buffer addresses must resolve to Store-managed registered
     * memory, either explicit register_buffer() regions or the setup-time local
     * buffer
     */
    std::vector<int> batch_get_into_multi_buffers(
        const std::vector<std::string> &keys,
        const std::vector<std::vector<void *>> &all_buffers,
        const std::vector<std::vector<size_t>> &all_sizes,
        bool prefer_same_node);

    /**
     * @brief Put object data directly from a pre-allocated buffer
     * @param key Key of the object to put
     * @param buffer Pointer to Store-managed registered memory, either
     * explicitly registered with register_buffer() or inside the setup-time
     * local buffer
     * @param size Size of the data to put
     * @return 0 on success, negative value on error
     * @note The buffer address must resolve to Store-managed registered memory,
     * either an explicit register_buffer() region or the setup-time local
     * buffer
     */
    int put_from(const std::string &key, void *buffer, size_t size,
                 const ReplicateConfig &config = ReplicateConfig{});

    /**
     * @brief Put one object directly from a registered data buffer plus a
     * registered metadata buffer
     * @param key Key of the object to put
     * @param buffer Pointer to the registered data buffer
     * @param metadata_buffer Pointer to the registered metadata buffer
     * @param size Size of the data buffer in bytes
     * @param metadata_size Size of the metadata buffer in bytes
     * @param config Replication configuration
     * @return 0 on success, negative value on error
     */
    int put_from_with_metadata(
        const std::string &key, void *buffer, void *metadata_buffer,
        size_t size, size_t metadata_size,
        const ReplicateConfig &config = ReplicateConfig{});

    /**
     * @brief Put object data directly from pre-allocated buffers for multiple
     * keys (batch version)
     * @param keys Vector of keys of the objects to put
     * @param buffers Vector of pointers to the pre-allocated buffers
     * @param sizes Vector of sizes of the buffers
     * @param config Replication configuration
     * @return Vector of integers, where each element is 0 on success, or a
     * negative value on error
     * @note The buffer addresses must resolve to Store-managed registered
     * memory, either explicit register_buffer() regions or the setup-time local
     * buffer
     */

    std::vector<int> batch_put_from(
        const std::vector<std::string> &keys,
        const std::vector<void *> &buffers, const std::vector<size_t> &sizes,
        const ReplicateConfig &config = ReplicateConfig{});

    /**
     * @brief Put object data directly from multiple pre-allocated buffers for
     * multiple keys (batch version)
     * @param keys Vector of keys of the objects to put
     * @param all_buffers Vector of vectors of pointers to the multiple
     * pre-allocated buffers
     * @param all_sizes Vector of vectors of sizes of the multiple buffers
     * @param config Replication configuration
     * @return Vector of integers, where each element is 0 on success, or a
     * negative value on error
     * @note The buffer addresses must resolve to Store-managed registered
     * memory, either explicit register_buffer() regions or the setup-time local
     * buffer
     */
    std::vector<int> batch_put_from_multi_buffers(
        const std::vector<std::string> &keys,
        const std::vector<std::vector<void *>> &all_buffers,
        const std::vector<std::vector<size_t>> &all_sizes,
        const ReplicateConfig &config = ReplicateConfig{});

    int put_parts(const std::string &key,
                  std::vector<std::span<const char>> values,
                  const ReplicateConfig &config = ReplicateConfig{});

    int put_batch(const std::vector<std::string> &keys,
                  const std::vector<std::span<const char>> &values,
                  const ReplicateConfig &config = ReplicateConfig{});

    int upsert(const std::string &key, std::span<const char> value,
               const ReplicateConfig &config = ReplicateConfig{});

    int upsert_from(const std::string &key, void *buffer, size_t size,
                    const ReplicateConfig &config = ReplicateConfig{});

    std::vector<int> batch_upsert_from(
        const std::vector<std::string> &keys,
        const std::vector<void *> &buffers, const std::vector<size_t> &sizes,
        const ReplicateConfig &config = ReplicateConfig{});

    int upsert_parts(const std::string &key,
                     std::vector<std::span<const char>> values,
                     const ReplicateConfig &config = ReplicateConfig{});

    int upsert_batch(const std::vector<std::string> &keys,
                     const std::vector<std::span<const char>> &values,
                     const ReplicateConfig &config = ReplicateConfig{});

    [[nodiscard]] std::string get_hostname() const;

    /**
     * @brief Get a buffer containing the data for a key
     * @param key Key to get data for
     * @return std::shared_ptr<BufferHandle> Buffer containing the data, or
     * nullptr if error
     */
    std::shared_ptr<BufferHandle> get_buffer(const std::string &key);

    /**
     * @brief Get buffers containing the data for multiple keys (batch version)
     * @param keys Vector of keys to get data for
     * @return Vector of std::shared_ptr<BufferHandle> buffers containing the
     * data, or nullptr for each key if error
     */
    std::vector<std::shared_ptr<BufferHandle>> batch_get_buffer(
        const std::vector<std::string> &keys);

    int remove(const std::string &key, bool force = false);

    long removeByRegex(const std::string &str, bool force = false);

    long removeAll(bool force = false);

    std::vector<int> batchRemove(const std::vector<std::string> &keys,
                                 bool force = false);

    int tearDownAll();

    int health_check() override;

    /**
     * @brief Check if an object exists
     * @param key Key to check
     * @param options Optional exist behavior (e.g. prefetch_to_memory)
     * @return 1 if exists, 0 if not exists, -1 if error
     */
    int isExist(const std::string &key,
                const ExistOptions &options = ExistOptions{});

    /**
     * @brief Check if multiple objects exist
     * @param keys Vector of keys to check
     * @param options Optional exist behavior (e.g. prefetch_to_memory)
     * @return Vector of existence results: 1 if exists, 0 if not exists, -1 if
     * error
     */
    std::vector<int> batchIsExist(const std::vector<std::string> &keys,
                                  const ExistOptions &options = ExistOptions{});

    /**
     * @brief Get the size of an object
     * @param key Key of the object
     * @return Size of the object in bytes, or -1 if error or object doesn't
     * exist
     */
    int64_t getSize(const std::string &key);

    /**
     * @brief Create a copy task to replicate an object's data to target
     * segments
     * @param key Object key
     * @param targets Target segments
     * @return tl::expected<UUID, ErrorCode> Task ID on success, ErrorCode on
     * failure
     */
    tl::expected<UUID, ErrorCode> create_copy_task(
        const std::string &key, const std::vector<std::string> &targets);

    /**
     * @brief Create a move task to move an object's replica from source segment
     * to target segment
     * @param key Object key
     * @param source Source segment
     * @param target Target segment
     * @return tl::expected<UUID, ErrorCode> Task ID on success, ErrorCode on
     * failure
     */
    tl::expected<UUID, ErrorCode> create_move_task(const std::string &key,
                                                   const std::string &source,
                                                   const std::string &target);

    /**
     * @brief Query a task by task id
     * @param task_id Task ID to query
     * @return tl::expected<QueryTaskResponse, ErrorCode> Task basic info
     * on success, ErrorCode on failure
     */
    tl::expected<QueryTaskResponse, ErrorCode> query_task(const UUID &task_id);

    // Hot cache acquire: returns (offset, size) within the shm region.
    // Increments ref_count to prevent eviction.
    tl::expected<std::tuple<uint64_t, size_t>, ErrorCode> acquire_hot_cache(
        const std::string &key);

    // Hot cache release: decrements ref_count after dummy is done reading.
    tl::expected<void, ErrorCode> release_hot_cache(const std::string &key);

    // Batch hot cache acquire: returns vector of (offset, size).
    // SIZE_MAX offset indicates cache miss for that key.
    std::vector<tl::expected<std::tuple<uint64_t, size_t>, ErrorCode>>
    batch_acquire_hot_cache(const std::vector<std::string> &keys);

    // Batch hot cache release.
    tl::expected<void, ErrorCode> batch_release_hot_cache(
        const std::vector<std::string> &keys);

    // Allocator-backed buffer acquire: allocates + fills, keeps handle alive.
    // Returns (dummy_addr, size).
    tl::expected<std::tuple<uint64_t, size_t>, ErrorCode> acquire_buffer_dummy(
        const std::string &key, const UUID &client_id);

    // Allocator-backed buffer release: frees the handle held by acquire.
    tl::expected<void, ErrorCode> release_buffer_dummy(uint64_t dummy_addr,
                                                       const UUID &client_id);

    // Batch allocator-backed buffer acquire.
    // Returns vector of (dummy_addr, size) per key.
    std::vector<tl::expected<std::tuple<uint64_t, size_t>, ErrorCode>>
    batch_acquire_buffer_dummy(const std::vector<std::string> &keys,
                               const UUID &client_id);

    tl::expected<void, ErrorCode> put_dummy_helper(
        const std::string &key, std::span<const char> value,
        const ReplicateConfig &config, const UUID &client_id);

    tl::expected<void, ErrorCode> upsert_dummy_helper(
        const std::string &key, std::span<const char> value,
        const ReplicateConfig &config, const UUID &client_id);

    tl::expected<void, ErrorCode> upsert_parts_dummy_helper(
        const std::string &key, std::vector<std::span<const char>> values,
        const ReplicateConfig &config, const UUID &client_id);

    tl::expected<void, ErrorCode> upsert_from_dummy_helper(
        const std::string &key, uint64_t dummy_buffer, size_t size,
        const ReplicateConfig &config, const UUID &client_id);

    std::vector<tl::expected<void, ErrorCode>> batch_upsert_from_dummy_helper(
        const std::vector<std::string> &keys,
        const std::vector<uint64_t> &dummy_buffers,
        const std::vector<size_t> &sizes, const ReplicateConfig &config,
        const UUID &client_id);

    tl::expected<void, ErrorCode> upsert_batch_dummy_helper(
        const std::vector<std::string> &keys,
        const std::vector<std::span<const char>> &values,
        const ReplicateConfig &config, const UUID &client_id);

    tl::expected<void, ErrorCode> put_batch_dummy_helper(
        const std::vector<std::string> &keys,
        const std::vector<std::span<const char>> &values,
        const ReplicateConfig &config, const UUID &client_id);

    tl::expected<void, ErrorCode> put_parts_dummy_helper(
        const std::string &key, std::vector<std::span<const char>> values,
        const ReplicateConfig &config, const UUID &client_id);

    async_simple::coro::Lazy<std::vector<tl::expected<int64_t, ErrorCode>>>
    batch_get_into_dummy_helper(const std::vector<std::string> &keys,
                                const std::vector<uint64_t> &buffers,
                                const std::vector<size_t> &sizes,
                                int32_t device_id, const UUID &client_id);

    std::vector<tl::expected<void, ErrorCode>> batch_put_from_dummy_helper(
        const std::vector<std::string> &keys,
        const std::vector<uint64_t> &dummy_buffers,
        const std::vector<size_t> &sizes, const ReplicateConfig &config,
        int32_t device_id, const UUID &client_id);

    std::vector<tl::expected<void, ErrorCode>>
    batch_put_from_multi_buffers_dummy_helper(
        const std::vector<std::string> &keys,
        const std::vector<std::vector<uint64_t>> &dummy_all_buffers,
        const std::vector<std::vector<size_t>> &all_sizes,
        const ReplicateConfig &config, int32_t device_id,
        const UUID &client_id);

    std::vector<tl::expected<int64_t, ErrorCode>>
    batch_get_into_multi_buffers_dummy_helper(
        const std::vector<std::string> &keys,
        const std::vector<std::vector<uint64_t>> &dummy_all_buffers,
        const std::vector<std::vector<size_t>> &all_sizes,
        bool prefer_alloc_in_same_node, int32_t device_id,
        const UUID &client_id);

    tl::expected<int64_t, ErrorCode> get_into_range_shm_helper(
        const std::string &key, uint64_t buffer, size_t dst_offset,
        size_t src_offset, size_t size, const UUID &client_id);

    std::vector<std::vector<std::vector<tl::expected<int64_t, ErrorCode>>>>
    get_into_ranges_shm_helper(
        const std::vector<uint64_t> &dummy_buffers,
        const std::vector<std::vector<std::string>> &all_keys,
        const std::vector<std::vector<std::vector<size_t>>> &all_dst_offsets,
        const std::vector<std::vector<std::vector<size_t>>> &all_src_offsets,
        const std::vector<std::vector<std::vector<size_t>>> &all_sizes,
        const std::map<std::string, CachedQueryResultResponse>
            &cached_query_results,
        int32_t device_id, const UUID &client_id);

    // Share mem management for dummy client
    // Modified: map_shm_internal now takes fd instead of just name
    tl::expected<void, ErrorCode> map_shm_internal(int fd,
                                                   uint64_t shm_base_addr,
                                                   size_t shm_size,
                                                   bool is_local_buffer,
                                                   const UUID &client_id);
    tl::expected<void, ErrorCode> map_shm_internal_with_device(
        int fd, uint64_t shm_base_addr, size_t shm_size, bool is_local_buffer,
        int32_t physical_device_id, const UUID &client_id);

    tl::expected<void, ErrorCode> unmap_shm_internal(const UUID &client_id);

    tl::expected<void, ErrorCode> ascend_shm_internal(
        uint64_t dummy_base_addr, size_t vmm_size, bool is_local_buffer,
        const std::string &shareable_handle_bytes, int32_t device_id,
        const UUID &client_id);

    tl::expected<void, ErrorCode> ascend_ipc_shm_internal(
        uint64_t dummy_base_addr, size_t mem_size, bool is_local_buffer,
        const std::string &ipc_key_bytes, int32_t device_id,
        const UUID &client_id);

    tl::expected<void, ErrorCode> ascend_unmap_shm_internal(
        const UUID &client_id);

    tl::expected<bool, ErrorCode> is_shm_mapped_internal(
        uint64_t dummy_base_addr, const UUID &client_id);

    tl::expected<void, ErrorCode> unregister_shm_buffer_internal(
        uint64_t dummy_base_addr, const UUID &client_id);

    // Internal versions that return tl::expected
    tl::expected<void, ErrorCode> service_ready_internal() { return {}; }

    tl::expected<void, ErrorCode> setup_internal(
        const std::string &local_hostname, const std::string &metadata_server,
        size_t global_segment_size = 1024 * 1024 * 16,
        size_t local_buffer_size = 1024 * 1024 * 16,
        const std::string &protocol = "tcp",
        const std::string &rdma_devices = "",
        const std::string &master_server_addr = "127.0.0.1:50051",
        const std::shared_ptr<TransferEngine> &transfer_engine = nullptr,
        const std::string &ipc_socket_path = "", int local_rpc_port = 50052,
        bool enable_ssd_offload = false, bool start_offload_rpc_server = false,
        const std::string &ssd_offload_path = "",
        const std::string &tenant_id = "default");

    // Overload that accepts a configuration dictionary
    tl::expected<void, ErrorCode> setup_internal(const ConfigDict &config);

    tl::expected<void, ErrorCode> initAll_internal(
        const std::string &protocol, const std::string &device_name,
        size_t mount_segment_size = 1024 * 1024 * 16);

    tl::expected<void, ErrorCode> unregister_buffer_internal(void *buffer);

    tl::expected<void, ErrorCode> put_internal(
        const std::string &key, std::span<const char> value,
        const ReplicateConfig &config = ReplicateConfig{},
        const std::shared_ptr<ClientBufferAllocator> &client_buffer_allocator =
            nullptr);

    tl::expected<void, ErrorCode> register_buffer_internal(void *buffer,
                                                           size_t size);

    struct RangedReadMetadata {
        QueryResult query_result;
        Replica::Descriptor replica;
        uint64_t total_size;
    };

    tl::expected<RangedReadMetadata, ErrorCode>
    build_ranged_read_metadata_from_query_result(
        const std::string &key,
        tl::expected<QueryResult, ErrorCode> query_result);

    tl::expected<RangedReadMetadata, ErrorCode> resolve_ranged_read_metadata(
        const std::string &key);

    tl::expected<int64_t, ErrorCode> execute_ranged_read(
        const std::string &key, void *buffer, size_t dst_offset,
        size_t src_offset, size_t size, const RangedReadMetadata &metadata,
        bool size_is_buffer_capacity = false);

    tl::expected<int64_t, ErrorCode> get_into_range_internal(
        const std::string &key, void *buffer, size_t dst_offset,
        size_t src_offset, size_t size, bool size_is_buffer_capacity = false);

    std::vector<std::vector<std::vector<tl::expected<int64_t, ErrorCode>>>>
    get_into_ranges_internal(
        const std::vector<void *> &buffers,
        const std::vector<std::vector<std::string>> &all_keys,
        const std::vector<std::vector<std::vector<size_t>>> &all_dst_offsets,
        const std::vector<std::vector<std::vector<size_t>>> &all_src_offsets,
        const std::vector<std::vector<std::vector<size_t>>> &all_sizes,
        const std::vector<size_t> *buffer_capacities = nullptr,
        std::vector<std::vector<std::vector<tl::expected<int64_t, ErrorCode>>>>
            *prepared_results = nullptr,
        const std::vector<std::vector<std::vector<bool>>> *valid_fragments =
            nullptr,
        const QueryResultCache *query_result_cache = nullptr);

    std::vector<tl::expected<int64_t, ErrorCode>> batch_get_into_internal(
        const std::vector<std::string> &keys,
        const std::vector<void *> &buffers, const std::vector<size_t> &sizes);

    std::vector<tl::expected<int64_t, ErrorCode>>
    batch_get_into_multi_buffers_internal(
        const std::vector<std::string> &keys,
        const std::vector<std::vector<void *>> &all_buffers,
        const std::vector<std::vector<size_t>> &all_sizes,
        bool prefer_same_node);

    tl::expected<void, ErrorCode> put_from_internal(
        const std::string &key, void *buffer, size_t size,
        const ReplicateConfig &config = ReplicateConfig{});

    tl::expected<void, ErrorCode> upsert_internal(
        const std::string &key, std::span<const char> value,
        const ReplicateConfig &config = ReplicateConfig{},
        std::shared_ptr<ClientBufferAllocator> client_buffer_allocator =
            nullptr);

    tl::expected<void, ErrorCode> upsert_from_internal(
        const std::string &key, void *buffer, size_t size,
        const ReplicateConfig &config = ReplicateConfig{});

    std::vector<tl::expected<void, ErrorCode>> batch_upsert_from_internal(
        const std::vector<std::string> &keys,
        const std::vector<void *> &buffers, const std::vector<size_t> &sizes,
        const ReplicateConfig &config = ReplicateConfig{});

    tl::expected<void, ErrorCode> upsert_parts_internal(
        const std::string &key, std::vector<std::span<const char>> values,
        const ReplicateConfig &config = ReplicateConfig{},
        std::shared_ptr<ClientBufferAllocator> client_buffer_allocator =
            nullptr);

    tl::expected<void, ErrorCode> upsert_batch_internal(
        const std::vector<std::string> &keys,
        const std::vector<std::span<const char>> &values,
        const ReplicateConfig &config = ReplicateConfig{},
        std::shared_ptr<ClientBufferAllocator> client_buffer_allocator =
            nullptr);

    std::vector<tl::expected<void, ErrorCode>> batch_put_from_internal(
        const std::vector<std::string> &keys,
        const std::vector<void *> &buffers, const std::vector<size_t> &sizes,
        const ReplicateConfig &config = ReplicateConfig{});

    std::vector<tl::expected<void, ErrorCode>>
    batch_put_from_multi_buffers_internal(
        const std::vector<std::string> &keys,
        const std::vector<std::vector<void *>> &all_buffers,
        const std::vector<std::vector<size_t>> &all_sizes,
        const ReplicateConfig &config = ReplicateConfig{});

    tl::expected<void, ErrorCode> put_parts_internal(
        const std::string &key,
        const std::vector<std::span<const char>> &values,
        const ReplicateConfig &config = ReplicateConfig{},
        const std::shared_ptr<ClientBufferAllocator> &client_buffer_allocator =
            nullptr);

    tl::expected<void, ErrorCode> put_batch_internal(
        const std::vector<std::string> &keys,
        const std::vector<std::span<const char>> &values,
        const ReplicateConfig &config = ReplicateConfig{},
        const std::shared_ptr<ClientBufferAllocator> &client_buffer_allocator =
            nullptr);

    tl::expected<void, ErrorCode> remove_internal(const std::string &key,
                                                  bool force = false);

    tl::expected<long, ErrorCode> removeByRegex_internal(const std::string &str,
                                                         bool force = false);

    tl::expected<int64_t, ErrorCode> removeAll_internal(bool force = false);

    std::vector<tl::expected<void, ErrorCode>> batchRemove_internal(
        const std::vector<std::string> &keys, bool force = false);

    tl::expected<void, ErrorCode> tearDownAll_internal();

    tl::expected<bool, ErrorCode> isExist_internal(const std::string &key);

    std::vector<tl::expected<bool, ErrorCode>> batchIsExist_internal(
        const std::vector<std::string> &keys);

    tl::expected<int64_t, ErrorCode> getSize_internal(const std::string &key);

    std::shared_ptr<BufferHandle> get_buffer_internal(
        const std::string &key,
        const std::shared_ptr<ClientBufferAllocator> &client_buffer_allocator =
            nullptr);

    std::vector<std::shared_ptr<BufferHandle>> batch_get_buffer_internal(
        const std::vector<std::string> &keys,
        const std::shared_ptr<ClientBufferAllocator> &client_buffer_allocator =
            nullptr);

    std::map<std::string, std::vector<Replica::Descriptor>>
    batch_get_replica_desc(const std::vector<std::string> &keys);
    std::vector<CachedQueryResultResponse> batch_get_query_results(
        const std::vector<std::string> &keys);
    std::vector<Replica::Descriptor> get_replica_desc(const std::string &key);

    std::vector<std::string> batch_replica_clear(
        const std::vector<std::string> &keys,
        const std::string &segment_name = "") override;

    tl::expected<PingResponse, ErrorCode> ping(const UUID &client_id);

    async_simple::coro::Lazy<
        tl::expected<BatchGetOffloadObjectResponse, ErrorCode>>
    batch_get_offload_object(const std::vector<std::string> &keys,
                             const std::vector<int64_t> &sizes);

    /**
     * @brief Holder-side RPC handler for cross-node SSD prefetch.
     *
     * Invoked by a remote requester when the LOCAL_DISK replica of a key is
     * held by THIS node. Runs the same promotion as the local prefetch path:
     * RegisterPrefetchTask (with this node's own client_id, so Master's
     * holder_id == client_id check passes) + FileStorage::PrefetchKeys to
     * stage the SSD object into DRAM. Best-effort and fire-and-forget: the
     * actual SSD I/O runs on a detached thread and this returns immediately.
     * @param keys SSD-only keys held by this node to promote into DRAM.
     * @param sizes Object sizes (bytes) captured by the requester from
     * Master metadata; index-aligned with keys.
     * @return true if the prefetch work was accepted/scheduled.
     */
    bool prefetch_offload_object(const std::vector<std::string> &keys,
                                 const std::vector<int64_t> &sizes);

    /**
     * @brief Releases buffer associated with a specific batch_id.
     * Called by remote client after transfer completion.
     * @param batch_id The unique identifier of the batch to release
     * @return true if batch was found and released, false otherwise
     */
    bool release_offload_buffer(uint64_t batch_id);

    /**
     * @brief Retrieves multiple stored objects from a remote service.
     * @param target_rpc_service_addr Address of the remote RPC service (e.g.,
     "ip:port").

     */
    tl::expected<void, ErrorCode> batch_get_into_offload_object_internal(
        const std::string &target_rpc_service_addr,
        std::unordered_map<std::string, std::vector<Slice>> &objects);

    int64_t get_offload_rpc_read_count() const {
        return offload_rpc_read_count_.load(std::memory_order_relaxed);
    }

    /**
     * @brief Mount a shared memory file region and return segment ids.
     *        If size > max_mr_size, it will be split into multiple chunks
     *        and mounted separately. RealClient will open(path) + mmap
     *        internally for each chunk.
     */
    int mountSegment(const std::string &path, size_t offset, size_t size,
                     const std::string &protocol, const std::string &location,
                     std::vector<std::string> &out_segment_ids);

    /**
     * @brief Unmount segments by their ids and clean up local mmap/fd.
     * @param grace_period_seconds 0 = immediate unmount (legacy behavior).
     */
    int unmountSegment(const std::vector<std::string> &segment_ids,
                       uint64_t grace_period_seconds = 0);

    /**
     * @brief Allocate memory internally and mount segments to master.
     *        If size > max_mr_size, it will be split into multiple chunks.
     *        Memory is allocated via allocate_buffer_allocator_memory.
     *        The actual allocated size (aligned up to Slab::kSize) is written
     *        to out_allocated_size if non-null.
     */
    int allocateAndMountSegment(size_t size, const std::string &protocol,
                                const std::string &location,
                                std::vector<std::string> &out_segment_ids,
                                size_t *out_allocated_size = nullptr);

    /**
     * @brief Unmount segments by their ids and free locally allocated memory.
     * @param grace_period_seconds 0 = immediate unmount (legacy behavior).
     */
    int unmountAndFreeSegment(const std::vector<std::string> &segment_ids,
                              uint64_t grace_period_seconds = 0);

    struct MountedSegmentRecord {
        void *mmap_base = nullptr;
        size_t size = 0;
        std::string path;
    };

    struct AllocatedSegmentRecord {
        void *base = nullptr;
        size_t size = 0;
        std::string protocol;
    };

    std::unique_ptr<AutoPortBinder> port_binder_ = nullptr;

    struct SegmentDeleter {
        void operator()(void *ptr) {
            if (ptr) {
                free(ptr);
            }
        }
    };

    struct HugepageSegmentDeleter {
        size_t size = 0;
        void operator()(void *ptr) const {
            if (ptr && size > 0) {
                free_buffer_mmap_memory(ptr, size);
            }
        }
    };

    struct AscendSegmentDeleter {
        std::string protocol = "ascend";
        void operator()(void *ptr) {
            if (ptr) {
                free_memory(protocol.c_str(), ptr);
            }
        }
    };

    struct UbSegmentDeleter {
        size_t size = 0;
        std::string protocol = "ub";
        void operator()(void *ptr) const {
            if (ptr && size > 0) {
                free_memory(protocol.c_str(), ptr);
            }
        }
    };

    std::vector<std::unique_ptr<void, HugepageSegmentDeleter>>
        hugepage_segment_ptrs_;
    std::vector<std::unique_ptr<void, SegmentDeleter>> segment_ptrs_;
    std::vector<std::unique_ptr<void, AscendSegmentDeleter>>
        ascend_segment_ptrs_;
    std::vector<std::unique_ptr<void, UbSegmentDeleter>> ub_segment_ptrs_;
    std::string protocol;
    std::string device_name;
    std::string local_hostname;
    std::string local_rpc_addr;
    std::unique_ptr<coro_rpc::coro_rpc_server> offload_rpc_server_;
    int offload_rpc_port_ = 0;
    bool use_hugepage_ = false;

    struct MappedShm {
        std::string shm_name;
        // Offset = real_base - dummy_base
        uintptr_t shm_addr_offset = 0;
        void *shm_buffer = nullptr;
        size_t shm_size = 0;
        uintptr_t dummy_base_addr = 0;
        bool is_ascend = false;
        bool is_ipc = false;
        // Ascend physical device id from dummy (dummy-real RPC).
        int32_t device_id = kInvalidPhysicalDeviceId;
        uint64_t vmm_handle = 0;
        std::string ipc_key_data;
    };

    struct ShmContext {
        // List of all mapped shared memory for this client
        std::vector<MappedShm> mapped_shms;
        std::shared_ptr<ClientBufferAllocator> client_buffer_allocator =
            nullptr;
        // Buffers held by dummy via acquire_buffer; keyed by dummy address
        std::unordered_map<uint64_t, std::shared_ptr<BufferHandle>>
            active_handles;
    };
    mutable std::shared_mutex dummy_client_mutex_;
    std::unordered_map<UUID, ShmContext, boost::hash<UUID>> shm_contexts_;

    mutable std::shared_mutex registered_buffer_mutex_;
    std::unordered_map<void *, size_t> registered_buffer_sizes_;
    std::optional<WritableBufferRegion> local_buffer_region_;

    // Dummy VA -> real VA using mapped_shms; last_hit_shm caches locality.
    bool map_dummy_range_in_shm(const MappedShm &shm, uint64_t dummy_addr,
                                size_t offset, size_t size,
                                void *&out_real) const;

    bool map_dummy_buffer_to_real(const ShmContext &shm_ctx,
                                  uint64_t dummy_addr, size_t buf_size,
                                  const MappedShm *&last_hit_shm,
                                  void *&out_real) const;

    bool map_dummy_buffer_range_to_real(const ShmContext &shm_ctx,
                                        uint64_t dummy_addr, size_t dst_offset,
                                        size_t size, void *&out_real) const;

    tl::expected<std::vector<void *>, ErrorCode> map_dummy_addrs_to_real_ptrs(
        const ShmContext &context, const std::vector<uint64_t> &dummy_addrs,
        const std::vector<size_t> &sizes, const UUID &client_id) const;

    tl::expected<std::vector<std::vector<void *>>, ErrorCode>
    map_dummy_nested_addrs_to_real_ptrs(
        const ShmContext &context,
        const std::vector<std::vector<uint64_t>> &dummy_all_buffers,
        const std::vector<std::vector<size_t>> &all_sizes,
        const std::vector<std::string> &keys, const UUID &client_id) const;

    // Ensure cleanup executes at most once across multiple entry points
    std::atomic<bool> closed_{false};

    // Counts every LOCAL_DISK read served via peer offload-RPC.
    std::atomic<int64_t> offload_rpc_read_count_{0};

    // Dummy Client manage related members
    void dummy_client_monitor_func();
    int start_dummy_client_monitor();
    void stop_dummy_client_monitor();
    std::thread dummy_client_monitor_thread_;
    std::atomic<bool> dummy_client_monitor_running_{false};
    static constexpr uint64_t kDummyClientMonitorSleepMs =
        1000;  // 1000 ms sleep between client monitor checks
    // boost lockfree queue requires trivial assignment operator
    struct PodUUID {
        uint64_t first;
        uint64_t second;
    };
    static constexpr size_t kDummyClientPingQueueSize =
        128 * 1024;  // Size of the client ping queue
    boost::lockfree::queue<PodUUID> dummy_client_ping_queue_{
        kDummyClientPingQueueSize};
    const int64_t dummy_client_live_ttl_sec_ = DEFAULT_CLIENT_LIVE_TTL_SEC;
    int64_t view_version_ = 0;

    // IPC Server members for receiving FD from Dummy Clients
    std::string ipc_socket_path_;
    std::jthread ipc_thread_;
    std::atomic<bool> ipc_running_{false};
    int start_ipc_server();
    int stop_ipc_server();
    void ipc_server_func();
    // Embedded HTTP server for health-check / metrics
    std::unique_ptr<coro_http::coro_http_server> http_server_;
    int start_http_server();
    void stop_http_server();

    void handle_ipc_shm_register(int client_sock);
    void handle_ipc_shm_fd_request(int client_sock);

    void teardown_ascend_shm_buffer(MappedShm &shm);
    tl::expected<void, ErrorCode> setup_ascend_internal(
        size_t local_buffer_size);

    void triggerSsdPrefetch(const std::vector<std::string> &keys);

    // Read env-tunables and build prefetch_pool_. Called once from
    // setup_internal when SSD offload is enabled.
    void initPrefetchRuntime();

    // Submit a best-effort prefetch job to the bounded prefetch_pool_ so
    // high-frequency probes don't explode into unbounded detached threads.
    // Falls back to a detached std::thread only if the pool is unavailable
    // (e.g. enqueue after shutdown).
    void submitPrefetchJob(std::function<void()> job);

    // Bounded worker pool for SSD prefetch promotion jobs. Fixed size (see
    // initPrefetchRuntime), consistent with ClientService's task pool; bounds
    // concurrent SSD reads / DRAM allocations and avoids unbounded detached
    // threads.
    std::shared_ptr<ThreadPool> prefetch_pool_;

    // get()-side wait-for-prefetch. When a get selects a LOCAL_DISK (SSD)
    // replica but prefetch for the same key is in flight, poll every 1 ms
    // (early exit on completion) up to this budget. Env MOONCAKE_SSD_GET_WAIT_MS
    // overrides mooncake.json ssd_get_wait_ms. 0 disables waiting.
    int64_t ssd_get_wait_ms_{DEFAULT_SSD_GET_WAIT_MS};
    int64_t ssd_get_wait_ms_config_{DEFAULT_SSD_GET_WAIT_MS};

    // Best-effort SSD prefetch throttle (memory-pressure cooldown + per-key
    // in-flight dedup). Shared with detached prefetch threads via shared_ptr.
    // Tunable via mooncake.json (ssd_prefetch_cooldown_sec /
    // ssd_prefetch_dedup_window_sec); see setup_internal(ConfigDict).
    std::shared_ptr<PrefetchThrottle> prefetch_throttle_ =
        std::make_shared<PrefetchThrottle>();

    /**
     * @brief Run prefetch promotion for keys whose LOCAL_DISK replica is held
     * by THIS node. Registers a promotion task per key (this node is the
     * holder) and stages the objects from SSD into DRAM via prefetch_pool_.
     * Called from prefetch_offload_object (remote holder RPC). Best-effort;
     * runs asynchronously. Register + PrefetchKeys execution is delegated to
     * RunLocalPrefetchRegisterAndPromote (same as triggerSsdPrefetch local
     * branch).
     * @param keys Keys to promote (LOCAL_DISK replica held by this node).
     * @param sizes Object sizes (bytes), index-aligned with keys.
     */
    void runLocalPrefetch(const std::vector<std::string> &keys,
                          const std::vector<int64_t> &sizes);

   private:
    std::unordered_map<std::string, MountedSegmentRecord>
        mounted_segment_records_;
    std::mutex mounted_segment_records_mutex_;

    std::unordered_map<std::string, AllocatedSegmentRecord>
        allocated_segment_records_;
    std::mutex allocated_segment_records_mutex_;

    void ReleaseMountedSegmentRecord(const std::string &segment_id);
    void ReleaseAllMountedSegmentRecords();
    void ReleaseAllocatedSegmentRecord(const std::string &segment_id);
    void ReleaseAllAllocatedSegmentRecords();
};

}  // namespace mooncake
