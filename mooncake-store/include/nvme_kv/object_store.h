#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "nvme_kv/connector.h"
#include "nvme_kv/object_layout.h"
#include "thread_pool.h"
#include "types.h"

namespace mooncake {

struct NvmeKvObjectKey {
    std::string logical_key;
    std::optional<uint64_t> allocation_id;
};

struct NvmeKvReadRoot {
    NvmeKvObjectHeader header{};
    NvmeKvStoredIdentityView identity;
    std::string blob;
    std::vector<NvmeKvManifestChunkRecord> chunks;

    // Returns a view of the root payload: object data for inline objects,
    // or the serialized manifest for chunked objects.
    // Requires a root successfully parsed by DecodeNvmeKvReadRoot().
    std::string_view Payload() const {
        return std::string_view(blob).substr(
            sizeof(NvmeKvObjectHeader) + header.identity_metadata_size,
            header.payload_size);
    }
};

struct NvmeKvResolvedRoot {
    NvmeKvPhysicalKey physical_key;
    uint32_t slot;
    NvmeKvReadRoot root;
};

tl::expected<NvmeKvReadRoot, ErrorCode> DecodeNvmeKvReadRoot(std::string blob);

tl::expected<void, ErrorCode> ValidateNvmeKvReadRoot(
    const NvmeKvObjectIdentity& identity, const NvmeKvPhysicalKey& expected_key,
    uint32_t expected_slot, std::optional<uint64_t> expected_payload_size,
    NvmeKvReadRoot& root,
    std::optional<NvmeKvObjectType> expected_type = std::nullopt);

struct NvmeKvPutResult {
    bool stored = false;
    bool inserted = false;
};

struct NvmeKvWriteAttempt {
    std::string payload_storage;
    std::string_view payload;
    std::optional<NvmeKvWritePlan> write_plan;
    std::vector<NvmeKvPhysicalKey> written_chunk_keys;
    bool root_written = false;
    ErrorCode error = ErrorCode::OK;
};

/**
 * @brief Backend-specific addressing, root lookup, and write-failure handling.
 *
 * Batch workers may call the same instance concurrently. Implementations must
 * synchronize mutable state. Common codecs and the Connector remain shared.
 */
class NvmeKvObjectPolicy {
   public:
    virtual ~NvmeKvObjectPolicy() = default;

    /**
     * @brief Computes a physical address without submitting I/O.
     * @param key Logical identity and optional allocation ID.
     * @param slot Local collision slot; ignored by allocation-based addressing.
     * @param part_index Zero for the root, or chunk index plus one.
     * @note Callers must first validate backend-specific key requirements.
     */
    virtual NvmeKvPhysicalKey EncodePhysicalKey(const NvmeKvObjectKey& key,
                                                uint32_t slot,
                                                uint32_t part_index) const = 0;

    /**
     * @brief Selects the initial addresses and builds a plan without I/O.
     * @param payload Input bytes borrowed until the synchronous batch returns.
     * @param value_limit Maximum encoded value size in bytes.
     * @return A plan owning the encoded root and borrowing any chunk payloads.
     */
    virtual tl::expected<NvmeKvWritePlan, ErrorCode> PrepareWrite(
        const NvmeKvObjectKey& key, std::string_view payload,
        uint32_t value_limit) const = 0;

    /**
     * @brief Retrieves the root and validates its identity and contents.
     * @param expected_payload_size Expected logical byte count.
     * @note GetBatch supplies a length and validates root contents.
     * @note IsExist passes std::nullopt and only checks root identity.
     * @note Both paths require a decodable root and valid placement.
     * @return An owned root, or OBJECT_NOT_FOUND when it is absent.
     * @note Chunk addresses are checked before caching or chunk I/O.
     */
    virtual tl::expected<NvmeKvReadRoot, ErrorCode> ReadRoot(
        NvmeKvConnector& connector, const NvmeKvObjectKey& key,
        std::optional<uint64_t> expected_payload_size) const = 0;

    /**
     * @brief Handles a failed Store through confirmation, retry, or rollback.
     * @param payload Input bytes borrowed until the synchronous batch returns.
     * @param attempt Plan, error, and confirmed writes; updated in place.
     * @return Availability and insertion status; stored=false if slots run out.
     * @note attempt.payload_storage and payload remain valid during recovery.
     * @note Shared policies never retry uncertain Stores or reclaim keys.
     */
    virtual tl::expected<NvmeKvPutResult, ErrorCode> HandleWriteFailure(
        NvmeKvConnector& connector, const NvmeKvObjectKey& key,
        std::string_view payload, NvmeKvWriteAttempt& attempt) const = 0;

    /**
     * @brief Abandons an unfinished write without retrying Store.
     * @param attempt Partial state; a write plan need not exist.
     * @note Called after in-flight work stops; completed objects are excluded.
     * @note Reclaims only backend-owned writes. Cleanup is best-effort.
     */
    virtual void AbortWrite(NvmeKvConnector& connector,
                            NvmeKvWriteAttempt& attempt) const noexcept = 0;
};

class NvmeKvObjectStore final {
   public:
    using PutResult = NvmeKvPutResult;

    struct PutRequest {
        NvmeKvObjectKey key;
        std::span<const Slice> slices;

        tl::expected<PutResult, ErrorCode> result =
            tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    };

    struct GetRequest {
        NvmeKvObjectKey key;
        uint64_t expected_payload_size;
        Slice dest_slice;

        // Success: logical bytes written. Failure: this object's error.
        tl::expected<uint64_t, ErrorCode> result =
            tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    };

    NvmeKvObjectStore(std::unique_ptr<NvmeKvConnector> connector,
                      std::unique_ptr<NvmeKvObjectPolicy> policy);

    tl::expected<void, ErrorCode> PutBatch(std::span<PutRequest> requests);

    void GetBatch(std::span<GetRequest> requests);

    tl::expected<bool, ErrorCode> IsExist(const NvmeKvObjectKey& key);

   private:
    using PhysicalKey = NvmeKvPhysicalKey;

    struct CachedManifest {
        std::optional<uint64_t> allocation_id;
        uint32_t resolved_slot = 0;
        uint32_t payload_size = 0;
        std::vector<NvmeKvManifestChunkRecord> chunk_records;
        std::vector<size_t> chunk_offsets;
    };

    struct OffloadPlan {
        NvmeKvObjectKey key;
        std::span<const Slice> slices;
        size_t payload_size;
        size_t request_index;
    };

    struct ReadPlan {
        NvmeKvObjectKey key;
        Slice dest_slice;
        std::shared_ptr<const CachedManifest> cached_manifest;
        size_t request_index;
    };

    void StoreBatchParallel(
        std::vector<NvmeKvCommandExecutor::StoreRequest>& requests);

    void RunParallelIo(size_t item_count,
                       const std::function<void(size_t)>& task,
                       size_t max_inflight = 0);

    void RunPipelinedIo(size_t item_count,
                        const std::function<void(size_t)>& io_task,
                        const std::function<void(size_t)>& completion_task);

    std::shared_ptr<const CachedManifest> FindCachedManifest(
        const NvmeKvObjectKey& key, size_t payload_size) const;

    std::shared_ptr<const CachedManifest> CacheManifest(
        const NvmeKvObjectKey& key, size_t payload_size, uint32_t resolved_slot,
        const std::vector<NvmeKvManifestChunkRecord>& chunk_records);

    void CacheManifestAfterWrite(
        const NvmeKvObjectKey& key, size_t payload_size, uint32_t resolved_slot,
        const std::vector<NvmeKvManifestChunkRecord>& chunk_records) noexcept;

    void InitIoWorkers();

    std::unique_ptr<NvmeKvConnector> connector_;
    std::unique_ptr<NvmeKvObjectPolicy> policy_;

    mutable std::shared_mutex manifest_cache_mutex_;
    std::unordered_map<std::string, std::shared_ptr<const CachedManifest>>
        manifest_cache_;

    size_t io_parallelism_ = 1;
    size_t prepare_concurrency_ = 1;
    size_t batch_submit_concurrency_ = 1;
    size_t root_submit_concurrency_ = 1;

    std::unique_ptr<ThreadPool> io_workers_;

    std::unique_ptr<ThreadPool> submit_workers_;
    std::unique_ptr<ThreadPool> root_submit_workers_;
};

}  // namespace mooncake
