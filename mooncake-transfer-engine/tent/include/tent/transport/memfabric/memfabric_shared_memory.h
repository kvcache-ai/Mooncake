// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#ifndef TENT_MEMFABRIC_SHARED_MEMORY_H
#define TENT_MEMFABRIC_SHARED_MEMORY_H
#include <array>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include "tent/common/status.h"
#include "tent/common/types.h"
#include "tent/thirdparty/nlohmann/json.h"
#include "tent/transport/memfabric/memfabric_under_api.h"
namespace mooncake {
namespace tent {
class MemFabricSharedMemory {
   public:
    MemFabricSharedMemory(MemFabricGvaConfig config, std::string segment_name);
    ~MemFabricSharedMemory();
    // Initialize the entity and reserve windows before allocation or mapping.
    Status initialize();
    // allocate, map, and prepare use the ACL device bound by the caller.
    Status allocate(void**, size_t, uint32_t memory_type, MemoryOptions&);
    Status free(void*, size_t);
    // Publish all complete allocations together; failure leaves publication
    // state and the caller's descriptors unchanged.
    Status publish(const std::vector<std::pair<void*, size_t>>& ranges,
                   std::vector<std::string>& descriptors);
    Status unpublish(void*, std::optional<size_t> size = std::nullopt);
    Status map(const std::string& peer_segment_name,
               const std::string& descriptor, uint64_t address, size_t size);
    Status prepare(const std::string& peer_segment_name,
                   const std::string& descriptor, uint64_t address,
                   size_t size);
    Status unmap(const std::string& peer_segment_name, uint64_t address);
    Status validateCopyTarget(const std::string& peer_segment_name,
                              uint64_t address,
                              const std::string& descriptor) const;
    Status beginCopy(uint64_t source, uint64_t destination, size_t size,
                     uint32_t& direction);
    // Each member has already been pinned by beginCopy; settles all those pins.
    Status runBatchCopy(const std::vector<MemFabricGvaCopy>&,
                        uint32_t direction);
    void releaseCopy(size_t count, bool completion_unknown = false);
    Status close();

   private:
    using Json = nlohmann::json;
    enum class InitState { Uninitialized, RuntimeInitialized, Ready };
    struct Slice {
        void* handle = nullptr;
        uint64_t length = 0;
        uint32_t memory_type = 0;
        bool published = false;
        Json descriptor;
    };
    struct RemoteSlice {
        Json descriptor;
        std::string publication;
        bool active = false, imported = false;
    };
    // Native imports grouped by rank, not a peer discovery/connection list.
    // segment name/generation identify the publication; slices retain
    // per-allocation import and active state that HYBM cannot query for TENT.
    struct ImportedRank {
        std::string segment_name, generation;
        Json entity_blob;
        std::map<uint64_t, RemoteSlice> slices;
        bool cleaned = false;
    };
    Status mapOrPrepare(const std::string&, const std::string&, uint64_t,
                        size_t, bool);
    Status validateConfig() const;
    template <typename Copy>
    Status runPinnedCopy(size_t count, Copy copy, const char* operation);
    // Resource helpers below require mutex_ to be held by the caller.
    // Device binding belongs to each native caller.
    void destroyEntity();
    Status bindDevice();
    Status validateDescriptor(const Json&,
                              const std::string& peer_segment_name) const;
    Status validateRetainedPeers(uint32_t, const Json&, const std::string&,
                                 const Json&) const;
    Status importBundle(uint32_t, const Json&, const std::string&, const Json&);
    RemoteSlice& recordSlice(ImportedRank&, const Json&);
    Status importPeer(ImportedRank&, const Json&);
    Status verifyPeerMapping(uint32_t, const RemoteSlice&);
    Status cleanupPeer(uint32_t, ImportedRank&);
    uint32_t memoryTypeForRange(uint64_t, size_t) const;
    Json makeIdentity() const;
    mutable std::mutex mutex_;
    MemFabricGvaConfig config_;
    std::string segment_name_, generation_;
    const bool available_;
    void* entity_ = nullptr;
    InitState init_state_ = InitState::Uninitialized;
    std::map<uint64_t, Slice> slices_;
    std::array<uint64_t, 2> window_bases_{{0, 0}};
    std::array<uint64_t, 2> consumed_{{0, 0}};
    std::vector<void*> rollback_;  // failed frees stay owned for retry
    // Retain failed imports for retry/cleanup. Native rank cleanup waits until
    // its last active allocation exits, including logically unmapped
    // companions.
    std::map<uint32_t, ImportedRank> imported_ranks_;
    Json entity_blob_;
    uint64_t slice_generation_ = 0;
    size_t pending_copies_ = 0;
    bool copy_failed_ = false;
};
}  // namespace tent
}  // namespace mooncake
#endif
