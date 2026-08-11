// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_BUFFERS_H_
#define TENT_TRANSPORT_UB_BUFFERS_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "tent/common/status.h"
#include "tent/runtime/segment.h"
#include "tent/transport/ub/context.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake::tent::ub {

struct UbBufferSegmentMetadata {
    Topology::NicID topology_id{-1};
    std::string device_name;
    std::string eid;
    int eid_index{-1};
    SegmentDescriptor descriptor;
};

struct UbBufferMetadata {
    static constexpr uint32_t kSchemaVersion = 1;

    uint32_t schema_version{kSchemaVersion};
    uint64_t generation{0};
    uint64_t base{0};
    uint64_t length{0};
    std::string location;
    Permission permission{kLocalReadWrite};
    std::vector<UbBufferSegmentMetadata> segments;
};

Status encodeBufferMetadata(const UbBufferMetadata& metadata,
                            std::string& encoded);
Status decodeBufferMetadata(std::string_view encoded,
                            UbBufferMetadata& metadata);

struct LocalSegmentRef {
    UbContextPtr context;
    LocalSegmentPtr segment;
    uint64_t generation{0};
    uint64_t buffer_base{0};
    uint64_t buffer_length{0};
};

struct ImportedSegmentRef {
    UbContextPtr context;
    RemoteSegmentPtr segment;
    uint64_t generation{0};
    uint64_t buffer_base{0};
    uint64_t buffer_length{0};
    Topology::NicID remote_topology_id{-1};
};

// Owns registrations and imports independently from Classic TE.  Every
// adapter handle is shared with completion tokens, so removal invalidates the
// metadata immediately while native destruction is deferred until in-flight
// work releases its last reference.
class UbBufferManager final {
   public:
    UbBufferManager(std::shared_ptr<UrmaAdapter> adapter,
                    std::vector<UbContextPtr> contexts);
    ~UbBufferManager();

    UbBufferManager(const UbBufferManager&) = delete;
    UbBufferManager& operator=(const UbBufferManager&) = delete;

    Status addBuffer(BufferDesc& desc, const MemoryOptions& options);
    Status addBuffers(std::vector<BufferDesc>& descs,
                      const MemoryOptions& options);
    Status removeBuffer(BufferDesc& desc);
    Status clear();

    Status findLocal(uint64_t address, size_t length,
                     Topology::NicID local_topology_id,
                     LocalSegmentRef& result) const;

    Status importRemote(SegmentID remote_segment_id,
                        Topology::NicID local_topology_id,
                        Topology::NicID remote_topology_id,
                        const BufferDesc& remote_buffer, Request::OpCode opcode,
                        uint64_t address, size_t length,
                        ImportedSegmentRef& result);

    [[nodiscard]] size_t localBufferCount() const;
    [[nodiscard]] size_t importedSegmentCount() const;

   private:
    struct AddressRange {
        uint64_t base{0};
        uint64_t length{0};

        bool operator<(const AddressRange& rhs) const {
            return base < rhs.base || (base == rhs.base && length < rhs.length);
        }
        [[nodiscard]] bool contains(uint64_t address, size_t size) const;
    };

    struct LocalRecord {
        MemoryOptions options;
        uint64_t generation{0};
        std::unordered_map<Topology::NicID, LocalSegmentPtr> segments;
    };

    struct ImportKey {
        Topology::NicID local_topology_id{-1};
        SegmentID remote_segment_id{LOCAL_SEGMENT_ID};
        Topology::NicID remote_topology_id{-1};
        uint64_t buffer_base{0};
        uint64_t generation{0};

        bool operator==(const ImportKey&) const = default;
    };

    struct ImportKeyHash {
        size_t operator()(const ImportKey& key) const noexcept;
    };

    Status addBufferInternal(BufferDesc& desc, const MemoryOptions& options);
    Status unregisterRecord(LocalRecord& record);
    void retainPendingRecord(LocalRecord& record);
    static uint32_t segmentAccess(Permission permission);
    static bool permissionAllows(Permission permission, Request::OpCode opcode);
    UbContextPtr findContext(Topology::NicID topology_id) const;

    std::shared_ptr<UrmaAdapter> adapter_;
    std::vector<UbContextPtr> contexts_;
    std::unordered_map<Topology::NicID, UbContextPtr> context_by_topology_id_;

    mutable std::shared_mutex local_mutex_;
    std::map<AddressRange, LocalRecord> local_buffers_;
    // Registrations created by an add/transaction rollback stay owned here
    // when the provider refuses the first unregister attempt. clear() retries
    // them before the manager can be destroyed.
    std::vector<LocalSegmentPtr> pending_local_segments_;

    mutable std::shared_mutex import_mutex_;
    std::unordered_map<ImportKey, RemoteSegmentPtr, ImportKeyHash> imports_;
    // Partial imports returned alongside a provider error have no usable cache
    // key, but still need stable ownership until unimport succeeds.
    std::vector<RemoteSegmentPtr> pending_remote_segments_;
    std::atomic<uint64_t> next_generation_{1};
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_BUFFERS_H_
