#pragma once

#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "allocator.h"
#include "placement/candidate.h"
#include "segment/region.h"

namespace mooncake {

struct RegionResource final {
    explicit RegionResource(
        std::unique_ptr<AllocationCandidate> allocation_candidate);

    // Resource inspection is separate from the placement-only candidate API.
    const std::shared_ptr<BufferAllocatorBase>& allocator() const noexcept {
        return candidate->allocator_handle();
    }

    std::unique_ptr<AllocationCandidate> candidate;
    bool active{false};
};

class RegionDriver {
   public:
    class PreparedRegionResource;
    virtual ~RegionDriver() = default;

    std::optional<BufferAllocatorType> allocator_type() const noexcept {
        return allocator_type_;
    }

    virtual tl::expected<PreparedRegionResource, ErrorCode> PrepareOpen(
        const RegionResourceSpec& spec,
        const std::vector<LiveAllocation>& live_allocations) = 0;
    virtual tl::expected<PreparedRegionResource, ErrorCode> PrepareAdopt(
        const RegionResourceSpec& spec,
        std::shared_ptr<BufferAllocatorBase> allocator) = 0;

    RegionResource* GetResource(const UUID& id);
    const RegionResource* GetResource(const UUID& id) const;
    bool Deactivate(const UUID& id);
    bool Reactivate(const UUID& id);
    bool Erase(const UUID& id);

   protected:
    struct StageKey {
        explicit StageKey() = default;
    };

    explicit RegionDriver(
        std::optional<BufferAllocatorType> allocator_type = std::nullopt)
        : allocator_type_(allocator_type) {}

    PreparedRegionResource Stage(
        const UUID& id, std::unique_ptr<RegionResource> resource,
        std::vector<std::unique_ptr<AllocatedBuffer>> imported_buffers = {});

   private:
    using ResourceMap = std::map<UUID, std::unique_ptr<RegionResource>>;

    // The caller retains the replaced resource until its indexes are updated.
    std::unique_ptr<RegionResource> CommitPrepared(
        ResourceMap::node_type& resource) noexcept;

    const std::optional<BufferAllocatorType> allocator_type_;

    ResourceMap resources_;
};

class RegionDriver::PreparedRegionResource final {
   public:
    ~PreparedRegionResource();

    PreparedRegionResource(PreparedRegionResource&& other) noexcept;
    PreparedRegionResource& operator=(PreparedRegionResource&& other) noexcept;
    PreparedRegionResource(const PreparedRegionResource&) = delete;
    PreparedRegionResource& operator=(const PreparedRegionResource&) = delete;

    // The staged resource remains valid until Commit() or a move.
    RegionResource& resource() const noexcept;
    const std::vector<std::unique_ptr<AllocatedBuffer>>& imported_buffers()
        const noexcept;
    std::vector<std::unique_ptr<AllocatedBuffer>> TakeImportedBuffers();

    void Commit() noexcept;

    PreparedRegionResource(
        StageKey, RegionDriver& driver, const UUID& id,
        std::unique_ptr<RegionResource> resource,
        std::vector<std::unique_ptr<AllocatedBuffer>> imported_buffers);

   private:
    struct State;
    std::unique_ptr<State> state_;
};

using PreparedRegionResource = RegionDriver::PreparedRegionResource;

using RegionDriverRegistry =
    std::unordered_map<RegionKind, std::unique_ptr<RegionDriver>>;

struct CxlRegionDriverConfig {
    std::string path;
    size_t size{0};
};

struct RegionDriverConfig {
    BufferAllocatorType memory_allocator{BufferAllocatorType::CACHELIB};
    std::optional<CxlRegionDriverConfig> cxl;
};

tl::expected<RegionDriverRegistry, ErrorCode> CreateRegionDrivers(
    const RegionDriverConfig& config);

// Converts descriptors that have already been canonicalized to
// spec.transport_endpoint. Segment-name aliases must be resolved by the
// recovery layer that owns the segment/catalog context before calling this
// helper.
tl::expected<std::vector<LiveAllocation>, ErrorCode> BuildRegionLiveAllocations(
    const RegionResourceSpec& spec,
    std::span<const AllocatedBuffer::Descriptor> descriptors);

}  // namespace mooncake
