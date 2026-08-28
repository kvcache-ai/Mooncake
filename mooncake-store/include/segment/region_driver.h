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
#include "placement/target.h"
#include "segment/region.h"

namespace mooncake {

struct RegionResource final {
    explicit RegionResource(std::unique_ptr<PlacementTarget> placement_target);

    std::unique_ptr<PlacementTarget> target;
    bool active{false};
};

class RegionDriver;

class PreparedRegionResource final {
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

   private:
    PreparedRegionResource(
        RegionDriver& driver, const UUID& id,
        std::unique_ptr<RegionResource> resource,
        std::vector<std::unique_ptr<AllocatedBuffer>> imported_buffers);

    struct State;
    std::unique_ptr<State> state_;

    friend class RegionDriver;
};

class RegionDriver {
   public:
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
    explicit RegionDriver(
        std::optional<BufferAllocatorType> allocator_type = std::nullopt)
        : allocator_type_(allocator_type) {}

    PreparedRegionResource Stage(
        const UUID& id, std::unique_ptr<RegionResource> resource,
        std::vector<std::unique_ptr<AllocatedBuffer>> imported_buffers = {});

   private:
    const std::optional<BufferAllocatorType> allocator_type_;
    void CommitPrepared(PreparedRegionResource& prepared) noexcept;

    std::map<UUID, std::unique_ptr<RegionResource>> resources_;

    friend class PreparedRegionResource;
};

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

tl::expected<std::vector<LiveAllocation>, ErrorCode> BuildRegionLiveAllocations(
    const RegionResourceSpec& spec,
    std::span<const AllocatedBuffer::Descriptor> descriptors);

}  // namespace mooncake
