#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "placement/target.h"
#include "segment/region.h"

namespace mooncake {

struct RegionInitialState;

struct RegionResource final {
    RegionResource(std::shared_ptr<BufferAllocatorBase> resource_allocator,
                   AllocationTargetKind target_kind,
                   std::string cxl_binding_name = {})
        : allocator(std::move(resource_allocator)),
          target(allocator.get(), target_kind, std::move(cxl_binding_name)) {}

    std::shared_ptr<BufferAllocatorBase> allocator;
    AllocationTarget target;
    bool active{false};
};

class RegionDriver;
using RegionResourceMap = std::map<UUID, std::unique_ptr<RegionResource>>;

class PreparedRegionResource final {
   public:
    ~PreparedRegionResource() = default;

    PreparedRegionResource(PreparedRegionResource&& other) noexcept = default;
    PreparedRegionResource& operator=(PreparedRegionResource&& other) noexcept =
        default;
    PreparedRegionResource(const PreparedRegionResource&) = delete;
    PreparedRegionResource& operator=(const PreparedRegionResource&) = delete;

    RegionResource* resource() const;
    const std::vector<std::unique_ptr<AllocatedBuffer>>& imported_buffers()
        const noexcept {
        return imported_buffers_;
    }
    std::vector<std::unique_ptr<AllocatedBuffer>> TakeImportedBuffers() {
        return std::move(imported_buffers_);
    }

    void Commit() noexcept;

   private:
    PreparedRegionResource(
        RegionDriver* driver, RegionResourceMap::node_type resource,
        std::vector<std::unique_ptr<AllocatedBuffer>> imported_buffers);

    RegionDriver* driver_{nullptr};
    RegionResourceMap::node_type resource_;
    std::vector<std::unique_ptr<AllocatedBuffer>> imported_buffers_;
    std::unique_ptr<RegionResource> replaced_resource_;

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
        const RegionInitialState& initial_state) = 0;
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

    tl::expected<PreparedRegionResource, ErrorCode> Stage(
        const UUID& id, std::unique_ptr<RegionResource> resource,
        std::vector<std::unique_ptr<AllocatedBuffer>> imported_buffers = {});

   private:
    const std::optional<BufferAllocatorType> allocator_type_;
    void CommitPrepared(PreparedRegionResource& prepared) noexcept;

    RegionResourceMap resources_;

    friend class PreparedRegionResource;
};

class MemoryRegionDriver final : public RegionDriver {
   public:
    explicit MemoryRegionDriver(BufferAllocatorType allocator_type)
        : RegionDriver(allocator_type) {}

    tl::expected<PreparedRegionResource, ErrorCode> PrepareOpen(
        const RegionResourceSpec& spec,
        const RegionInitialState& initial_state) override;
    tl::expected<PreparedRegionResource, ErrorCode> PrepareAdopt(
        const RegionResourceSpec& spec,
        std::shared_ptr<BufferAllocatorBase> allocator) override;
};

class CxlRegionDriver final : public RegionDriver {
   public:
    CxlRegionDriver(std::string path, size_t size);
    ~CxlRegionDriver() override;

    tl::expected<PreparedRegionResource, ErrorCode> PrepareOpen(
        const RegionResourceSpec& spec,
        const RegionInitialState& initial_state) override;
    tl::expected<PreparedRegionResource, ErrorCode> PrepareAdopt(
        const RegionResourceSpec& spec,
        std::shared_ptr<BufferAllocatorBase> allocator) override;

   private:
    std::shared_ptr<BufferAllocatorBase> global_allocator_;
};

using RegionDriverRegistry =
    std::unordered_map<RegionKind, std::unique_ptr<RegionDriver>>;

struct RegionDriverConfig {
    BufferAllocatorType memory_allocator{BufferAllocatorType::CACHELIB};
    bool enable_cxl{false};
    std::string cxl_path;
    size_t cxl_size{0};
};

RegionDriverRegistry CreateRegionDrivers(const RegionDriverConfig& config);

}  // namespace mooncake
