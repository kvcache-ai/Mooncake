#include "segment/region_driver.h"

#include <limits>
#include <map>
#include <utility>

namespace mooncake {
namespace {

using RegionResourceMap = std::map<UUID, std::unique_ptr<RegionResource>>;

bool IsValidSpec(const RegionResourceSpec& spec) {
    return spec.id != UUID{0, 0} && !spec.name.empty() && spec.base != 0 &&
           spec.size != 0 &&
           spec.base <= std::numeric_limits<uintptr_t>::max() - spec.size;
}

class NativePlacementTarget final : public PlacementTarget {
   public:
    explicit NativePlacementTarget(
        std::shared_ptr<BufferAllocatorBase> allocator)
        : PlacementTarget(std::move(allocator)) {}

    std::unique_ptr<AllocatedBuffer> Allocate(size_t size) const override {
        return allocator().allocate(size);
    }
};

class CxlPlacementTarget final : public PlacementTarget {
   public:
    CxlPlacementTarget(std::shared_ptr<BufferAllocatorBase> allocator,
                       std::string binding_name)
        : PlacementTarget(std::move(allocator)),
          cxl_binding_name_(std::move(binding_name)) {}

    std::unique_ptr<AllocatedBuffer> Allocate(size_t size) const override {
        auto buffer = allocator().allocate(size);
        if (buffer) {
            buffer->change_to_cxl(cxl_binding_name_);
        }
        return buffer;
    }

   private:
    std::string cxl_binding_name_;
};

std::unique_ptr<RegionResource> MakeNativeResource(
    std::shared_ptr<BufferAllocatorBase> allocator) {
    auto target = std::make_unique<NativePlacementTarget>(std::move(allocator));
    return std::make_unique<RegionResource>(std::move(target));
}

std::unique_ptr<RegionResource> MakeCxlResource(
    const RegionResourceSpec& spec,
    std::shared_ptr<BufferAllocatorBase> allocator) {
    auto target =
        std::make_unique<CxlPlacementTarget>(std::move(allocator), spec.name);
    return std::make_unique<RegionResource>(std::move(target));
}

class MemoryRegionDriver final : public RegionDriver {
   public:
    explicit MemoryRegionDriver(BufferAllocatorType allocator_type)
        : RegionDriver(allocator_type) {}

    tl::expected<PreparedRegionResource, ErrorCode> PrepareOpen(
        const RegionResourceSpec& spec,
        const std::vector<LiveAllocation>& live_allocations) override;
    tl::expected<PreparedRegionResource, ErrorCode> PrepareAdopt(
        const RegionResourceSpec& spec,
        std::shared_ptr<BufferAllocatorBase> allocator) override;
};

class CxlRegionDriver final : public RegionDriver {
   public:
    explicit CxlRegionDriver(
        std::shared_ptr<BufferAllocatorBase> global_allocator)
        : global_allocator_(std::move(global_allocator)) {}

    tl::expected<PreparedRegionResource, ErrorCode> PrepareOpen(
        const RegionResourceSpec& spec,
        const std::vector<LiveAllocation>& live_allocations) override;
    tl::expected<PreparedRegionResource, ErrorCode> PrepareAdopt(
        const RegionResourceSpec& spec,
        std::shared_ptr<BufferAllocatorBase> allocator) override;

   private:
    std::shared_ptr<BufferAllocatorBase> global_allocator_;
};

}  // namespace

RegionResource::RegionResource(
    std::unique_ptr<PlacementTarget> placement_target)
    : target(std::move(placement_target)) {}

struct PreparedRegionResource::State {
    State(RegionDriver& resource_driver, const UUID& id,
          std::unique_ptr<RegionResource> staged_resource,
          std::vector<std::unique_ptr<AllocatedBuffer>> buffers)
        : driver(resource_driver), imported_buffers(std::move(buffers)) {
        RegionResourceMap staged;
        auto inserted = staged.emplace(id, std::move(staged_resource));
        resource = staged.extract(inserted.first);
    }

    RegionDriver& driver;
    RegionResourceMap::node_type resource;
    std::vector<std::unique_ptr<AllocatedBuffer>> imported_buffers;
    std::unique_ptr<RegionResource> replaced_resource;
};

PreparedRegionResource::PreparedRegionResource(
    RegionDriver& driver, const UUID& id,
    std::unique_ptr<RegionResource> resource,
    std::vector<std::unique_ptr<AllocatedBuffer>> imported_buffers)
    : state_(std::make_unique<State>(driver, id, std::move(resource),
                                     std::move(imported_buffers))) {}

PreparedRegionResource::~PreparedRegionResource() = default;
PreparedRegionResource::PreparedRegionResource(
    PreparedRegionResource&& other) noexcept = default;
PreparedRegionResource& PreparedRegionResource::operator=(
    PreparedRegionResource&& other) noexcept = default;

RegionResource& PreparedRegionResource::resource() const noexcept {
    return *state_->resource.mapped();
}

const std::vector<std::unique_ptr<AllocatedBuffer>>&
PreparedRegionResource::imported_buffers() const noexcept {
    static const std::vector<std::unique_ptr<AllocatedBuffer>> empty;
    return state_ ? state_->imported_buffers : empty;
}

std::vector<std::unique_ptr<AllocatedBuffer>>
PreparedRegionResource::TakeImportedBuffers() {
    return state_ ? std::move(state_->imported_buffers)
                  : std::vector<std::unique_ptr<AllocatedBuffer>>{};
}

void PreparedRegionResource::Commit() noexcept {
    if (!state_ || state_->resource.empty()) {
        return;
    }
    state_->driver.CommitPrepared(*this);
}

RegionResource* RegionDriver::GetResource(const UUID& id) {
    auto it = resources_.find(id);
    return it == resources_.end() ? nullptr : it->second.get();
}

const RegionResource* RegionDriver::GetResource(const UUID& id) const {
    auto it = resources_.find(id);
    return it == resources_.end() ? nullptr : it->second.get();
}

bool RegionDriver::Deactivate(const UUID& id) {
    auto* resource = GetResource(id);
    if (!resource || !resource->active) {
        return false;
    }
    resource->active = false;
    return true;
}

bool RegionDriver::Reactivate(const UUID& id) {
    auto* resource = GetResource(id);
    if (!resource || resource->active) {
        return false;
    }
    resource->active = true;
    return true;
}

bool RegionDriver::Erase(const UUID& id) { return resources_.erase(id) != 0; }

PreparedRegionResource RegionDriver::Stage(
    const UUID& id, std::unique_ptr<RegionResource> resource,
    std::vector<std::unique_ptr<AllocatedBuffer>> imported_buffers) {
    return PreparedRegionResource(*this, id, std::move(resource),
                                  std::move(imported_buffers));
}

void RegionDriver::CommitPrepared(PreparedRegionResource& prepared) noexcept {
    auto existing = resources_.extract(prepared.state_->resource.key());
    if (!existing.empty()) {
        prepared.state_->replaced_resource = std::move(existing.mapped());
    }
    prepared.state_->resource.mapped()->active = true;
    resources_.insert(std::move(prepared.state_->resource));
}

namespace {

tl::expected<PreparedRegionResource, ErrorCode> MemoryRegionDriver::PrepareOpen(
    const RegionResourceSpec& spec,
    const std::vector<LiveAllocation>& live_allocations) {
    if (!IsValidSpec(spec)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (live_allocations.empty()) {
        auto allocator =
            CreateBufferAllocator(*allocator_type(), spec.name, spec.base,
                                  spec.size, spec.transport_endpoint);
        if (!allocator) {
            return tl::make_unexpected(allocator.error());
        }
        return Stage(spec.id, MakeNativeResource(std::move(*allocator)));
    }

    if (*allocator_type() == BufferAllocatorType::CACHELIB) {
        auto restored = ImportCachelibBufferAllocator(
            spec.name, spec.base, spec.size, spec.transport_endpoint,
            live_allocations);
        if (!restored) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        auto resource = MakeNativeResource(std::move(restored->allocator));
        return Stage(spec.id, std::move(resource),
                     std::move(restored->buffers));
    }
    if (*allocator_type() == BufferAllocatorType::OFFSET) {
        auto restored = ImportOffsetBufferAllocator(
            spec.name, spec.base, spec.size, spec.transport_endpoint,
            live_allocations);
        if (!restored) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        auto resource = MakeNativeResource(std::move(restored->allocator));
        return Stage(spec.id, std::move(resource),
                     std::move(restored->buffers));
    }
    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
}

tl::expected<PreparedRegionResource, ErrorCode>
MemoryRegionDriver::PrepareAdopt(
    const RegionResourceSpec& spec,
    std::shared_ptr<BufferAllocatorBase> allocator) {
    if (!IsValidSpec(spec) || !allocator ||
        allocator->getSegmentName() != spec.name ||
        allocator->getTransportEndpoint() != spec.transport_endpoint ||
        allocator->base() != spec.base || allocator->capacity() != spec.size) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return Stage(spec.id, MakeNativeResource(std::move(allocator)));
}

tl::expected<PreparedRegionResource, ErrorCode> CxlRegionDriver::PrepareOpen(
    const RegionResourceSpec& spec,
    const std::vector<LiveAllocation>& live_allocations) {
    if (!live_allocations.empty()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    if (spec.id == UUID{0, 0} || spec.name.empty() || spec.size == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return Stage(spec.id, MakeCxlResource(spec, global_allocator_));
}

tl::expected<PreparedRegionResource, ErrorCode> CxlRegionDriver::PrepareAdopt(
    const RegionResourceSpec& spec,
    std::shared_ptr<BufferAllocatorBase> allocator) {
    (void)spec;
    (void)allocator;
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

}  // namespace

tl::expected<RegionDriverRegistry, ErrorCode> CreateRegionDrivers(
    const RegionDriverConfig& config) {
    RegionDriverRegistry drivers;
    drivers.emplace(
        RegionKind::HOST_MEMORY,
        std::make_unique<MemoryRegionDriver>(config.memory_allocator));
    if (config.cxl) {
        auto allocator = CreateBufferAllocator(
            BufferAllocatorType::CACHELIB, config.cxl->path, DEFAULT_CXL_BASE,
            config.cxl->size, config.cxl->path);
        if (!allocator) {
            return tl::make_unexpected(allocator.error());
        }
        drivers.emplace(RegionKind::CXL, std::make_unique<CxlRegionDriver>(
                                             std::move(*allocator)));
    }
    return drivers;
}

tl::expected<std::vector<LiveAllocation>, ErrorCode> BuildRegionLiveAllocations(
    const RegionResourceSpec& spec,
    std::span<const AllocatedBuffer::Descriptor> descriptors) {
    if (spec.id == UUID{0, 0} || spec.name.empty() || spec.base == 0 ||
        spec.size == 0 ||
        spec.base > std::numeric_limits<uintptr_t>::max() - spec.size) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const uintptr_t end = spec.base + spec.size;
    std::vector<LiveAllocation> allocations;
    allocations.reserve(descriptors.size());
    for (const auto& descriptor : descriptors) {
        if (descriptor.transport_endpoint_ != spec.transport_endpoint ||
            descriptor.size_ == 0 || descriptor.buffer_address_ < spec.base ||
            descriptor.buffer_address_ >= end ||
            descriptor.size_ > end - descriptor.buffer_address_) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        allocations.push_back(
            {descriptor.buffer_address_ - spec.base, descriptor.size_});
    }
    return allocations;
}

}  // namespace mooncake
