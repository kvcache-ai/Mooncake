#include "segment/region_driver.h"

#include <limits>
#include <utility>

#include "master_metric_manager.h"

namespace mooncake {
namespace {

bool IsValidSpec(const RegionResourceSpec& spec) {
    return spec.id != UUID{0, 0} && !spec.name.empty() && spec.base != 0 &&
           spec.size != 0 &&
           spec.base <= std::numeric_limits<uintptr_t>::max() - spec.size;
}

std::unique_ptr<RegionResource> MakeResource(
    const RegionResourceSpec& spec,
    std::shared_ptr<BufferAllocatorBase> allocator,
    AllocationTargetKind target_kind) {
    if (!allocator) {
        return nullptr;
    }
    return std::make_unique<RegionResource>(
        spec, std::move(allocator), target_kind,
        target_kind == AllocationTargetKind::CXL ? spec.name : std::string{});
}

}  // namespace

PreparedRegionResource::PreparedRegionResource(
    RegionDriver* driver, UUID id,
    std::vector<std::unique_ptr<AllocatedBuffer>> imported_buffers)
    : driver_(driver),
      id_(id),
      imported_buffers_(std::move(imported_buffers)) {}

PreparedRegionResource::~PreparedRegionResource() { Rollback(); }

PreparedRegionResource::PreparedRegionResource(
    PreparedRegionResource&& other) noexcept
    : driver_(std::exchange(other.driver_, nullptr)),
      id_(other.id_),
      imported_buffers_(std::move(other.imported_buffers_)),
      replaced_resource_(std::move(other.replaced_resource_)),
      committed_(std::exchange(other.committed_, true)) {}

PreparedRegionResource& PreparedRegionResource::operator=(
    PreparedRegionResource&& other) noexcept {
    if (this == &other) {
        return *this;
    }
    Rollback();
    driver_ = std::exchange(other.driver_, nullptr);
    id_ = other.id_;
    imported_buffers_ = std::move(other.imported_buffers_);
    replaced_resource_ = std::move(other.replaced_resource_);
    committed_ = std::exchange(other.committed_, true);
    return *this;
}

RegionResource* PreparedRegionResource::resource() const {
    if (!driver_) {
        return nullptr;
    }
    return committed_ ? driver_->GetResource(id_)
                      : driver_->GetPreparedResource(id_);
}

void PreparedRegionResource::Commit() noexcept {
    if (!driver_ || committed_) {
        return;
    }
    driver_->CommitPrepared(*this);
    committed_ = true;
}

void PreparedRegionResource::Rollback() noexcept {
    if (driver_ && !committed_) {
        driver_->RollbackPrepared(id_);
    }
    driver_ = nullptr;
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

tl::expected<PreparedRegionResource, ErrorCode> RegionDriver::Stage(
    std::unique_ptr<RegionResource> resource,
    std::vector<std::unique_ptr<AllocatedBuffer>> imported_buffers) {
    if (!resource || prepared_.contains(resource->spec.id)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const UUID id = resource->spec.id;
    prepared_.emplace(id, std::move(resource));
    return PreparedRegionResource(this, id, std::move(imported_buffers));
}

RegionResource* RegionDriver::GetPreparedResource(const UUID& id) {
    auto it = prepared_.find(id);
    return it == prepared_.end() ? nullptr : it->second.get();
}

void RegionDriver::CommitPrepared(PreparedRegionResource& prepared) noexcept {
    auto staged = prepared_.extract(prepared.id_);
    if (staged.empty()) {
        return;
    }
    auto existing = resources_.extract(prepared.id_);
    if (!existing.empty()) {
        prepared.replaced_resource_ = std::move(existing.mapped());
    }
    staged.mapped()->active = true;
    resources_.insert(std::move(staged));
}

void RegionDriver::RollbackPrepared(const UUID& id) noexcept {
    prepared_.erase(id);
}

tl::expected<PreparedRegionResource, ErrorCode> MemoryRegionDriver::PrepareOpen(
    const RegionResourceSpec& spec, const RegionInitialState& initial_state) {
    if (!IsValidSpec(spec)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    try {
        if (initial_state.allocations.empty()) {
            std::shared_ptr<BufferAllocatorBase> allocator;
            switch (*allocator_type()) {
                case BufferAllocatorType::CACHELIB:
                    allocator = std::make_shared<CachelibBufferAllocator>(
                        spec.name, spec.base, spec.size,
                        spec.transport_endpoint);
                    break;
                case BufferAllocatorType::OFFSET:
                    allocator = std::make_shared<OffsetBufferAllocator>(
                        spec.name, spec.base, spec.size,
                        spec.transport_endpoint);
                    break;
                default:
                    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            return Stage(MakeResource(spec, std::move(allocator),
                                      AllocationTargetKind::NATIVE));
        }

        if (*allocator_type() == BufferAllocatorType::CACHELIB) {
            auto restored = RestoreCachelibBufferAllocator(
                spec.name, spec.base, spec.size, spec.transport_endpoint,
                initial_state.allocations);
            if (!restored) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            auto resource = MakeResource(spec, std::move(restored->allocator),
                                         AllocationTargetKind::NATIVE);
            return Stage(std::move(resource), std::move(restored->buffers));
        }
        if (*allocator_type() == BufferAllocatorType::OFFSET) {
            auto restored = RestoreOffsetBufferAllocator(
                spec.name, spec.base, spec.size, spec.transport_endpoint,
                initial_state.allocations);
            if (!restored) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            auto resource = MakeResource(spec, std::move(restored->allocator),
                                         AllocationTargetKind::NATIVE);
            return Stage(std::move(resource), std::move(restored->buffers));
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "region=" << spec.name
                   << ", error=allocator_prepare_exception, what=" << e.what();
    } catch (...) {
        LOG(ERROR) << "region=" << spec.name
                   << ", error=allocator_prepare_unknown_exception";
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
    return Stage(
        MakeResource(spec, std::move(allocator), AllocationTargetKind::NATIVE));
}

CxlRegionDriver::CxlRegionDriver(std::string path, size_t size)
    : path_(std::move(path)), size_(size) {
    global_allocator_ = std::make_shared<CachelibBufferAllocator>(
        path_, DEFAULT_CXL_BASE, size_, path_);
    MasterMetricManager::instance().inc_total_mem_capacity(path_, size_);
}

CxlRegionDriver::~CxlRegionDriver() {
    MasterMetricManager::instance().dec_total_mem_capacity(path_, size_);
    MasterMetricManager::instance().remove_segment_metrics(path_);
}

tl::expected<PreparedRegionResource, ErrorCode> CxlRegionDriver::PrepareOpen(
    const RegionResourceSpec& spec, const RegionInitialState& initial_state) {
    if (!initial_state.allocations.empty()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    if (spec.id == UUID{0, 0} || spec.name.empty() || spec.size == 0 ||
        !global_allocator_) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return Stage(
        MakeResource(spec, global_allocator_, AllocationTargetKind::CXL));
}

tl::expected<PreparedRegionResource, ErrorCode> CxlRegionDriver::PrepareAdopt(
    const RegionResourceSpec& spec,
    std::shared_ptr<BufferAllocatorBase> allocator) {
    (void)spec;
    (void)allocator;
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
}

RegionDriverRegistry CreateRegionDrivers(const RegionDriverConfig& config) {
    RegionDriverRegistry drivers;
    drivers.emplace(
        RegionKind::HOST_MEMORY,
        std::make_unique<MemoryRegionDriver>(config.memory_allocator));
    if (config.enable_cxl) {
        drivers.emplace(RegionKind::CXL, std::make_unique<CxlRegionDriver>(
                                             config.cxl_path, config.cxl_size));
    }
    return drivers;
}

}  // namespace mooncake
