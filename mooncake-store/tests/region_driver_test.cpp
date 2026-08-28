#include "segment/region_driver.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

namespace mooncake {
namespace {

constexpr size_t kRegionSize = 16U * 1024 * 1024;

RegionResourceSpec MakeSpec(uintptr_t base = 0x100000000ULL) {
    return {generate_uuid(), "memory", base, kRegionSize, "memory-endpoint"};
}

std::unique_ptr<RegionDriver> CreateTestDriver(RegionKind kind) {
    RegionDriverConfig config;
    config.memory_allocator = BufferAllocatorType::OFFSET;
    if (kind == RegionKind::CXL) {
        config.cxl = CxlRegionDriverConfig{"cxl-test", kRegionSize};
    }
    auto drivers = CreateRegionDrivers(config);
    if (!drivers) {
        return nullptr;
    }
    auto driver = drivers->extract(kind);
    return driver.empty() ? nullptr : std::move(driver.mapped());
}

TEST(RegionDriverTest, PreparedResourceRollsBackUntilCommitted) {
    auto driver = CreateTestDriver(RegionKind::HOST_MEMORY);
    ASSERT_NE(driver, nullptr);
    const auto spec = MakeSpec();

    {
        auto prepared = driver->PrepareOpen(spec, {});
        ASSERT_TRUE(prepared.has_value());
        EXPECT_EQ(driver->GetResource(spec.id), nullptr);
    }
    EXPECT_EQ(driver->GetResource(spec.id), nullptr);

    auto prepared = driver->PrepareOpen(spec, {});
    ASSERT_TRUE(prepared.has_value());
    prepared->Commit();
    auto* resource = driver->GetResource(spec.id);
    ASSERT_NE(resource, nullptr);
    EXPECT_TRUE(resource->active);
}

TEST(RegionDriverTest, ReplacementRollbackKeepsCommittedResource) {
    auto driver = CreateTestDriver(RegionKind::HOST_MEMORY);
    ASSERT_NE(driver, nullptr);
    const auto spec = MakeSpec();
    auto first = driver->PrepareOpen(spec, {});
    ASSERT_TRUE(first.has_value());
    first->Commit();
    auto* committed = driver->GetResource(spec.id);
    ASSERT_NE(committed, nullptr);

    {
        auto replacement = driver->PrepareOpen(spec, {});
        ASSERT_TRUE(replacement.has_value());
        EXPECT_NE(&replacement->resource(), committed);
    }
    EXPECT_EQ(driver->GetResource(spec.id), committed);
}

TEST(RegionDriverTest, RestoreInputValidatesEndpointBoundsAndPreservesOrder) {
    const auto spec = MakeSpec(0x200000000ULL);
    std::vector<AllocatedBuffer::Descriptor> descriptors{
        {4096, spec.base + 8192, "tcp", spec.transport_endpoint},
        {4096, spec.base, "tcp", spec.transport_endpoint}};
    auto allocations = BuildRegionLiveAllocations(spec, descriptors);
    ASSERT_TRUE(allocations.has_value());
    ASSERT_EQ(allocations->size(), 2U);
    EXPECT_EQ((*allocations)[0].offset_from_base, 8192U);
    EXPECT_EQ((*allocations)[1].offset_from_base, 0U);

    auto bad_endpoint = descriptors;
    bad_endpoint[0].transport_endpoint_ = "other";
    EXPECT_EQ(BuildRegionLiveAllocations(spec, bad_endpoint).error(),
              ErrorCode::INVALID_PARAMS);
    auto out_of_bounds = descriptors;
    out_of_bounds[0].buffer_address_ = spec.base + spec.size - 1024;
    EXPECT_EQ(BuildRegionLiveAllocations(spec, out_of_bounds).error(),
              ErrorCode::INVALID_PARAMS);
}

TEST(RegionDriverTest, OffsetImportPreservesInputOrder) {
    auto driver = CreateTestDriver(RegionKind::HOST_MEMORY);
    ASSERT_NE(driver, nullptr);
    const auto spec = MakeSpec(0x300000000ULL);
    std::vector<LiveAllocation> allocations{{8192, 4096}, {0, 4096}};
    auto prepared = driver->PrepareOpen(spec, allocations);
    ASSERT_TRUE(prepared.has_value());
    ASSERT_EQ(prepared->imported_buffers().size(), 2U);
    EXPECT_EQ(
        reinterpret_cast<uintptr_t>(prepared->imported_buffers()[0]->data()),
        spec.base + 8192);
    EXPECT_EQ(
        reinterpret_cast<uintptr_t>(prepared->imported_buffers()[1]->data()),
        spec.base);
}

TEST(RegionDriverTest, FailedRestoreDoesNotPublishResource) {
    auto driver = CreateTestDriver(RegionKind::HOST_MEMORY);
    ASSERT_NE(driver, nullptr);
    const auto spec = MakeSpec(0x400000000ULL);
    auto prepared = driver->PrepareOpen(spec, {{{0, 4096}, {0, 4096}}});
    EXPECT_FALSE(prepared.has_value());
    EXPECT_EQ(prepared.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(driver->GetResource(spec.id), nullptr);
}

TEST(RegionDriverTest, CxlRejectsLiveRestoreInput) {
    auto driver = CreateTestDriver(RegionKind::CXL);
    ASSERT_NE(driver, nullptr);
    RegionResourceSpec spec{generate_uuid(), "binding", 0, kRegionSize,
                            "transport"};
    auto prepared = driver->PrepareOpen(spec, {{{0, 4096}}});
    EXPECT_FALSE(prepared.has_value());
    EXPECT_EQ(prepared.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    EXPECT_EQ(driver->GetResource(spec.id), nullptr);
}

TEST(RegionDriverTest, CxlTargetProducesCxlDescriptors) {
    auto driver = CreateTestDriver(RegionKind::CXL);
    ASSERT_NE(driver, nullptr);
    RegionResourceSpec spec{generate_uuid(), "binding", 0, kRegionSize,
                            "transport"};
    auto prepared = driver->PrepareOpen(spec, {});
    ASSERT_TRUE(prepared.has_value());

    auto buffer = prepared->resource().target->Allocate(4096);
    ASSERT_NE(buffer, nullptr);
    const auto descriptor = buffer->get_descriptor();
    EXPECT_EQ(descriptor.protocol_, "cxl");
    EXPECT_EQ(descriptor.transport_endpoint_, spec.name);
}

TEST(RegionDriverTest, InvalidCxlConfigIsReturnedExplicitly) {
    RegionDriverConfig config;
    config.cxl =
        CxlRegionDriverConfig{"cxl-test", facebook::cachelib::Slab::kSize + 1};

    auto drivers = CreateRegionDrivers(config);
    ASSERT_FALSE(drivers.has_value());
    EXPECT_EQ(drivers.error(), ErrorCode::INVALID_PARAMS);
}

}  // namespace
}  // namespace mooncake
