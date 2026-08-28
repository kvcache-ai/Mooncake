#include "segment/region_driver.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "segment/region_initial_state.h"

namespace mooncake {
namespace {

constexpr size_t kRegionSize = 16U * 1024 * 1024;

RegionResourceSpec MakeSpec(uintptr_t base = 0x100000000ULL) {
    return {generate_uuid(), "memory", base, kRegionSize, "memory-endpoint"};
}

TEST(RegionDriverTest, PreparedResourceRollsBackUntilCommitted) {
    MemoryRegionDriver driver(BufferAllocatorType::OFFSET);
    const auto spec = MakeSpec();

    {
        auto prepared = driver.PrepareOpen(spec, {});
        ASSERT_TRUE(prepared.has_value());
        ASSERT_NE(prepared->resource(), nullptr);
        EXPECT_EQ(driver.GetResource(spec.id), nullptr);
    }
    EXPECT_EQ(driver.GetResource(spec.id), nullptr);

    auto prepared = driver.PrepareOpen(spec, {});
    ASSERT_TRUE(prepared.has_value());
    prepared->Commit();
    auto* resource = driver.GetResource(spec.id);
    ASSERT_NE(resource, nullptr);
    EXPECT_TRUE(resource->active);
    EXPECT_TRUE(driver.Deactivate(spec.id));
    EXPECT_FALSE(resource->active);
    EXPECT_TRUE(driver.Reactivate(spec.id));
    EXPECT_TRUE(driver.Erase(spec.id));
}

TEST(RegionDriverTest, ReplacementRollbackKeepsCommittedResource) {
    MemoryRegionDriver driver(BufferAllocatorType::OFFSET);
    const auto spec = MakeSpec();
    auto first = driver.PrepareOpen(spec, {});
    ASSERT_TRUE(first.has_value());
    first->Commit();
    auto* committed = driver.GetResource(spec.id);
    ASSERT_NE(committed, nullptr);

    {
        auto replacement = driver.PrepareOpen(spec, {});
        ASSERT_TRUE(replacement.has_value());
        EXPECT_NE(replacement->resource(), committed);
    }
    EXPECT_EQ(driver.GetResource(spec.id), committed);
    EXPECT_TRUE(driver.Erase(spec.id));
}

TEST(RegionDriverTest, InitialStateValidatesAliasesBoundsAndPreservesOrder) {
    const auto spec = MakeSpec(0x200000000ULL);
    std::vector<AllocatedBuffer::Descriptor> descriptors{
        {4096, spec.base + 8192, "tcp", spec.name},
        {4096, spec.base, "tcp", spec.transport_endpoint}};
    auto state = BuildRegionInitialState(spec, descriptors);
    ASSERT_TRUE(state.has_value());
    ASSERT_EQ(state->allocations.size(), 2U);
    EXPECT_EQ(state->allocations[0].offset_bytes, 8192U);
    EXPECT_EQ(state->allocations[1].offset_bytes, 0U);

    auto bad_endpoint = descriptors;
    bad_endpoint[0].transport_endpoint_ = "other";
    EXPECT_EQ(BuildRegionInitialState(spec, bad_endpoint).error(),
              ErrorCode::INVALID_PARAMS);
    auto out_of_bounds = descriptors;
    out_of_bounds[0].buffer_address_ = spec.base + spec.size - 1024;
    EXPECT_EQ(BuildRegionInitialState(spec, out_of_bounds).error(),
              ErrorCode::INVALID_PARAMS);

    auto overlapping = descriptors;
    overlapping[0].buffer_address_ = spec.base;
    EXPECT_TRUE(BuildRegionInitialState(spec, overlapping).has_value());
}

TEST(RegionDriverTest, OffsetImportPreservesInputOrder) {
    MemoryRegionDriver driver(BufferAllocatorType::OFFSET);
    const auto spec = MakeSpec(0x300000000ULL);
    RegionInitialState state{{{8192, 4096}, {0, 4096}}};
    auto prepared = driver.PrepareOpen(spec, state);
    ASSERT_TRUE(prepared.has_value());
    ASSERT_EQ(prepared->imported_buffers().size(), 2U);
    EXPECT_EQ(
        reinterpret_cast<uintptr_t>(prepared->imported_buffers()[0]->data()),
        spec.base + 8192);
    EXPECT_EQ(
        reinterpret_cast<uintptr_t>(prepared->imported_buffers()[1]->data()),
        spec.base);
}

TEST(RegionDriverTest, ConcreteAllocatorRejectsOverlappingImport) {
    MemoryRegionDriver driver(BufferAllocatorType::OFFSET);
    const auto spec = MakeSpec(0x400000000ULL);
    auto prepared = driver.PrepareOpen(spec, {{{0, 4096}, {0, 4096}}});
    EXPECT_FALSE(prepared.has_value());
    EXPECT_EQ(prepared.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(driver.GetResource(spec.id), nullptr);
}

TEST(RegionDriverTest, CxlRejectsLiveInitialState) {
    CxlRegionDriver driver("cxl-test", kRegionSize);
    RegionResourceSpec spec{generate_uuid(), "binding", 0, kRegionSize,
                            "transport"};
    auto prepared = driver.PrepareOpen(spec, {{{0, 4096}}});
    EXPECT_FALSE(prepared.has_value());
    EXPECT_EQ(prepared.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    EXPECT_EQ(driver.GetResource(spec.id), nullptr);
}

}  // namespace
}  // namespace mooncake
