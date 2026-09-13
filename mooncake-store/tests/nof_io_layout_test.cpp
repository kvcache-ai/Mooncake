#include "nof_io_layout.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>

using namespace mooncake;

namespace {

constexpr uint32_t kBlock512 = 512;
constexpr uint32_t kBlock4096 = 4096;
// Any address that is a multiple of the largest block size under test.
constexpr uintptr_t kAlignedAddress = 0x200000;

}  // namespace

TEST(NoFIoLayoutTest, AlignedRequestStaysDirect) {
    for (uint32_t block_size : {kBlock512, kBlock4096}) {
        NoFIoLayout layout;
        ASSERT_TRUE(BuildNoFIoLayout(block_size, block_size * 4,
                                     kAlignedAddress, block_size * 3, layout))
            << "block_size=" << block_size;
        ASSERT_EQ(layout.segments.size(), 1u);
        EXPECT_TRUE(layout.is_direct());
        EXPECT_EQ(layout.staging_bytes(block_size), 0u);

        const auto& segment = layout.segments[0];
        EXPECT_EQ(segment.kind, NoFIoSegmentKind::kDirect);
        EXPECT_EQ(segment.lba, 4u);
        EXPECT_EQ(segment.lba_count, 3u);
        EXPECT_EQ(segment.buffer_offset, 0u);
        EXPECT_EQ(segment.payload_size, block_size * 3u);
    }
}

TEST(NoFIoLayoutTest, PartialTrailingBlockIsStagedAlone) {
    NoFIoLayout layout;
    ASSERT_TRUE(BuildNoFIoLayout(kBlock512, 0, kAlignedAddress, 513, layout));
    ASSERT_EQ(layout.segments.size(), 2u);
    EXPECT_FALSE(layout.is_direct());
    // The 513 B object occupies 1024 B of disk, and only the last byte is
    // copied through a bounce buffer.
    EXPECT_EQ(layout.staging_bytes(kBlock512), 512u);

    const auto& head = layout.segments[0];
    EXPECT_EQ(head.kind, NoFIoSegmentKind::kDirect);
    EXPECT_EQ(head.lba, 0u);
    EXPECT_EQ(head.lba_count, 1u);
    EXPECT_EQ(head.buffer_offset, 0u);
    EXPECT_EQ(head.payload_size, 512u);

    const auto& tail = layout.segments[1];
    EXPECT_EQ(tail.kind, NoFIoSegmentKind::kStaged);
    EXPECT_EQ(tail.lba, 1u);
    EXPECT_EQ(tail.lba_count, 1u);
    EXPECT_EQ(tail.buffer_offset, 512u);
    EXPECT_EQ(tail.payload_size, 1u);
}

TEST(NoFIoLayoutTest, ObjectSmallerThanOneBlockIsFullyStaged) {
    NoFIoLayout layout;
    ASSERT_TRUE(
        BuildNoFIoLayout(kBlock4096, kBlock4096, kAlignedAddress, 100, layout));
    ASSERT_EQ(layout.segments.size(), 1u);

    const auto& segment = layout.segments[0];
    EXPECT_EQ(segment.kind, NoFIoSegmentKind::kStaged);
    EXPECT_EQ(segment.lba, 1u);
    EXPECT_EQ(segment.lba_count, 1u);
    EXPECT_EQ(segment.buffer_offset, 0u);
    EXPECT_EQ(segment.payload_size, 100u);
}

TEST(NoFIoLayoutTest, UnalignedBufferAddressIsFullyStaged) {
    NoFIoLayout layout;
    ASSERT_TRUE(
        BuildNoFIoLayout(kBlock512, 0, kAlignedAddress + 1, 1024, layout));
    ASSERT_EQ(layout.segments.size(), 1u);
    EXPECT_EQ(layout.staging_bytes(kBlock512), 1024u);

    // A tail-only copy cannot repair an incompatible starting address, so the
    // whole payload moves through the bounce buffer.
    const auto& segment = layout.segments[0];
    EXPECT_EQ(segment.kind, NoFIoSegmentKind::kStaged);
    EXPECT_EQ(segment.lba, 0u);
    EXPECT_EQ(segment.lba_count, 2u);
    EXPECT_EQ(segment.buffer_offset, 0u);
    EXPECT_EQ(segment.payload_size, 1024u);
}

TEST(NoFIoLayoutTest, UnalignedAddressAndLengthArePaddedTogether) {
    NoFIoLayout layout;
    ASSERT_TRUE(BuildNoFIoLayout(kBlock4096, kBlock4096 * 2,
                                 kAlignedAddress + 3, 4097, layout));
    ASSERT_EQ(layout.segments.size(), 1u);

    const auto& segment = layout.segments[0];
    EXPECT_EQ(segment.kind, NoFIoSegmentKind::kStaged);
    EXPECT_EQ(segment.lba, 2u);
    EXPECT_EQ(segment.lba_count, 2u);
    EXPECT_EQ(segment.payload_size, 4097u);
    // Padding never grows the caller's buffer: the staged area is 8192 B but
    // only the first 4097 B are ever copied.
    EXPECT_EQ(layout.staging_bytes(kBlock4096), 8192u);
}

TEST(NoFIoLayoutTest, PayloadNeverEscapesTheCallerBuffer) {
    const size_t size = 5000;
    for (uint32_t block_size : {kBlock512, kBlock4096}) {
        for (uintptr_t skew : {0u, 1u, 7u}) {
            NoFIoLayout layout;
            ASSERT_TRUE(BuildNoFIoLayout(block_size, 0, kAlignedAddress + skew,
                                         size, layout));
            uint64_t covered = 0;
            for (const auto& segment : layout.segments) {
                EXPECT_EQ(segment.buffer_offset, covered);
                EXPECT_GT(segment.payload_size, 0u);
                EXPECT_LE(
                    segment.payload_size,
                    static_cast<uint64_t>(segment.lba_count) * block_size);
                covered += segment.payload_size;
            }
            EXPECT_EQ(covered, size);
        }
    }
}

TEST(NoFIoLayoutTest, RejectsMisalignedDiskOffset) {
    NoFIoLayout layout;
    // The client cannot repair an allocation that does not start on a block
    // boundary, because its first byte is not addressable by an NVMe command.
    EXPECT_FALSE(BuildNoFIoLayout(kBlock512, 1, kAlignedAddress, 512, layout));
    EXPECT_TRUE(layout.segments.empty());
}

TEST(NoFIoLayoutTest, RejectsDegenerateInputs) {
    NoFIoLayout layout;
    EXPECT_FALSE(BuildNoFIoLayout(0, 0, kAlignedAddress, 512, layout));
    EXPECT_FALSE(BuildNoFIoLayout(kBlock512, 0, kAlignedAddress, 0, layout));
    EXPECT_TRUE(layout.segments.empty());
}

TEST(NoFIoLayoutTest, RejectsBlockCountOverflow) {
    NoFIoLayout layout;
    const size_t size =
        (static_cast<size_t>(std::numeric_limits<uint32_t>::max()) + 2) *
        kBlock512;
    EXPECT_FALSE(BuildNoFIoLayout(kBlock512, 0, kAlignedAddress, size, layout));
}
