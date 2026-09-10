#include "ha/snapshot/store_resource_snapshot_codec.h"

#include <gtest/gtest.h>
#include <msgpack.hpp>

#include <algorithm>
#include <functional>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/zstd_util.h"
#include "ha/snapshot/local_ssd_codec.h"
#include "master_metric_manager.h"
#include "segment.h"
#include "segment/pool.h"
#include "segment/pool_read_access.h"
#include "segment/pool_write_access.h"
#include "segment/snapshot.h"

namespace mooncake::ha {
namespace {

constexpr size_t kRegionSize = 16U * 1024 * 1024;

RegionDriverRegistry Drivers(bool enable_cxl = false) {
    RegionDriverConfig config;
    config.memory_allocator = BufferAllocatorType::OFFSET;
    if (enable_cxl) {
        config.cxl = CxlRegionDriverConfig{"cxl-test-path", kRegionSize};
    }
    auto drivers = CreateRegionDrivers(config);
    EXPECT_TRUE(drivers.has_value());
    return std::move(drivers.value());
}

Segment MakeSegment(size_t index, std::string name,
                    std::string protocol = "tcp", std::string host = {}) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = std::move(name);
    segment.base = 0x100000000ULL + index * 0x2000000ULL;
    segment.size = kRegionSize;
    segment.te_endpoint = segment.name + "-endpoint";
    segment.protocol = std::move(protocol);
    segment.host_id = std::move(host);
    return segment;
}

void CommitUnmount(SegmentPool& pool, const Segment& segment,
                   const UUID& client_id) {
    auto transaction = [&] {
        auto access = pool.AcquireWriteAccess();
        return access.PrepareUnmount(segment.id, client_id);
    }();
    ASSERT_TRUE(transaction.has_value());
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(std::move(*transaction).Commit(access), ErrorCode::OK);
    EXPECT_EQ(std::move(*transaction).Commit(access),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_EQ(std::move(*transaction).Rollback(access),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

tl::expected<std::vector<uint8_t>, SerializationError> CaptureAndEncode(
    const SegmentPool& pool, const LocalSsdPersistedState& local_ssd) {
    auto snapshot = pool.CaptureSnapshot();
    if (!snapshot) {
        return tl::make_unexpected(
            SerializationError(snapshot.error(), "capture SegmentPool failed"));
    }
    return StoreResourceSnapshotCodec::Encode(*snapshot, local_ssd);
}

tl::expected<LocalSsdPersistedState, SerializationError> DecodeAndRestore(
    SegmentPool& pool, const std::vector<uint8_t>& bytes,
    bool account_capacity) {
    auto decoded = StoreResourceSnapshotCodec::Decode(bytes);
    if (!decoded) return tl::make_unexpected(decoded.error());
    auto restored = pool.RestoreSnapshot(std::move(decoded->segment_pool),
                                         account_capacity);
    if (!restored)
        return tl::make_unexpected(
            SerializationError(restored.error(), "restore SegmentPool failed"));
    return std::move(decoded->local_ssd);
}

// Mutate the unpacked wire payload rather than duplicating the codec in tests.
std::vector<uint8_t> RewriteSnapshot(
    const std::vector<uint8_t>& snapshot,
    const std::function<void(msgpack::object&, msgpack::zone&)>& rewrite) {
    const auto bytes = zstd_decompress(snapshot);
    auto unpacked = msgpack::unpack(reinterpret_cast<const char*>(bytes.data()),
                                    bytes.size());
    auto root = unpacked.get();
    rewrite(root, *unpacked.zone());

    msgpack::sbuffer buffer;
    msgpack::pack(buffer, root);
    return zstd_compress(reinterpret_cast<const uint8_t*>(buffer.data()),
                         buffer.size(), 3);
}

msgpack::object& SnapshotField(msgpack::object& root, std::string_view name) {
    for (uint32_t i = 0; i < root.via.map.size; ++i) {
        auto& item = root.via.map.ptr[i];
        if (item.key.as<std::string>() == name) {
            return item.val;
        }
    }

    throw std::runtime_error("missing snapshot field");
}

std::string PackLocalSsd(const LocalSsdPersistedState& state) {
    msgpack::sbuffer buffer;
    MsgpackPacker packer(&buffer);
    EXPECT_TRUE(LocalSsdCodec::Encode(state, packer).has_value());
    return {buffer.data(), buffer.size()};
}

std::vector<uint8_t> ReplaceSnapshotActiveNames(
    const std::vector<uint8_t>& snapshot,
    const std::vector<std::string>& active_names) {
    return RewriteSnapshot(snapshot, [&](auto& root, auto& zone) {
        SnapshotField(root, "an") = msgpack::object(active_names, zone);
    });
}

std::vector<uint8_t> ReplaceSnapshotAllocatorType(
    const std::vector<uint8_t>& snapshot, uint64_t allocator_type) {
    return RewriteSnapshot(snapshot, [&](auto& root, auto&) {
        SnapshotField(root, "ma") = msgpack::object(allocator_type);
    });
}

}  // namespace

TEST(StoreResourceSnapshotCodecTest,
     CapturedSnapshotOutlivesPoolAndPreservesOriginalLayout) {
    auto& metrics = MasterMetricManager::instance();
    const auto baseline_used = metrics.get_allocated_mem_size();
    const auto baseline_capacity = metrics.get_total_mem_capacity();
    const auto segment = MakeSegment(0, "detached-snapshot", "tcp", "host-a");
    const UUID client = generate_uuid();
    std::optional<SegmentPoolSnapshot> snapshot;
    std::vector<uint8_t> original_bytes;
    uintptr_t first_address = 0;
    uintptr_t last_address = 0;
    {
        SegmentPool pool(Drivers());
        ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(segment, client),
                  ErrorCode::OK);
        auto allocator = pool.AcquireReadAccess().GetAllocator(segment.id);
        auto first = allocator->allocate(4096);
        auto hole = allocator->allocate(4096);
        auto last = allocator->allocate(4096);
        ASSERT_NE(first, nullptr);
        ASSERT_NE(hole, nullptr);
        ASSERT_NE(last, nullptr);
        first_address = reinterpret_cast<uintptr_t>(first->data());
        last_address = reinterpret_cast<uintptr_t>(last->data());
        hole.reset();
        const auto used = metrics.get_allocated_mem_size();
        auto captured = pool.CaptureSnapshot();
        ASSERT_TRUE(captured.has_value());
        snapshot.emplace(std::move(*captured));
        EXPECT_EQ(metrics.get_allocated_mem_size(), used);
        auto encoded = StoreResourceSnapshotCodec::Encode(*snapshot, {});
        ASSERT_TRUE(encoded.has_value());
        original_bytes = std::move(*encoded);

        // Change both allocation bins/nodes and catalog/placement after
        // capture.
        first.reset();
        last.reset();
        auto all = allocator->allocate(kRegionSize);
        ASSERT_NE(all, nullptr);
        ASSERT_EQ(pool.AcquireWriteAccess().SetSegmentStatusByName(
                      segment.name, SegmentStatus::DRAINED),
                  ErrorCode::OK);
        pool.AcquireWriteAccess().Clear();
    }
    EXPECT_EQ(metrics.get_allocated_mem_size(), baseline_used);
    EXPECT_EQ(metrics.get_total_mem_capacity(), baseline_capacity);
    auto encoded = StoreResourceSnapshotCodec::Encode(*snapshot, {});
    ASSERT_TRUE(encoded.has_value());
    EXPECT_EQ(*encoded, original_bytes);
    snapshot.reset();
    EXPECT_EQ(metrics.get_allocated_mem_size(), baseline_used);

    SegmentPool restored(Drivers());
    ASSERT_TRUE(DecodeAndRestore(restored, *encoded, false).has_value());
    EXPECT_EQ(restored.GetMemoryUsage().used_bytes, 8192U);
    auto access = restored.AcquireReadAccess();
    const auto* mounted = access.Catalog().Find(segment.id);
    ASSERT_NE(mounted, nullptr);
    EXPECT_EQ(mounted->status, SegmentStatus::OK);
    EXPECT_EQ(mounted->client_id, client);
    EXPECT_EQ(mounted->segment.host_id, segment.host_id);
    EXPECT_TRUE(access.Placement().Contains(segment.name,
                                            AllocationCandidateKind::NATIVE));
    auto next = access.GetAllocator(segment.id)->allocate(4096);
    ASSERT_NE(next, nullptr);
    EXPECT_NE(reinterpret_cast<uintptr_t>(next->data()), first_address);
    EXPECT_NE(reinterpret_cast<uintptr_t>(next->data()), last_address);
}

TEST(StoreResourceSnapshotCodecTest,
     DecodeReturnsDetachedStateWithoutMetricSideEffects) {
    SegmentPool source(Drivers());
    const auto segment = MakeSegment(0, "decode-detached");
    const auto client = generate_uuid();
    ASSERT_EQ(source.AcquireWriteAccess().MountSegment(segment, client),
              ErrorCode::OK);
    auto allocator = source.AcquireReadAccess().GetAllocator(segment.id);
    auto buffer = allocator->allocate(4096);
    ASSERT_NE(buffer, nullptr);
    auto encoded = CaptureAndEncode(source, {});
    ASSERT_TRUE(encoded);
    auto& metrics = MasterMetricManager::instance();
    const auto used = metrics.get_allocated_mem_size();
    const auto capacity = metrics.get_total_mem_capacity();
    {
        auto decoded = StoreResourceSnapshotCodec::Decode(*encoded);
        ASSERT_TRUE(decoded);
        ASSERT_EQ(decoded->segment_pool.regions.size(), 1U);
        EXPECT_EQ(decoded->segment_pool.regions.front().allocator.used_bytes,
                  4096U);
        EXPECT_EQ(metrics.get_allocated_mem_size(), used);
        EXPECT_EQ(metrics.get_total_mem_capacity(), capacity);
    }
    EXPECT_EQ(metrics.get_allocated_mem_size(), used);
    EXPECT_EQ(metrics.get_total_mem_capacity(), capacity);
}

TEST(StoreResourceSnapshotCodecTest, SnapshotDecodeReencodePreservesBytes) {
    SegmentPool pool(Drivers());
    const UUID client{1, 1};
    auto first = MakeSegment(0, "roundtrip-z");
    auto second = MakeSegment(1, "roundtrip-a");
    auto inactive = MakeSegment(2, "roundtrip-inactive");
    // ID order differs from both mount order and active-name order.
    first.id = UUID{0, 3};
    second.id = UUID{0, 1};
    inactive.id = UUID{0, 2};
    {
        auto access = pool.AcquireWriteAccess();
        for (const auto* segment : {&first, &second, &inactive}) {
            ASSERT_EQ(access.MountSegment(*segment, client), ErrorCode::OK);
        }
        ASSERT_EQ(access.SetSegmentStatusByName(inactive.name,
                                                SegmentStatus::DRAINED),
                  ErrorCode::OK);
    }
    auto encoded = CaptureAndEncode(pool, {});
    ASSERT_TRUE(encoded.has_value()) << encoded.error().message;

    for (bool reverse_region_order : {false, true}) {
        SCOPED_TRACE(reverse_region_order);
        auto input = *encoded;
        if (reverse_region_order) {
            input = RewriteSnapshot(*encoded, [](auto& root, auto&) {
                auto& regions = SnapshotField(root, "ms").via.map;
                std::reverse(regions.ptr, regions.ptr + regions.size);
            });
        }
        auto decoded = StoreResourceSnapshotCodec::Decode(input);
        ASSERT_TRUE(decoded.has_value()) << decoded.error().message;
        const auto& snapshot = decoded->segment_pool;
        ASSERT_EQ(snapshot.regions.size(), 3U);
        EXPECT_EQ(snapshot.regions[0].mounted.segment.id, second.id);
        EXPECT_EQ(snapshot.regions[1].mounted.segment.id, inactive.id);
        EXPECT_EQ(snapshot.regions[2].mounted.segment.id, first.id);
        EXPECT_EQ(snapshot.active_names,
                  (std::vector<std::string>{first.name, second.name}));

        auto reencoded =
            StoreResourceSnapshotCodec::Encode(snapshot, decoded->local_ssd);
        ASSERT_TRUE(reencoded.has_value()) << reencoded.error().message;
        EXPECT_EQ(*reencoded, *encoded);
    }
}

TEST(StoreResourceSnapshotCodecTest,
     OutOfRangeSnapshotAllocatorTypeReturnsError) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "snapshot-invalid-allocator-type");
    ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(segment, client),
              ErrorCode::OK);
    auto encoded = CaptureAndEncode(pool, LocalSsdPersistedState{});
    ASSERT_TRUE(encoded.has_value()) << encoded.error().message;

    for (const uint64_t value :
         {uint64_t{std::numeric_limits<int32_t>::max()} + 1,
          std::numeric_limits<uint64_t>::max()}) {
        auto corrupted = ReplaceSnapshotAllocatorType(*encoded, value);
        ASSERT_NO_THROW({
            auto result = DecodeAndRestore(pool, corrupted, false);
            ASSERT_FALSE(result.has_value());
            EXPECT_EQ(result.error().code, ErrorCode::DESERIALIZE_FAIL);
        });
        EXPECT_NE(pool.AcquireReadAccess().Catalog().Find(segment.id), nullptr);
    }
    CommitUnmount(pool, segment, client);
}

TEST(StoreResourceSnapshotCodecTest,
     InvalidSnapshotDoesNotReplacePublishedPool) {
    SegmentPool source(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "snapshot-active");
    ASSERT_EQ(source.AcquireWriteAccess().MountSegment(segment, client),
              ErrorCode::OK);
    auto encoded = CaptureAndEncode(source, LocalSsdPersistedState{});
    ASSERT_TRUE(encoded.has_value()) << encoded.error().message;

    SegmentPool restored(Drivers());
    auto stale = MakeSegment(1, "published");
    ASSERT_EQ(restored.AcquireWriteAccess().MountSegment(stale, client),
              ErrorCode::OK);
    auto corrupted = ReplaceSnapshotActiveNames(*encoded, {});
    auto result = DecodeAndRestore(restored, corrupted, false);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code, ErrorCode::DESERIALIZE_FAIL);

    {
        auto view = restored.AcquireReadAccess();
        EXPECT_NE(view.Catalog().Find(stale.id), nullptr);
        EXPECT_EQ(view.Catalog().Find(segment.id), nullptr);
    }

    CommitUnmount(source, segment, client);
}

TEST(StoreResourceSnapshotCodecTest,
     SnapshotInteroperatesWithLegacySerializer) {
    SegmentPool source(Drivers());
    const UUID client = generate_uuid();
    // Preserve insertion order, not lexicographic name order.
    auto first = MakeSegment(0, "compat-z", "tcp", "host-a");
    auto second = MakeSegment(1, "compat-a", "tcp", "host-b");
    ASSERT_EQ(source.AcquireWriteAccess().MountSegment(first, client),
              ErrorCode::OK);
    ASSERT_EQ(source.AcquireWriteAccess().MountSegment(second, client),
              ErrorCode::OK);

    LocalSsdPersistedState ssd;
    ssd[client] = LocalSsdPersistedClient{
        .enable_offloading = true,
        .total_capacity_bytes = 12345,
        .pending_offloads = {
            {"tenant/key",
             OffloadTaskItem{
                 .tenant_id = "tenant", .key = "key", .size = 4096}}}};
    auto encoded = CaptureAndEncode(source, ssd);
    ASSERT_TRUE(encoded.has_value());

    SegmentManager legacy(BufferAllocatorType::OFFSET);
    SegmentSerializer serializer(&legacy);
    auto old_decoded = serializer.Deserialize(*encoded);
    ASSERT_TRUE(old_decoded.has_value()) << old_decoded.error().message;
    EXPECT_EQ(PackLocalSsd(*old_decoded), PackLocalSsd(ssd));

    MountedSegment mounted;
    ASSERT_EQ(legacy.getView().GetMountedSegment(first.id, mounted),
              ErrorCode::OK);
    EXPECT_EQ(mounted.segment.host_id, first.host_id);

    auto old_encoded = serializer.Serialize(*old_decoded);
    ASSERT_TRUE(old_encoded.has_value());

    SegmentPool restored(Drivers());
    auto decoded = DecodeAndRestore(restored, *old_encoded, false);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().message;
    EXPECT_EQ(PackLocalSsd(*decoded), PackLocalSsd(ssd));
    {
        auto access = restored.AcquireReadAccess();
        ASSERT_EQ(access.Catalog().Regions().size(), 2U);
        ASSERT_NE(access.Catalog().Find(first.id), nullptr);
        EXPECT_EQ(access.Catalog().Find(first.id)->client_id, client);
        std::vector<std::string> names;
        access.Placement().GetActiveSegmentNames(
            AllocationCandidateKind::NATIVE, names);
        EXPECT_EQ(names, (std::vector<std::string>{first.name, second.name}));
    }

    // Historical MountedSegment arrays had no host_id, and ld was optional.
    auto historical = RewriteSnapshot(*old_encoded, [](auto& root, auto&) {
        auto& regions = SnapshotField(root, "ms");
        for (uint32_t i = 0; i < regions.via.map.size; ++i) {
            regions.via.map.ptr[i].val.via.array.size = 8;
        }

        for (uint32_t i = 0; i < root.via.map.size; ++i) {
            if (root.via.map.ptr[i].key.template as<std::string>() == "ld") {
                root.via.map.ptr[i] = root.via.map.ptr[root.via.map.size - 1];
                --root.via.map.size;
                break;
            }
        }
    });
    decoded = DecodeAndRestore(restored, historical, false);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(decoded->empty());
    EXPECT_TRUE(restored.AcquireReadAccess()
                    .Catalog()
                    .Find(first.id)
                    ->segment.host_id.empty());
}

TEST(StoreResourceSnapshotCodecTest,
     MalformedSnapshotsPreservePublishedResourcesAndMetrics) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    const auto segment = MakeSegment(0, "snapshot-atomic");
    const auto other = MakeSegment(1, "snapshot-atomic-other");
    ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(segment, client),
              ErrorCode::OK);
    ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(other, client),
              ErrorCode::OK);
    auto allocator = pool.AcquireReadAccess().GetAllocator(segment.id);
    auto live = allocator->allocate(4096);
    ASSERT_NE(live, nullptr);
    auto encoded = CaptureAndEncode(pool, {});
    ASSERT_TRUE(encoded.has_value());
    auto& metrics = MasterMetricManager::instance();
    const auto used = metrics.get_allocated_mem_size();
    const auto capacity = metrics.get_total_mem_capacity();

    using Rewrite = std::function<void(msgpack::object&, msgpack::zone&)>;
    const std::vector<std::pair<std::string, Rewrite>> cases{
        {"missing owner",
         [](auto& root, auto&) { SnapshotField(root, "cs").via.map.size = 0; }},
        {"duplicate ownership",
         [](auto& root, auto&) {
             auto& ids = SnapshotField(root, "cs").via.map.ptr[0].val;
             ids.via.array.ptr[1] = ids.via.array.ptr[0];
         }},
        {"unknown owned region",
         [](auto& root, auto& zone) {
             SnapshotField(root, "cs").via.map.ptr[0].val.via.array.ptr[0] =
                 msgpack::object(UuidToString(UUID{99, 99}), zone);
         }},
        {"duplicate region",
         [](auto& root, auto&) {
             auto& regions = SnapshotField(root, "ms");
             regions.via.map.ptr[1] = regions.via.map.ptr[0];
         }},
        {"invalid status",
         [](auto& root, auto&) {
             SnapshotField(root, "ms").via.map.ptr[1].val.via.array.ptr[5] =
                 msgpack::object(99);
         }},
        {"mismatched base",
         [](auto& root, auto&) {
             SnapshotField(root, "ms").via.map.ptr[1].val.via.array.ptr[2] =
                 msgpack::object(uint64_t{1});
         }},
        {"mismatched capacity",
         [](auto& root, auto&) {
             SnapshotField(root, "ms").via.map.ptr[1].val.via.array.ptr[3] =
                 msgpack::object(uint64_t{1});
         }},
        {"mismatched name",
         [](auto& root, auto& zone) {
             SnapshotField(root, "ms").via.map.ptr[1].val.via.array.ptr[1] =
                 msgpack::object(std::string("wrong"), zone);
         }},
        {"mismatched endpoint",
         [](auto& root, auto& zone) {
             SnapshotField(root, "ms").via.map.ptr[1].val.via.array.ptr[4] =
                 msgpack::object(std::string("wrong"), zone);
         }},
        {"broken allocator",
         [](auto& root, auto&) {
             SnapshotField(root, "ms").via.map.ptr[1].val.via.array.ptr[7] =
                 msgpack::object();
         }},
        {"usage exceeds capacity",
         [](auto& root, auto&) {
             SnapshotField(root, "ms")
                 .via.map.ptr[1]
                 .val.via.array.ptr[7]
                 .via.array.ptr[3] = msgpack::object(uint64_t{kRegionSize + 1});
         }},
        {"usage overflows metrics",
         [](auto& root, auto&) {
             auto& allocator =
                 SnapshotField(root, "ms").via.map.ptr[1].val.via.array.ptr[7];
             allocator.via.array.ptr[2] =
                 msgpack::object(std::numeric_limits<uint64_t>::max());
             allocator.via.array.ptr[3] = allocator.via.array.ptr[2];
         }},
        {"second adoption fails",
         [&](auto& root, auto& zone) {
             // A zero UUID passes payload consistency checks but is rejected by
             // the memory driver. The first active region has already staged.
             const auto zero = msgpack::object(UuidToString(UUID{0, 0}), zone);
             auto& regions = SnapshotField(root, "ms");
             for (uint32_t i = 0; i < regions.via.map.size; ++i) {
                 auto& item = regions.via.map.ptr[i];
                 if (item.key.template as<std::string>() ==
                     UuidToString(other.id)) {
                     item.key = zero;
                     item.val.via.array.ptr[0] = zero;
                 }
             }
             auto& ids = SnapshotField(root, "cs").via.map.ptr[0].val;
             for (uint32_t i = 0; i < ids.via.array.size; ++i) {
                 if (ids.via.array.ptr[i].template as<std::string>() ==
                     UuidToString(other.id)) {
                     ids.via.array.ptr[i] = zero;
                 }
             }
         }},
        {"invalid local ssd",
         [](auto& root, auto&) {
             SnapshotField(root, "ld") = msgpack::object(42);
         }},
    };

    for (const auto& [name, rewrite] : cases) {
        SCOPED_TRACE(name);
        auto corrupted = RewriteSnapshot(*encoded, rewrite);
        auto result = DecodeAndRestore(pool, corrupted, true);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code, name == "second adoption fails"
                                           ? ErrorCode::INVALID_PARAMS
                                           : ErrorCode::DESERIALIZE_FAIL);
        EXPECT_EQ(pool.AcquireReadAccess().GetAllocator(segment.id), allocator);

        if (name == "second adoption fails") {
            EXPECT_EQ(result.error().message, "restore SegmentPool failed");
        } else if (name == "usage exceeds capacity" ||
                   name == "usage overflows metrics") {
            EXPECT_EQ(result.error().message,
                      "snapshot OffsetBufferAllocator has invalid usage");
        }

        EXPECT_EQ(pool.AcquireReadAccess().Catalog().Regions().size(), 2U);
        EXPECT_TRUE(pool.AcquireReadAccess().Placement().Contains(
            segment.name, AllocationCandidateKind::NATIVE));
        EXPECT_EQ(pool.GetMemoryUsage().used_bytes, 4096U);
        EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes, 2 * kRegionSize);
        EXPECT_EQ(metrics.get_allocated_mem_size(), used);
        EXPECT_EQ(metrics.get_total_mem_capacity(), capacity);
        EXPECT_EQ(metrics.get_segment_allocated_mem_size(segment.name), 4096);
    }
}

TEST(StoreResourceSnapshotCodecTest,
     SnapshotRejectsUnsupportedDriversAndCorruptContainers) {
    RegionDriverConfig config;
    config.memory_allocator = BufferAllocatorType::CACHELIB;
    auto drivers = CreateRegionDrivers(config);
    ASSERT_TRUE(drivers.has_value());
    SegmentPool unsupported(std::move(*drivers));
    auto encoded = CaptureAndEncode(unsupported, {});
    ASSERT_FALSE(encoded.has_value());
    EXPECT_EQ(encoded.error().code, ErrorCode::SERIALIZE_UNSUPPORTED);

    SegmentPool cxl(Drivers(true));
    ASSERT_EQ(cxl.AcquireWriteAccess().MountSegment(
                  MakeSegment(0, "snapshot-cxl", "cxl"), generate_uuid()),
              ErrorCode::OK);
    encoded = CaptureAndEncode(cxl, {});
    ASSERT_FALSE(encoded.has_value());
    EXPECT_EQ(encoded.error().code, ErrorCode::SERIALIZE_UNSUPPORTED);

    SegmentPool pool(Drivers());
    const auto segment = MakeSegment(1, "snapshot-container");
    ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(segment, generate_uuid()),
              ErrorCode::OK);
    auto allocator = pool.AcquireReadAccess().GetAllocator(segment.id);

    // Invalid zstd, invalid MessagePack, and a valid non-map root.
    const uint8_t invalid_msgpack = 0xc1;
    const uint8_t array = 0x90;
    const std::vector<std::vector<uint8_t>> corrupt{
        {},
        {1, 2, 3},
        zstd_compress(&invalid_msgpack, 1, 3),
        zstd_compress(&array, 1, 3)};
    for (const auto& bytes : corrupt) {
        auto decoded = DecodeAndRestore(pool, bytes, false);
        ASSERT_FALSE(decoded.has_value());
        EXPECT_EQ(decoded.error().code, ErrorCode::DESERIALIZE_FAIL);
        EXPECT_EQ(pool.AcquireReadAccess().GetAllocator(segment.id), allocator);
    }
}

}  // namespace mooncake::ha
