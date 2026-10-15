// Wire compatibility of the L4 task id (OffloadTaskItem::task_id and
// PromotionTaskItem::task_id, struct_pack compatible fields).
//
// Masters and stores of different versions talk to each other during a roll.
// coro_rpc packs the arguments of a call with struct_pack as one tuple and
// unpacks them into the handler's argument tuple; it packs a return value the
// same way. These tests pack with the item layout from before task ids
// (wire_v0) and unpack with the current one, and the other way round, in the
// shapes the offload and promotion RPCs use. They must agree on the type hash
// (else the peer refuses the buffer) and every field other than task_id must
// survive; task_id must arrive empty from an old peer.

#include <gtest/gtest.h>

#include <string>
#include <tuple>
#include <vector>

#include <ylt/struct_pack.hpp>
#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace wire_v0 {

// The layouts before task ids.
struct OffloadTaskItem {
    std::string tenant_id;
    std::string key;
    int64_t size;
};
YLT_REFL(OffloadTaskItem, tenant_id, key, size);

struct PromotionTaskItem {
    std::string tenant_id;
    std::string key;
    int64_t size;
};
YLT_REFL(PromotionTaskItem, tenant_id, key, size);

}  // namespace wire_v0

namespace mooncake {
namespace {

using OffloadArgsV0 = std::tuple<UUID, std::vector<wire_v0::OffloadTaskItem>,
                                 std::vector<StorageObjectMetadata>>;
using OffloadArgs = std::tuple<UUID, std::vector<OffloadTaskItem>,
                               std::vector<StorageObjectMetadata>>;
using OffloadResultV0 =
    tl::expected<std::vector<wire_v0::OffloadTaskItem>, ErrorCode>;
using OffloadResult = tl::expected<std::vector<OffloadTaskItem>, ErrorCode>;
using PromotionResultV0 =
    tl::expected<std::vector<wire_v0::PromotionTaskItem>, ErrorCode>;
using PromotionResult = tl::expected<std::vector<PromotionTaskItem>, ErrorCode>;

constexpr UUID kClient{0x1111, 0x2222};

std::vector<StorageObjectMetadata> Metas() {
    return {StorageObjectMetadata{7, 4096, 3, 1000, "10.0.0.1:1234"},
            StorageObjectMetadata{-1, 0, 0, -1, ""},
            StorageObjectMetadata{9, 8192, 5, 2000, "10.0.0.1:1234"}};
}

std::vector<wire_v0::OffloadTaskItem> OldItems() {
    return {{"default", "key-a", 1000},
            {"tenant-b", "key-b", 1},
            {"default", std::string(300, 'k'), 2000}};
}

std::vector<OffloadTaskItem> NewItems() {
    std::vector<OffloadTaskItem> items{
        OffloadTaskItem{.tenant_id = "default", .key = "key-a", .size = 1000},
        OffloadTaskItem{.tenant_id = "tenant-b", .key = "key-b", .size = 1},
        OffloadTaskItem{.tenant_id = "default",
                        .key = std::string(300, 'k'),
                        .size = 2000}};
    items[0].task_id = L4TaskId{0xfedcba9876543210ULL, 1};
    // items[1] has no id: a vector may mix both.
    items[2].task_id = L4TaskId{0x0123456789abcdefULL, ~0ULL};
    return items;
}

template <typename A, typename B>
void ExpectSameObjectFields(const std::vector<A>& a, const std::vector<B>& b) {
    ASSERT_EQ(a.size(), b.size());
    for (size_t i = 0; i < a.size(); ++i) {
        EXPECT_EQ(a[i].tenant_id, b[i].tenant_id) << i;
        EXPECT_EQ(a[i].key, b[i].key) << i;
        EXPECT_EQ(a[i].size, b[i].size) << i;
    }
}

void ExpectSameMetas(const std::vector<StorageObjectMetadata>& a,
                     const std::vector<StorageObjectMetadata>& b) {
    ASSERT_EQ(a.size(), b.size());
    for (size_t i = 0; i < a.size(); ++i) {
        EXPECT_EQ(a[i].bucket_id, b[i].bucket_id) << i;
        EXPECT_EQ(a[i].offset, b[i].offset) << i;
        EXPECT_EQ(a[i].key_size, b[i].key_size) << i;
        EXPECT_EQ(a[i].data_size, b[i].data_size) << i;
        EXPECT_EQ(a[i].transport_endpoint, b[i].transport_endpoint) << i;
    }
}

TEST(L4TaskIdWireTest, TypeHashIgnoresTheTaskId) {
    EXPECT_EQ(struct_pack::get_type_code<wire_v0::OffloadTaskItem>(),
              struct_pack::get_type_code<OffloadTaskItem>());
    EXPECT_EQ(struct_pack::get_type_code<OffloadArgsV0>(),
              struct_pack::get_type_code<OffloadArgs>());
    EXPECT_EQ(struct_pack::get_type_code<OffloadResultV0>(),
              struct_pack::get_type_code<OffloadResult>());
    EXPECT_EQ(struct_pack::get_type_code<PromotionResultV0>(),
              struct_pack::get_type_code<PromotionResult>());
}

// NotifyOffloadSuccess(client_id, tasks, metadatas) from an old store to a
// new master: the master reads every item without an id.
TEST(L4TaskIdWireTest, OldCompletionArgumentsReadByANewMaster) {
    const auto old_items = OldItems();
    const auto metas = Metas();
    // coro_rpc_client packs the arguments variadically.
    const auto buffer =
        struct_pack::serialize<std::string>(kClient, old_items, metas);
    OffloadArgs args;
    ASSERT_FALSE(struct_pack::deserialize_to(args, buffer));
    EXPECT_EQ(std::get<0>(args), kClient);
    ExpectSameObjectFields(std::get<1>(args), old_items);
    for (const auto& item : std::get<1>(args)) {
        EXPECT_FALSE(item.task_id.has_value());
    }
    ExpectSameMetas(std::get<2>(args), metas);
}

// NotifyOffloadSuccess from a new store to an old master: the old master
// skips the ids and reads everything else, including the metadata vector
// that follows the items.
TEST(L4TaskIdWireTest, NewCompletionArgumentsReadByAnOldMaster) {
    const auto new_items = NewItems();
    const auto metas = Metas();
    const auto buffer =
        struct_pack::serialize<std::string>(kClient, new_items, metas);
    OffloadArgsV0 args;
    ASSERT_FALSE(struct_pack::deserialize_to(args, buffer));
    EXPECT_EQ(std::get<0>(args), kClient);
    ExpectSameObjectFields(std::get<1>(args), new_items);
    ExpectSameMetas(std::get<2>(args), metas);
}

TEST(L4TaskIdWireTest, NewCompletionArgumentsKeepTheirIds) {
    const auto new_items = NewItems();
    const auto metas = Metas();
    const auto buffer =
        struct_pack::serialize<std::string>(kClient, new_items, metas);
    OffloadArgs args;
    ASSERT_FALSE(struct_pack::deserialize_to(args, buffer));
    const auto& items = std::get<1>(args);
    ExpectSameObjectFields(items, new_items);
    ASSERT_EQ(items.size(), 3u);
    ASSERT_TRUE(items[0].task_id.has_value());
    EXPECT_EQ(*items[0].task_id, *new_items[0].task_id);
    EXPECT_FALSE(items[1].task_id.has_value());
    ASSERT_TRUE(items[2].task_id.has_value());
    EXPECT_EQ(*items[2].task_id, *new_items[2].task_id);
    ExpectSameMetas(std::get<2>(args), metas);
}

// OffloadObjectHeartbeat's return value, both directions.
TEST(L4TaskIdWireTest, HeartbeatResultBothDirections) {
    {
        const OffloadResult sent = NewItems();
        const auto buffer = struct_pack::serialize<std::string>(sent);
        OffloadResultV0 got;
        ASSERT_FALSE(struct_pack::deserialize_to(got, buffer));
        ASSERT_TRUE(got.has_value());
        ExpectSameObjectFields(*got, *sent);
    }
    {
        const OffloadResultV0 sent = OldItems();
        const auto buffer = struct_pack::serialize<std::string>(sent);
        OffloadResult got;
        ASSERT_FALSE(struct_pack::deserialize_to(got, buffer));
        ASSERT_TRUE(got.has_value());
        ExpectSameObjectFields(*got, *sent);
        for (const auto& item : *got) {
            EXPECT_FALSE(item.task_id.has_value());
        }
    }
    {
        const OffloadResult sent = NewItems();
        const auto buffer = struct_pack::serialize<std::string>(sent);
        OffloadResult got;
        ASSERT_FALSE(struct_pack::deserialize_to(got, buffer));
        ASSERT_TRUE(got.has_value());
        ASSERT_TRUE((*got)[0].task_id.has_value());
        EXPECT_EQ(*(*got)[0].task_id, *(*sent)[0].task_id);
        EXPECT_FALSE((*got)[1].task_id.has_value());
    }
    {
        const OffloadResult sent =
            tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        const auto buffer = struct_pack::serialize<std::string>(sent);
        OffloadResultV0 got;
        ASSERT_FALSE(struct_pack::deserialize_to(got, buffer));
        ASSERT_FALSE(got.has_value());
        EXPECT_EQ(got.error(), ErrorCode::SEGMENT_NOT_FOUND);
    }
}

// PromotionObjectHeartbeat's return value, both directions.
TEST(L4TaskIdWireTest, PromotionHeartbeatResultBothDirections) {
    std::vector<PromotionTaskItem> new_items{
        PromotionTaskItem{.tenant_id = "default", .key = "p1", .size = 10},
        PromotionTaskItem{.tenant_id = "default", .key = "p2", .size = 20}};
    new_items[0].task_id = L4TaskId{42, 7};
    {
        const PromotionResult sent = new_items;
        const auto buffer = struct_pack::serialize<std::string>(sent);
        PromotionResultV0 got;
        ASSERT_FALSE(struct_pack::deserialize_to(got, buffer));
        ASSERT_TRUE(got.has_value());
        ExpectSameObjectFields(*got, new_items);
    }
    {
        const PromotionResultV0 sent = std::vector<wire_v0::PromotionTaskItem>{
            {"default", "p1", 10}, {"default", "p2", 20}};
        const auto buffer = struct_pack::serialize<std::string>(sent);
        PromotionResult got;
        ASSERT_FALSE(struct_pack::deserialize_to(got, buffer));
        ASSERT_TRUE(got.has_value());
        ExpectSameObjectFields(*got, *sent);
        for (const auto& item : *got) {
            EXPECT_FALSE(item.task_id.has_value());
        }
    }
    {
        const PromotionResult sent = new_items;
        const auto buffer = struct_pack::serialize<std::string>(sent);
        PromotionResult got;
        ASSERT_FALSE(struct_pack::deserialize_to(got, buffer));
        ASSERT_TRUE((*got)[0].task_id.has_value());
        EXPECT_EQ(*(*got)[0].task_id, (L4TaskId{42, 7}));
        EXPECT_FALSE((*got)[1].task_id.has_value());
    }
}

// The argument shape of the promotion RPCs that take one item.
TEST(L4TaskIdWireTest, SingleItemArgumentKeepsItsId) {
    PromotionTaskItem item{.tenant_id = "default", .key = "p", .size = 3};
    item.task_id = L4TaskId{5, 6};
    const std::vector<std::string> preferred{"seg-a"};
    const auto buffer =
        struct_pack::serialize<std::string>(kClient, item, preferred);
    std::tuple<UUID, PromotionTaskItem, std::vector<std::string>> args;
    ASSERT_FALSE(struct_pack::deserialize_to(args, buffer));
    EXPECT_EQ(std::get<0>(args), kClient);
    EXPECT_EQ(std::get<1>(args).key, "p");
    ASSERT_TRUE(std::get<1>(args).task_id.has_value());
    EXPECT_EQ(*std::get<1>(args).task_id, (L4TaskId{5, 6}));
    EXPECT_EQ(std::get<2>(args), preferred);
}

}  // namespace
}  // namespace mooncake
