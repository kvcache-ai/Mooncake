#include "common/sharded_map.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <stdexcept>
#include <string>

#include "random.h"

namespace mooncake::test {
namespace {
struct IdentityHash {
    size_t operator()(size_t key) const noexcept { return key; }
};
struct CountingEqual {
    static inline size_t calls = 0;
    bool operator()(size_t a, size_t b) const noexcept {
        ++calls;
        return a == b;
    }
};
template <typename Base>
struct ObservedMap : Base {
    static inline size_t largest_rehash = 0, rehashes = 0;
    auto try_emplace(size_t key, int value) {
        const auto before = this->bucket_count();
        auto result = Base::try_emplace(key, value);
        if (this->bucket_count() != before) {
            largest_rehash = std::max(largest_rehash, this->size());
            ++rehashes;
        }
        return result;
    }
};
template <typename Base>
struct ReservedMap : Base {
    static inline size_t max_bucket_size = 0;
    // libc++ preserves powers of two here, exposing correlated routing bits.
    ReservedMap() { this->rehash(2048); }
    auto find(size_t key) const {
        // Cached hashes can skip equality checks while still walking a chain.
        if constexpr (requires { this->bucket_size(this->bucket(key)); }) {
            max_bucket_size =
                std::max(max_bucket_size, this->bucket_size(this->bucket(key)));
        }
        return Base::find(key);
    }
};
struct CollidingHash {
    size_t operator()(size_t) const noexcept { return 0; }
};
struct StandardMaps {
    using Strings = std::unordered_map<std::string, int>;
    using Collisions = std::unordered_map<size_t, int, CollidingHash>;
    using Numbers =
        std::unordered_map<size_t, int, IdentityHash, CountingEqual>;
};
#if BOOST_VERSION >= 108200
struct BoostMaps {
    using Strings = boost::unordered_node_map<std::string, int>;
    using Collisions = boost::unordered_node_map<size_t, int, CollidingHash>;
    using Numbers =
        boost::unordered_node_map<size_t, int, IdentityHash, CountingEqual>;
};
using Implementations = ::testing::Types<StandardMaps, BoostMaps>;
#else
using Implementations = ::testing::Types<StandardMaps>;
#endif
template <typename T>
class ShardedMapTest : public ::testing::Test {};
TYPED_TEST_SUITE(ShardedMapTest, Implementations);

TYPED_TEST(ShardedMapTest, MutationsMatchReference) {
    ShardedMap<std::string, int, 16, typename TypeParam::Strings> index;
    std::unordered_map<std::string, int> reference;
    RandomEngine generator(42);
    for (int i = 0; i < 100000; ++i) {
        const auto id = randomIndex(20000, generator);
        const std::string key = std::string("tenant\0", 7) + std::to_string(id);
        switch (randomIndex(3, generator)) {
            case 0:
                EXPECT_EQ(index.insert(key, i),
                          reference.emplace(key, i).second);
                break;
            case 1:
                index.insert_or_assign(key, i);
                reference[key] = i;
                break;
            default:
                EXPECT_EQ(index.erase(key), reference.erase(key) != 0);
        }
        ASSERT_EQ(index.size(), reference.size());
        const auto* actual = index.find(key);
        const auto expected = reference.find(key);
        ASSERT_EQ(actual == nullptr, expected == reference.end());
        if (actual) {
            EXPECT_EQ(*actual, expected->second);
        }
    }
    for (const auto& [key, value] : reference) {
        ASSERT_NE(index.find(key), nullptr);
        EXPECT_EQ(*index.find(key), value);
    }
}

TYPED_TEST(ShardedMapTest, RehashesStayLocal) {
    using Map = ObservedMap<typename TypeParam::Numbers>;
    Map::largest_rehash = Map::rehashes = 0;
    ShardedMap<size_t, int, 64, Map> index;
    constexpr size_t count = 65536;
    for (size_t i = 0; i < count; ++i) ASSERT_TRUE(index.insert(i, i));
    EXPECT_GT(Map::rehashes, 64u);
    EXPECT_LT(Map::largest_rehash, 2048u);
    EXPECT_EQ(index.size(), count);
    for (size_t i = 0; i < count; ++i) {
        ASSERT_NE(index.find(i), nullptr);
        EXPECT_EQ(*index.find(i), i);
    }
}

TYPED_TEST(ShardedMapTest, RoutingDoesNotConcentrateInnerBuckets) {
    using Map = ReservedMap<typename TypeParam::Numbers>;
    Map::max_bucket_size = 0;
    ShardedMap<size_t, int, 64, Map> index;
    constexpr size_t count = 65536;
    for (size_t i = 0; i < count; ++i) ASSERT_TRUE(index.insert(i, i));
    CountingEqual::calls = 0;
    for (size_t i = 0; i < count; ++i) {
        ASSERT_NE(index.find(i), nullptr);
        EXPECT_EQ(*index.find(i), i);
    }
    EXPECT_LT(CountingEqual::calls, count * 8);
    EXPECT_LT(Map::max_bucket_size, 16u);
}

TYPED_TEST(ShardedMapTest, ConditionalEraseAndClear) {
    ShardedMap<std::string, int, 16, typename TypeParam::Strings> index;
    for (int round = 0; round < 2; ++round) {
        ASSERT_TRUE(index.insert("", 7));
        EXPECT_FALSE(index.insert("", 9));
        EXPECT_EQ(*index.find(""), 7);
        EXPECT_FALSE(index.erase_if("", [](int v) { return v == 9; }));
        EXPECT_THROW(
            index.erase_if(
                "", [](int) -> bool { throw std::runtime_error("predicate"); }),
            std::runtime_error);
        EXPECT_EQ(index.size(), 1u);
        EXPECT_EQ(*index.find(""), 7);
        EXPECT_TRUE(index.erase_if("", [](int v) { return v == 7; }));
        EXPECT_FALSE(index.erase(""));
        for (int i = 0; i < 4097; ++i) index.insert(std::to_string(i), i);
        index.clear();
        EXPECT_EQ(index.size(), 0u);
        EXPECT_EQ(index.find("0"), nullptr);
    }
}

TYPED_TEST(ShardedMapTest, ErasePreservesOtherKeys) {
    ShardedMap<size_t, int, 16, typename TypeParam::Collisions> index;
    for (int i = 0; i < 1024; ++i) ASSERT_TRUE(index.insert(i, i));
    for (int i : {0, 512, 1023}) ASSERT_TRUE(index.erase(i));
    for (int i = 0; i < 1024; ++i) {
        if (i == 0 || i == 512 || i == 1023) continue;
        ASSERT_NE(index.find(i), nullptr);
        EXPECT_EQ(*index.find(i), i);
        ASSERT_TRUE(index.erase(i));
    }
    EXPECT_EQ(index.size(), 0u);
}
}  // namespace
}  // namespace mooncake::test
