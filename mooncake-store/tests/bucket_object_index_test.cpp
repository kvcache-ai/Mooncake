#include "bucket_object_index.h"

#include <gtest/gtest.h>

#include <charconv>
#include <stdexcept>
#include <utility>
#include <vector>

#include "random.h"

namespace mooncake::test {
struct BucketObjectIndexTestPeer {
    template <typename Hash>
    static auto Segments(const BucketObjectIndex<Hash>& index) {
        std::vector<std::pair<const void*, std::vector<const void*>>> result;
        for (const auto& segment : index.segments_) {
            std::vector<const void*> heads;
            for (auto head : segment.heads)
                heads.push_back(head == index.end_ ? nullptr : &*head);
            result.emplace_back(&segment, std::move(heads));
        }
        return result;
    }
    template <typename Hash>
    static void CheckBucketDistribution(BucketObjectIndex<Hash>& index) {
        size_t total = 0;
        for (const auto& segment : index.segments_) {
            for (size_t bucket = 0; bucket < segment.heads.size(); ++bucket) {
                size_t count = 0;
                for (auto it = segment.heads[bucket]; it != index.end_;
                     it = it->next) {
                    ASSERT_EQ(it->hash, Hash{}(it->key));
                    ASSERT_EQ(it->hash & (segment.heads.size() - 1), bucket);
                    ASSERT_LE(++count, 32u);
                    ++total;
                }
            }
        }
        EXPECT_EQ(total, index.size());
    }
};

namespace {
StorageObjectMetadata Metadata(int64_t id) {
    return StorageObjectMetadata{id, id, 8, 16, ""};
}

struct CountingHash {
    static inline size_t calls = 0;
    size_t operator()(const std::string& key) const noexcept {
        ++calls;
        size_t value = 0;
        std::from_chars(key.data(), key.data() + key.size(), value);
        return value;
    }
};

struct CollidingHash {
    size_t operator()(const std::string&) const noexcept { return 0; }
};
}  // namespace

TEST(BucketObjectIndexTest, GrowthDoesNotRehashTheFullIndex) {
    BucketObjectIndex<CountingHash> index;
    constexpr size_t count = 256 * 1024;
    for (size_t i = 0; i < count; ++i) {
        ASSERT_TRUE(index.insert(std::to_string(i), Metadata(i)));
    }
    std::vector<const StorageObjectMetadata*> pointers;
    for (size_t i = 0; i < count; ++i) {
        pointers.push_back(index.find(std::to_string(i)));
    }
    CountingHash::calls = 0;
    const auto before = BucketObjectIndexTestPeer::Segments(index);
    ASSERT_TRUE(index.insert(std::to_string(count), Metadata(count)));
    EXPECT_EQ(CountingHash::calls, 1u);
    const auto after = BucketObjectIndexTestPeer::Segments(index);
    ASSERT_EQ(after.size(), before.size() + 1);
    size_t changed = 0;
    for (size_t i = 0; i < before.size(); ++i) {
        EXPECT_EQ(before[i].first, after[i].first);
        changed += before[i].second != after[i].second;
    }
    EXPECT_LE(changed, 2u);  // Split source plus the new key's segment.
    EXPECT_EQ(index.size(), count + 1);
    for (size_t i = 0; i <= count; ++i) {
        const auto* metadata = index.find(std::to_string(i));
        ASSERT_NE(metadata, nullptr);
        EXPECT_EQ(metadata->bucket_id, i);
        if (i < count) EXPECT_EQ(metadata, pointers[i]);
    }
}

TEST(BucketObjectIndexTest, SegmentRoutingDoesNotConcentrateInnerHashBuckets) {
    BucketObjectIndex<CountingHash> index;
    constexpr size_t count = 256 * 1024;
    for (size_t i = 0; i < count; ++i) {
        ASSERT_TRUE(index.insert(std::to_string(i), Metadata(i)));
    }
    BucketObjectIndexTestPeer::CheckBucketDistribution(index);
    for (size_t i = 0; i < count; ++i) {
        const auto* metadata = index.find(std::to_string(i));
        ASSERT_NE(metadata, nullptr);
        EXPECT_EQ(metadata->bucket_id, i);
        ASSERT_TRUE(index.erase(std::to_string(i)));
    }
    EXPECT_EQ(index.size(), 0);
}

TEST(BucketObjectIndexTest, MutationsMatchReferenceAcrossSplitRounds) {
    BucketObjectIndex<> index;
    std::unordered_map<std::string, int64_t> reference;
    RandomEngine generator(42);
    for (int i = 0; i < 100000; ++i) {
        const auto id = randomIndex(20000, generator);
        const std::string key = std::string("tenant\0", 7) + std::to_string(id);
        switch (randomIndex(3, generator)) {
            case 0:
                EXPECT_EQ(index.insert(key, Metadata(i)),
                          reference.emplace(key, i).second);
                break;
            case 1:
                index.insert_or_assign(key, Metadata(i));
                reference[key] = i;
                break;
            default:
                EXPECT_EQ(index.erase(key), reference.erase(key) != 0);
        }
        ASSERT_EQ(index.size(), reference.size());
        const auto* actual = index.find(key);
        const auto expected = reference.find(key);
        ASSERT_EQ(actual == nullptr, expected == reference.end());
        if (actual) EXPECT_EQ(actual->bucket_id, expected->second);
    }
    for (const auto& [key, value] : reference) {
        ASSERT_NE(index.find(key), nullptr);
        EXPECT_EQ(index.find(key)->bucket_id, value);
    }
    index.clear();
    EXPECT_EQ(index.size(), 0);
    EXPECT_EQ(index.find(""), nullptr);
    EXPECT_TRUE(index.insert("", Metadata(7)));
    EXPECT_FALSE(index.insert("", Metadata(9)));
    EXPECT_EQ(index.find("")->bucket_id, 7);
}

TEST(BucketObjectIndexTest, CollisionsAndClearPreserveContents) {
    BucketObjectIndex<CollidingHash> index;
    for (int round = 0; round < 2; ++round) {
        for (int i = 0; i < 4097; ++i) {
            ASSERT_TRUE(index.insert(std::to_string(i), Metadata(i)));
        }
        // Exercise the head, middle and tail of the same collision chain.
        for (int i : {4096, 2048, 0}) {
            ASSERT_TRUE(index.erase(std::to_string(i)));
            EXPECT_EQ(index.find(std::to_string(i)), nullptr);
        }
        for (int i = 0; i < 4097; ++i) {
            if (i == 0 || i == 2048 || i == 4096) continue;
            ASSERT_NE(index.find(std::to_string(i)), nullptr);
            EXPECT_EQ(index.find(std::to_string(i))->bucket_id, i);
            ASSERT_TRUE(index.erase(std::to_string(i)));
        }
        EXPECT_EQ(index.size(), 0);
        index.clear();
    }
}

TEST(BucketObjectIndexTest, LookupAndConditionalEraseHashOnce) {
    BucketObjectIndex<CountingHash> index;
    ASSERT_TRUE(index.insert("42", Metadata(7)));
    CountingHash::calls = 0;
    ASSERT_EQ(index.find("42")->bucket_id, 7);
    EXPECT_EQ(CountingHash::calls, 1u);
    CountingHash::calls = 0;
    EXPECT_FALSE(
        index.erase_if("42", [](const auto& m) { return m.bucket_id == 8; }));
    EXPECT_EQ(CountingHash::calls, 1u);
    EXPECT_EQ(index.size(), 1u);
    EXPECT_THROW(index.erase_if("42",
                                [](const auto&) -> bool {
                                    throw std::runtime_error("predicate");
                                }),
                 std::runtime_error);
    EXPECT_EQ(index.size(), 1u);
    CountingHash::calls = 0;
    EXPECT_TRUE(
        index.erase_if("42", [](const auto& m) { return m.bucket_id == 7; }));
    EXPECT_EQ(CountingHash::calls, 1u);
    EXPECT_EQ(index.size(), 0u);
    EXPECT_FALSE(index.erase("42"));
}

}  // namespace mooncake::test
