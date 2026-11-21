#include "nvme_kv/backend.h"

#include <chrono>
#include <condition_variable>
#include <future>
#include <map>
#include <mutex>
#include <stdexcept>

#include <gtest/gtest.h>

#include "connector_test_peer.h"

namespace mooncake::test {
namespace {

class MemoryExecutor final : public NvmeKvCommandExecutor {
   public:
    tl::expected<void, ErrorCode> Store(const PhysicalKey& key,
                                        std::string value,
                                        StoreMode mode) override {
        std::lock_guard lock(mutex);
        ++stores;
        if (throw_store && key == *throw_store) {
            throw std::runtime_error("injected Store exception");
        }
        if (fail_store && key == *fail_store) {
            if (++failed_stores > 1 && throw_on_retry) {
                throw std::runtime_error("injected recovery failure");
            }
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        if (mode == StoreMode::kUpdate) {
            auto it = values.find(key);
            if (it == values.end())
                return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            it->second = std::move(value);
            if (lose_update_completion)
                return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        } else {
            if (!values.emplace(key, std::move(value)).second)
                return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
            if (lose_insert_completion == key)
                return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        return {};
    }

    tl::expected<std::string, ErrorCode> Retrieve(const PhysicalKey& key,
                                                  uint32_t = 0) const override {
        std::unique_lock lock(mutex);
        ++retrieves[key];
        ++active_reads;
        max_active_reads = std::max(max_active_reads, active_reads);
        if (wait_for_parallel) {
            if (active_reads >= 2) {
                parallel_observed = true;
                ready.notify_all();
            }
            ready.wait_for(lock, std::chrono::seconds(2),
                           [&] { return parallel_observed; });
        }
        --active_reads;
        const auto it = values.find(key);
        if (it == values.end())
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        return it->second;
    }

    tl::expected<void, ErrorCode> Delete(const PhysicalKey& key) override {
        std::lock_guard lock(mutex);
        ++deletes;
        if (throw_delete && key == *throw_delete) {
            throw std::runtime_error("injected Delete exception");
        }
        values.erase(key);
        return {};
    }

    const Capabilities& GetCapabilities() const override { return caps; }
    Capabilities caps{.effective_max_value_size = 4096, .queue_depth = 1};
    mutable std::mutex mutex;
    mutable std::condition_variable ready;
    mutable std::map<PhysicalKey, size_t> retrieves;
    mutable size_t active_reads = 0, max_active_reads = 0;
    mutable bool parallel_observed = false;
    bool wait_for_parallel = false;
    std::map<PhysicalKey, std::string> values;
    std::optional<PhysicalKey> fail_store;
    std::optional<PhysicalKey> throw_store;
    std::optional<PhysicalKey> throw_delete;
    bool throw_on_retry = false;
    bool lose_update_completion = false;
    std::optional<PhysicalKey> lose_insert_completion;
    size_t failed_stores = 0;
    size_t stores = 0, deletes = 0;
};

class NvmeKvObjectStoreTest : public ::testing::TestWithParam<uint32_t> {
   protected:
    NvmeKvObjectStoreTest() { NewStore(); }

    void NewStore() {
        auto executor = std::make_unique<MemoryExecutor>();
        executor->caps.queue_depth = GetParam();
        if (store) executor->values = device->values;
        device = executor.get();
        auto connector = std::make_unique<NvmeKvConnector>();
        NvmeKvConnectorTestPeer::SetExecutor(*connector, std::move(executor));
        store = NvmeKvStorageBackend::CreateObjectStore(std::move(connector));
    }

    auto Put(const std::string& key, std::string_view value) {
        Slice slice{const_cast<char*>(value.data()), value.size()};
        NvmeKvObjectStore::PutRequest request{{key}, {&slice, 1}};
        store->PutBatch(std::span(&request, 1));
        return request.result;
    }

    auto Get(const std::string& key, std::string& value) {
        Slice slice{value.data(), value.size()};
        NvmeKvObjectStore::GetRequest request{{key}, value.size(), slice};
        store->GetBatch(std::span(&request, 1));
        return request.result;
    }

    MemoryExecutor* device = nullptr;
    std::unique_ptr<NvmeKvObjectStore> store;
};

TEST_P(NvmeKvObjectStoreTest, LocalConflictAndChunkedLayoutRemainReadable) {
    ASSERT_TRUE(Put("other", "original"));
    const auto other = EncodeNvmeKvPhysicalKey({"other"});
    const auto first_slot = EncodeNvmeKvPhysicalKey({"target"});
    device->values[first_slot] = device->values[other];

    const std::string payload(9000, 'v');
    ASSERT_TRUE(Put("target", payload));
    EXPECT_TRUE(
        device->values.contains(EncodeNvmeKvPhysicalKey({"target"}, 1)));
    EXPECT_EQ(device->values[first_slot], device->values[other]);
    NewStore();  // Read through the manifest rather than the writer's cache.
    std::string output(payload.size(), '?');
    ASSERT_TRUE(Get("target", output));
    EXPECT_EQ(output, payload);
    const std::string replacement(payload.size(), 'w');
    ASSERT_TRUE(Put("target", replacement).value_or(NvmeKvPutResult{}).stored);
    EXPECT_EQ(device->values[first_slot], device->values[other]);
    NewStore();
    ASSERT_TRUE(Get("target", output));
    EXPECT_EQ(output, replacement);
}

TEST_P(NvmeKvObjectStoreTest, SameKeyRewritesSurviveReopen) {
    for (const size_t size : {4u, 9000u}) {
        const std::string key = "rewrite-" + std::to_string(size);
        for (char fill : {'a', 'b', 'a', 'a'}) {
            const std::string value(size, fill);
            auto result = Put(key, value);
            ASSERT_TRUE(result && result->stored);
            for (bool reopen : {false, true}) {
                if (reopen) NewStore();
                std::string output(size, '?');
                ASSERT_TRUE(Get(key, output));
                EXPECT_TRUE(output == value)
                    << key << " expected " << fill << ", reopen=" << reopen;
            }
        }
    }
}

TEST_P(NvmeKvObjectStoreTest, InlineRewriteSupersedesCachedManifest) {
    const std::string large(9000, 'a');
    ASSERT_TRUE(Put("rewrite", large).value_or(NvmeKvPutResult{}).stored);
    ASSERT_TRUE(Put("rewrite", "tiny").value_or(NvmeKvPutResult{}).stored);
    std::string old_size(large.size(), '?');
    EXPECT_FALSE(Get("rewrite", old_size));
    NewStore();
    std::string output(4, '?');
    ASSERT_TRUE(Get("rewrite", output));
    EXPECT_EQ(output, "tiny");
}

TEST_P(NvmeKvObjectStoreTest, FailedRewriteKeepsPreviousRootAndCanBeRetried) {
    const std::string first(9000, 'a'), second(9000, 'b');
    ASSERT_TRUE(Put("key", first).value_or(NvmeKvPutResult{}).stored);
    device->fail_store = EncodeNvmeKvPhysicalKey({"key"});
    ASSERT_FALSE(Put("key", second));
    NewStore();
    std::string output(first.size(), '?');
    ASSERT_TRUE(Get("key", output));
    EXPECT_EQ(output, first);
    ASSERT_TRUE(Put("key", second).value_or(NvmeKvPutResult{}).stored);
    NewStore();
    ASSERT_TRUE(Get("key", output));
    EXPECT_EQ(output, second);
}

TEST_P(NvmeKvObjectStoreTest, LostRootUpdateCompletionKeepsPublishedChunks) {
    const std::string first(9000, 'a'), second(9000, 'b');
    ASSERT_TRUE(Put("key", first).value_or(NvmeKvPutResult{}).stored);
    device->lose_update_completion = true;
    ASSERT_FALSE(Put("key", second));
    NewStore();
    std::string output(second.size(), '?');
    ASSERT_TRUE(Get("key", output));
    EXPECT_EQ(output, second);
    auto retry = Put("key", second);
    ASSERT_TRUE(retry && retry->stored);
    EXPECT_FALSE(retry->inserted);
}

TEST_P(NvmeKvObjectStoreTest, RewritesAccountForNewPayloadStorage) {
    auto result = Put("key", "a");
    ASSERT_TRUE(result && result->stored && result->inserted);
    EXPECT_EQ(result->inline_bytes, 1u);
    EXPECT_EQ(result->new_chunk_bytes, 0u);
    result = Put("key", "longer");
    ASSERT_TRUE(result && result->stored && !result->inserted);
    EXPECT_EQ(result->inline_bytes, 6u);
    const std::string value(9000, 'x');
    result = Put("key", value);
    ASSERT_TRUE(result && result->stored && !result->inserted);
    EXPECT_EQ(result->inline_bytes, 0u);
    EXPECT_EQ(result->new_chunk_bytes, value.size());
    result = Put("key", value);
    ASSERT_TRUE(result && result->stored && !result->inserted);
    EXPECT_EQ(result->new_chunk_bytes, 0u);
}

TEST_P(NvmeKvObjectStoreTest, ConcurrentRewritesPublishOneCompleteValue) {
    const std::string original(9000, 'a'), left(9000, 'b'), right(9000, 'c');
    ASSERT_TRUE(Put("key", original).value_or(NvmeKvPutResult{}).stored);
    auto writer =
        std::async(std::launch::async, [&] { return Put("key", left); });
    auto second = Put("key", right);
    auto first = writer.get();
    ASSERT_TRUE(first && first->stored);
    ASSERT_TRUE(second && second->stored);
    std::string warm(original.size(), '?'), cold(original.size(), '?');
    ASSERT_TRUE(Get("key", warm));
    EXPECT_TRUE(warm == left || warm == right);
    NewStore();
    ASSERT_TRUE(Get("key", cold));
    EXPECT_EQ(warm, cold);
}

TEST_P(NvmeKvObjectStoreTest, PublishedRewriteSurvivesAnotherWritersRollback) {
    const std::string key = "rewrite";
    const std::string value(9000, 'x');
    ASSERT_TRUE(Put(key, "old").value_or(NvmeKvPutResult{}).stored);

    // Pause one writer after staging its first chunk, before publishing a root.
    const auto chunk = EncodeNvmeKvChunkPhysicalKey({key}, 0);
    ASSERT_TRUE(device->Store(
        chunk, value.substr(0, device->caps.effective_max_value_size),
        NvmeKvCommandExecutor::StoreMode::kInsert));
    ASSERT_TRUE(Put(key, value).value_or(NvmeKvPutResult{}).stored);
    std::string output(value.size(), '?');
    ASSERT_TRUE(Get(key, output));
    ASSERT_EQ(output, value);

    // The first writer still owns the speculative chunk and may roll it back.
    ASSERT_TRUE(device->Delete(chunk));
    ASSERT_TRUE(Get(key, output));
    EXPECT_EQ(output, value);
    NewStore();
    ASSERT_TRUE(Get(key, output));
    EXPECT_EQ(output, value);
}

TEST_P(NvmeKvObjectStoreTest, LostInsertCompletionPreservesPublishedChunks) {
    const std::string key = "lost-root";
    const std::string value(9000, 'x');
    device->lose_insert_completion = EncodeNvmeKvPhysicalKey({key});
    ASSERT_TRUE(Put(key, value).value_or(NvmeKvPutResult{}).stored);
    EXPECT_EQ(device->deletes, 0u);
    NewStore();
    std::string output(value.size(), '?');
    ASSERT_TRUE(Get(key, output));
    EXPECT_EQ(output, value);
}

TEST_P(NvmeKvObjectStoreTest, FailedManifestRetentionCanBeRetried) {
    const std::string value(9000, 'x');
    for (bool applied : {false, true}) {
        const std::string key = applied ? "applied" : "rejected";
        ASSERT_TRUE(Put(key, "old").value_or(NvmeKvPutResult{}).stored);
        auto plan = BuildNvmeKvWritePlan(
            {key}, value, device->caps.effective_max_value_size,
            {EncodeNvmeKvPhysicalKey({key}), 0, [&](uint32_t index) {
                 return EncodeNvmeKvChunkPhysicalKey({key}, index);
             }});
        ASSERT_TRUE(plan);
        const auto record =
            EncodeNvmeKvRetainedManifestKey({key}, plan->root_blob);
        if (applied) {
            device->lose_insert_completion = record;
        } else {
            device->fail_store = record;
        }
        Slice slice{const_cast<char*>(value.data()), value.size()};
        NvmeKvObjectStore::PutRequest request{{key}, {&slice, 1}};
        store->PutBatch(std::span(&request, 1));
        ASSERT_FALSE(request.result);
        EXPECT_EQ(request.retained_on_failure, applied ? value.size() : 0u);
        std::string old(3, '?');
        ASSERT_TRUE(Get(key, old));
        EXPECT_EQ(old, "old");
        for (const auto& chunk : plan->manifest_records) {
            EXPECT_EQ(device->values.contains(chunk.physical_key), applied);
        }

        NewStore();
        auto retry = Put(key, value);
        ASSERT_TRUE(retry && retry->stored);
        EXPECT_EQ(retry->new_chunk_bytes, applied ? 0u : value.size());
        NewStore();
        std::string output(value.size(), '?');
        ASSERT_TRUE(Get(key, output));
        EXPECT_EQ(output, value);
    }
}

TEST_P(NvmeKvObjectStoreTest, ReopenedWriterReusesRetiredChunkSets) {
    const std::string key = "rewrite";
    const std::string first(9000, 'a'), second(9000, 'b');
    ASSERT_TRUE(Put(key, first).value_or(NvmeKvPutResult{}).stored);
    ASSERT_TRUE(Put(key, second).value_or(NvmeKvPutResult{}).stored);
    for (const auto& value : {first, second}) {
        NewStore();
        auto restored = Put(key, value);
        ASSERT_TRUE(restored && restored->stored);
        EXPECT_EQ(restored->new_chunk_bytes, 0u);
        std::string output(value.size(), '?');
        ASSERT_TRUE(Get(key, output));
        EXPECT_EQ(output, value);
    }
}

TEST_P(NvmeKvObjectStoreTest, RootPlacementIsValidatedForReadsAndExistence) {
    const auto physical = EncodeNvmeKvPhysicalKey({"key"});
    const auto wrong_address = EncodeNvmeKvPhysicalKey({"key"}, 1);
    for (const auto& placement : {NvmeKvWritePlacement{wrong_address, 1, {}},
                                  NvmeKvWritePlacement{physical, 1, {}}}) {
        auto plan = BuildNvmeKvWritePlan(
            {"key"}, "data", device->caps.effective_max_value_size, placement);
        ASSERT_TRUE(plan);
        device->values[physical] = plan->root_blob;

        auto exists = store->IsExist({"key"});
        ASSERT_FALSE(exists);
        EXPECT_EQ(exists.error(), ErrorCode::FILE_READ_FAIL);

        std::string output(4, 'x');
        auto read = Get("key", output);
        ASSERT_FALSE(read);
        EXPECT_EQ(read.error(), ErrorCode::FILE_READ_FAIL);
        EXPECT_EQ(output, "xxxx");
    }
}

TEST_P(NvmeKvObjectStoreTest, RootReadsKeepTheExistingParallelPreparation) {
    ASSERT_TRUE(Put("first", "data"));
    ASSERT_TRUE(Put("second", "data"));
    device->wait_for_parallel = GetParam() > 1;
    std::string first(4, '?'), second(4, '?');
    Slice a{first.data(), first.size()}, b{second.data(), second.size()};
    std::vector<NvmeKvObjectStore::GetRequest> requests{{{"first"}, 4, a},
                                                        {{"second"}, 4, b}};
    store->GetBatch(requests);

    ASSERT_TRUE(requests[0].result);
    ASSERT_TRUE(requests[1].result);
    EXPECT_EQ(first, "data");
    EXPECT_EQ(second, "data");
    EXPECT_EQ(device->max_active_reads, GetParam() > 1 ? 2u : 1u);
}

TEST_P(NvmeKvObjectStoreTest, LocalPreparationFailurePreservesOtherReads) {
    ASSERT_TRUE(Put("chunked", std::string(9000, 'v')));
    NewStore();
    std::string missing(4, '?'), chunked(9000, '?');
    Slice a{missing.data(), missing.size()}, b{chunked.data(), chunked.size()};
    std::vector<NvmeKvObjectStore::GetRequest> requests{
        {{"missing"}, a.size, a},
        {{"invalid"}, 4, {nullptr, 4}},
        {{"chunked"}, b.size, b}};
    store->GetBatch(requests);

    ASSERT_FALSE(requests[0].result);
    EXPECT_EQ(requests[0].result.error(), ErrorCode::OBJECT_NOT_FOUND);
    ASSERT_FALSE(requests[1].result);
    EXPECT_EQ(requests[1].result.error(), ErrorCode::INVALID_PARAMS);
    ASSERT_TRUE(requests[2].result);
    EXPECT_EQ(chunked, std::string(9000, 'v'));
    EXPECT_GT(device->retrieves[EncodeNvmeKvChunkPhysicalKey({"chunked"}, 0)],
              0u);
}

TEST_P(NvmeKvObjectStoreTest, RecoveryCleansAllSuccessfulChunks) {
    ASSERT_TRUE(Put("other", "keep"));
    device->fail_store = EncodeNvmeKvChunkPhysicalKey({"failed"}, 1);

    auto result = Put("failed", std::string(9000, 'v'));

    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::FILE_WRITE_FAIL);
    ASSERT_EQ(device->values.size(), 1u);
    EXPECT_TRUE(device->values.contains(EncodeNvmeKvPhysicalKey({"other"})));
}

TEST_P(NvmeKvObjectStoreTest, ManifestAddressesAreValidatedBeforeChunkReads) {
    ASSERT_TRUE(Put("key", std::string(9000, 'v')));
    const auto physical = EncodeNvmeKvPhysicalKey({"key"});
    NvmeKvObjectHeader header{};
    std::string_view identity, payload;
    ASSERT_TRUE(ParseNvmeKvObjectBlob(device->values[physical], header,
                                      identity, payload));
    NvmeKvManifestMetadata metadata{};
    std::vector<NvmeKvManifestChunkRecord> chunks;
    ASSERT_TRUE(ParseNvmeKvManifest(payload, metadata, chunks));
    chunks.front().physical_key[0] ^= 1;
    const auto invalid_key = chunks.front().physical_key;
    const auto manifest = SerializeNvmeKvManifest(metadata, chunks);
    header.payload_checksum = ComputeNvmeKvPayloadChecksum(manifest);
    header.header_checksum = ComputeNvmeKvHeaderChecksum(header);
    device->values[physical] =
        BuildNvmeKvObjectBlob(header, identity, manifest);
    NewStore();
    std::string output(9000, '?');

    auto result = Get("key", output);

    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::FILE_READ_FAIL);
    EXPECT_EQ(output, std::string(9000, '?'));
    EXPECT_EQ(device->retrieves[invalid_key], 0u);
}

TEST_P(NvmeKvObjectStoreTest, LocalCorruptionDoesNotModifyOutput) {
    ASSERT_TRUE(Put("key", "data"));
    device->values[EncodeNvmeKvPhysicalKey({"key"})].back() ^= 1;
    // Existence still checks root identity without validating the payload.
    auto exists = store->IsExist({"key"});
    ASSERT_TRUE(exists);
    EXPECT_TRUE(*exists);
    std::string output(4, 'x');
    auto result = Get("key", output);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::FILE_READ_FAIL);
    EXPECT_EQ(output, "xxxx");
}

TEST_P(NvmeKvObjectStoreTest, BatchWriteFailurePreservesOtherObjects) {
    device->fail_store = EncodeNvmeKvPhysicalKey({"failed"});
    std::string first = "pay", second = "load";
    std::vector<Slice> slices{{first.data(), first.size()},
                              {second.data(), second.size()}};
    std::vector<NvmeKvObjectStore::PutRequest> requests{{{"failed"}, slices},
                                                        {{"good"}, slices}};
    store->PutBatch(requests);
    EXPECT_FALSE(requests[0].result);
    ASSERT_TRUE(requests[1].result);
    std::string output(7, '?');
    ASSERT_TRUE(Get("good", output));
    EXPECT_EQ(output, "payload");
}

TEST_P(NvmeKvObjectStoreTest, RecoveryExceptionPreservesCompletedWrites) {
    device->fail_store = EncodeNvmeKvPhysicalKey({"failed"});
    device->throw_on_retry = true;
    std::string first = "pay", second = "load";
    std::vector<Slice> slices{{first.data(), first.size()},
                              {second.data(), second.size()}};
    std::vector<NvmeKvObjectStore::PutRequest> requests{{{"failed"}, slices},
                                                        {{"good"}, slices}};

    auto batch_result = store->PutBatch(requests);

    ASSERT_FALSE(batch_result);
    EXPECT_EQ(batch_result.error(), ErrorCode::INTERNAL_ERROR);
    ASSERT_FALSE(requests[0].result);
    EXPECT_EQ(requests[0].result.error(), ErrorCode::INTERNAL_ERROR);
    ASSERT_TRUE(requests[1].result);
    EXPECT_TRUE(requests[1].result->stored);
    EXPECT_TRUE(requests[1].result->inserted);
    EXPECT_EQ(device->failed_stores, 2u);
    EXPECT_EQ(device->values.size(), 1u);
    EXPECT_EQ(device->deletes, 0u);

    std::string output(7, '?');
    ASSERT_TRUE(Get("good", output));
    EXPECT_EQ(output, "payload");
}

TEST_P(NvmeKvObjectStoreTest, AbortCleansChunksAfterRootException) {
    device->throw_store = EncodeNvmeKvPhysicalKey({"failed"});

    auto result = Put("failed", std::string(9000, 'v'));

    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
    EXPECT_TRUE(device->values.empty());
    EXPECT_EQ(device->stores, 4u);
    EXPECT_EQ(device->deletes, 3u);
}

TEST_P(NvmeKvObjectStoreTest, AbortContinuesAfterDeleteException) {
    device->throw_store = EncodeNvmeKvPhysicalKey({"failed"});
    device->throw_delete = EncodeNvmeKvChunkPhysicalKey({"failed"}, 0);

    auto result = Put("failed", std::string(9000, 'v'));

    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(device->deletes, 3u);
    ASSERT_EQ(device->values.size(), 1u);
    EXPECT_TRUE(device->values.contains(*device->throw_delete));
}

TEST(NvmeKvObjectLayoutTest, IdentitySizesMatchSerializedBytes) {
    for (size_t key_size : {0u, 1u, 15u, 16u, 256u, 4096u}) {
        const NvmeKvObjectIdentity identity{std::string(key_size, '\0')};
        const auto canonical = SerializeNvmeKvCanonicalIdentity(identity);
        EXPECT_EQ(ComputeNvmeKvCanonicalIdentitySize(key_size),
                  canonical.size());

        const auto metadata = BuildNvmeKvStoredIdentityMetadata({}, 0);
        const auto stored = SerializeNvmeKvStoredIdentity(identity, metadata);
        EXPECT_EQ(ComputeNvmeKvStoredIdentityMetadataSize(identity),
                  stored.size());
        auto inline_size = ComputeNvmeKvInlineSize(key_size, 7, UINT32_MAX);
        ASSERT_TRUE(inline_size);
        EXPECT_EQ(*inline_size, sizeof(NvmeKvObjectHeader) + stored.size() + 7);
    }
}

TEST(NvmeKvObjectLayoutTest, InlineSizeChecksEncodingAndDeviceLimits) {
    auto size = ComputeNvmeKvInlineSize(3, 7, UINT32_MAX);
    ASSERT_TRUE(size);
    EXPECT_TRUE(ComputeNvmeKvInlineSize(3, 7, *size));
    EXPECT_FALSE(ComputeNvmeKvInlineSize(3, 7, *size - 1));
    EXPECT_FALSE(
        ComputeNvmeKvInlineSize(uint64_t{UINT32_MAX} + 1, 7, UINT32_MAX));
    EXPECT_FALSE(ComputeNvmeKvInlineSize(3, UINT64_MAX, UINT32_MAX));
}

INSTANTIATE_TEST_SUITE_P(QueueDepth, NvmeKvObjectStoreTest,
                         ::testing::Values(1u, 8u));

}  // namespace
}  // namespace mooncake::test
