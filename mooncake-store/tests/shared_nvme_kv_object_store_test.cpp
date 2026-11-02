#include "shared_nvme_kv_backend.h"

#include <map>
#include <mutex>
#include <stdexcept>

#include <gtest/gtest.h>

#include "nvme_kv/connector_test_peer.h"

namespace mooncake::test {
namespace {

class MemoryExecutor final : public NvmeKvCommandExecutor {
   public:
    tl::expected<void, ErrorCode> Store(const PhysicalKey& key,
                                        std::string value) override {
        std::lock_guard lock(mutex);
        ++stores;
        if (fail_store && key == *fail_store) {
            if (commit_on_failure) {
                values.emplace(key, std::move(value));
            }
            if (throw_on_failure) {
                throw std::runtime_error("injected uncertain Store exception");
            }
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        if (!values.emplace(key, std::move(value)).second) {
            if (disappear_on_exists) {
                values.erase(key);
            }
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
        return {};
    }

    tl::expected<std::string, ErrorCode> Retrieve(
        const PhysicalKey& key, uint32_t hint = 0) const override {
        std::lock_guard lock(mutex);
        ++retrieves;
        if (read_error != ErrorCode::OK) {
            return tl::make_unexpected(read_error);
        }

        const auto it = values.find(key);
        if (it == values.end()) {
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        if (hint && it->second.size() > hint) {
            return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
        }
        return inflate_read_size ? it->second + "x" : it->second;
    }

    tl::expected<void, ErrorCode> Delete(const PhysicalKey& key) override {
        std::lock_guard lock(mutex);
        ++deletes;
        values.erase(key);
        return {};
    }

    const Capabilities& GetCapabilities() const override { return caps; }

    Capabilities caps{.effective_max_value_size = 4096, .queue_depth = 1};
    mutable std::mutex mutex;
    std::map<PhysicalKey, std::string> values;
    std::optional<PhysicalKey> fail_store;
    bool commit_on_failure = false;
    bool throw_on_failure = false;
    bool disappear_on_exists = false;
    bool inflate_read_size = false;
    size_t stores = 0, deletes = 0;
    mutable size_t retrieves = 0;
    ErrorCode read_error = ErrorCode::OK;
};

NvmeKvObjectKey Fixed(std::string key, uint64_t allocation_id) {
    return {std::move(key), allocation_id};
}

class SharedNvmeKvObjectStoreTest : public ::testing::TestWithParam<uint32_t> {
   protected:
    SharedNvmeKvObjectStoreTest() {
        auto executor = std::make_unique<MemoryExecutor>();
        executor->caps.queue_depth = GetParam();
        device = executor.get();
        auto connector = std::make_unique<NvmeKvConnector>();
        NvmeKvConnectorTestPeer::SetExecutor(*connector, std::move(executor));
        store = SharedNvmeKvBackend::CreateObjectStore(std::move(connector));
    }

    auto Put(const NvmeKvObjectKey& key, std::string_view value) {
        Slice slice{const_cast<char*>(value.data()), value.size()};
        NvmeKvObjectStore::PutRequest request{key, {&slice, 1}};
        store->PutBatch(std::span(&request, 1));
        return request.result;
    }

    auto Get(const NvmeKvObjectKey& key, std::string& value) {
        Slice slice{value.data(), value.size()};
        NvmeKvObjectStore::GetRequest request{key, value.size(), slice};
        store->GetBatch(std::span(&request, 1));
        return request.result;
    }

    MemoryExecutor* device;
    std::shared_ptr<NvmeKvObjectStore> store;
};

TEST_P(SharedNvmeKvObjectStoreTest,
       FixedAddressesDoNotProbeOrDeleteConflictingValues) {
    ASSERT_TRUE(Put(Fixed("a", 1), "payload"));
    auto retry = Put(Fixed("a", 1), "payload");
    ASSERT_TRUE(retry);
    EXPECT_FALSE(retry->inserted);
    auto conflict = Put(Fixed("a", 1), "changed");
    ASSERT_FALSE(conflict);
    EXPECT_EQ(conflict.error(), ErrorCode::INVALID_WRITE);
    auto different_identity = Put(Fixed("b", 1), "payload");
    ASSERT_FALSE(different_identity);
    EXPECT_EQ(different_identity.error(), ErrorCode::INVALID_WRITE);
    EXPECT_EQ(device->retrieves, 3u);
    EXPECT_EQ(device->stores, 4u);
    EXPECT_EQ(device->values.size(), 1u);
    EXPECT_EQ(device->deletes, 0u);
}

TEST_P(SharedNvmeKvObjectStoreTest,
       FixedStoreFailureIsNotAutomaticallyRetried) {
    auto key = Fixed("key", 1);
    device->fail_store = EncodeSharedNvmeKvPhysicalKey(*key.allocation_id);
    device->commit_on_failure = true;
    auto result = Put(key, "payload");
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::FILE_WRITE_FAIL);
    EXPECT_EQ(device->stores, 1u);
    EXPECT_EQ(device->retrieves, 0u);
    EXPECT_EQ(device->values.size(), 1u);
    EXPECT_EQ(device->deletes, 0u);
}

TEST_P(SharedNvmeKvObjectStoreTest,
       MissingValueAfterKeyExistsRemainsRetryable) {
    const auto key = Fixed("key", 1);
    ASSERT_TRUE(Put(key, "payload"));
    device->disappear_on_exists = true;
    auto result = Put(key, "payload");
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_REPLICA_BUSY);
    EXPECT_EQ(device->stores, 2u);
    EXPECT_EQ(device->deletes, 0u);
}

TEST_P(SharedNvmeKvObjectStoreTest,
       CorruptionAndAddressMismatchFailBeforeCopy) {
    const auto key = Fixed("key", 1);
    ASSERT_TRUE(Put(key, "data"));
    const auto physical = EncodeSharedNvmeKvPhysicalKey(*key.allocation_id);
    device->values[EncodeSharedNvmeKvPhysicalKey(2)] = device->values[physical];
    std::string output(4, 'x');
    auto misplaced = Get(Fixed("key", 2), output);
    ASSERT_FALSE(misplaced);
    EXPECT_EQ(misplaced.error(), ErrorCode::INVALID_KEY);
    EXPECT_EQ(output, "xxxx");
    device->values[physical].back() ^= 1;
    auto corrupt = Get(key, output);
    ASSERT_FALSE(corrupt);
    EXPECT_EQ(corrupt.error(), ErrorCode::CHECKSUM_MISMATCH);
    EXPECT_EQ(output, "xxxx");
    auto conflict = Put(key, "data");
    ASSERT_FALSE(conflict);
    EXPECT_EQ(conflict.error(), ErrorCode::INVALID_WRITE);
    EXPECT_EQ(device->deletes, 0u);
}

TEST_P(SharedNvmeKvObjectStoreTest,
       OversizedInlineObjectsAreRejectedBeforeStore) {
    std::string payload(9000, 'v');
    EXPECT_FALSE(Put(Fixed("fixed", 1), payload));
    EXPECT_TRUE(device->values.empty());
}

TEST_P(SharedNvmeKvObjectStoreTest,
       BatchReadsPreserveSuccessWhenAnotherReadFails) {
    ASSERT_TRUE(Put(Fixed("short", 1), "data"));
    ASSERT_TRUE(Put(Fixed("good", 2), "payload8"));
    std::string short_output(8, 'x');
    std::string output(8, 'y');
    Slice short_slice{short_output.data(), short_output.size()};
    Slice output_slice{output.data(), output.size()};
    std::vector<NvmeKvObjectStore::GetRequest> requests{
        {Fixed("short", 1), 8, short_slice},
        {Fixed("good", 2), 8, output_slice}};
    store->GetBatch(requests);
    ASSERT_FALSE(requests[0].result);
    EXPECT_EQ(requests[0].result.error(), ErrorCode::CHECKSUM_MISMATCH);
    EXPECT_EQ(short_output, "xxxxxxxx");
    ASSERT_TRUE(requests[1].result);
    EXPECT_EQ(output, "payload8");
}

TEST_P(SharedNvmeKvObjectStoreTest, RejectsValueBeyondExpectedInlineSize) {
    ASSERT_TRUE(Put(Fixed("key", 1), "data"));
    device->inflate_read_size = true;
    std::string output(4, 'x');
    auto result = Get(Fixed("key", 1), output);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::BUFFER_OVERFLOW);
    EXPECT_EQ(output, "xxxx");
}

TEST_P(SharedNvmeKvObjectStoreTest, BatchWriteFailurePreservesOtherObjects) {
    const auto failed = Fixed("failed", 1);
    device->fail_store = EncodeSharedNvmeKvPhysicalKey(*failed.allocation_id);
    std::string first = "pay", second = "load";
    std::vector<Slice> slices{{first.data(), first.size()},
                              {second.data(), second.size()}};
    std::vector<NvmeKvObjectStore::PutRequest> requests{
        {failed, slices},
        {Fixed("good", 2), slices},
        {Fixed("other", 3), slices}};
    store->PutBatch(requests);
    EXPECT_FALSE(requests[0].result);
    ASSERT_TRUE(requests[1].result);
    ASSERT_TRUE(requests[2].result);
    std::string output(7, '?');
    ASSERT_TRUE(Get(Fixed("good", 2), output));
    EXPECT_EQ(output, "payload");
    EXPECT_EQ(device->deletes, 0u);
}

TEST_P(SharedNvmeKvObjectStoreTest, AbortDoesNotRetryOrDeleteUncertainStore) {
    const auto key = Fixed("failed", 1);
    device->fail_store = EncodeSharedNvmeKvPhysicalKey(*key.allocation_id);
    device->commit_on_failure = true;
    device->throw_on_failure = true;

    auto result = Put(key, "payload");

    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(device->stores, 1u);
    EXPECT_EQ(device->deletes, 0u);
    EXPECT_TRUE(device->values.contains(
        EncodeSharedNvmeKvPhysicalKey(*key.allocation_id)));
}

TEST_P(SharedNvmeKvObjectStoreTest, LargeBatchPreservesEveryResult) {
    constexpr size_t count = 600;
    std::string value = "value";
    Slice input{value.data(), value.size()};
    std::vector<NvmeKvObjectStore::PutRequest> writes;
    std::vector<std::string> output(count, std::string(value.size(), '?'));
    std::vector<Slice> slices;
    slices.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        writes.push_back(
            {Fixed("key-" + std::to_string(i), i + 1), {&input, 1}});
        slices.push_back({output[i].data(), output[i].size()});
    }
    store->PutBatch(writes);
    std::vector<NvmeKvObjectStore::GetRequest> reads;
    for (size_t i = 0; i < count; ++i) {
        ASSERT_TRUE(writes[i].result);
        reads.push_back({writes[i].key, value.size(), slices[i]});
    }
    store->GetBatch(reads);
    for (size_t i = 0; i < count; ++i) {
        ASSERT_TRUE(reads[i].result);
        EXPECT_EQ(output[i], value);
    }
}

TEST_P(SharedNvmeKvObjectStoreTest, InvalidAllocationIdsAreRejectedBeforeIo) {
    for (const auto& key : {NvmeKvObjectKey{"key"}, Fixed("key", 0)}) {
        auto result = Put(key, "data");
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);

        std::string output(4, 'x');
        auto read = Get(key, output);
        ASSERT_FALSE(read);
        EXPECT_EQ(read.error(), ErrorCode::INVALID_PARAMS);
        EXPECT_EQ(output, "xxxx");

        auto exists = store->IsExist(key);
        ASSERT_FALSE(exists);
        EXPECT_EQ(exists.error(), ErrorCode::INVALID_PARAMS);
    }
    EXPECT_EQ(device->stores, 0u);
    EXPECT_EQ(device->retrieves, 0u);
}

TEST_P(SharedNvmeKvObjectStoreTest, ConfirmationPropagatesRetrieveFailure) {
    const auto key = Fixed("key", 1);
    ASSERT_TRUE(Put(key, "data"));
    device->read_error = ErrorCode::FILE_READ_FAIL;

    auto result = Put(key, "data");

    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::FILE_READ_FAIL);
    EXPECT_EQ(device->stores, 2u);
    EXPECT_EQ(device->retrieves, 1u);
    EXPECT_EQ(device->deletes, 0u);
}

TEST_P(SharedNvmeKvObjectStoreTest, ExistenceChecksPlacementAndInlineType) {
    const auto key = Fixed("key", 1);
    const auto physical = EncodeSharedNvmeKvPhysicalKey(1);
    ASSERT_TRUE(Put(key, "data"));
    EXPECT_TRUE(store->IsExist(key).value_or(false));

    device->values[EncodeSharedNvmeKvPhysicalKey(2)] = device->values[physical];
    auto misplaced = store->IsExist(Fixed("key", 2));
    ASSERT_FALSE(misplaced);
    EXPECT_EQ(misplaced.error(), ErrorCode::INVALID_KEY);
    EXPECT_FALSE(store->IsExist(Fixed("key", 3)).value_or(true));

    std::string payload(9000, 'v');
    auto plan = BuildNvmeKvWritePlan(
        {"key"}, payload, device->caps.effective_max_value_size,
        {physical, 0, [](uint32_t index) {
             return EncodeSharedNvmeKvPhysicalKey(1, index + 1);
         }});
    ASSERT_TRUE(plan);
    ASSERT_FALSE(plan->store_inline);
    device->values[physical] = plan->root_blob;

    auto exists = store->IsExist(key);
    ASSERT_FALSE(exists);
    EXPECT_EQ(exists.error(), ErrorCode::NOT_SUPPORTED);
    std::string output(1024, 'x');
    auto read = Get(key, output);
    ASSERT_FALSE(read);
    EXPECT_EQ(read.error(), ErrorCode::NOT_SUPPORTED);
    EXPECT_EQ(output, std::string(1024, 'x'));
}

TEST_P(SharedNvmeKvObjectStoreTest,
       AllocationsOfTheSameLogicalKeyStaySeparate) {
    ASSERT_TRUE(Put(Fixed("key", 1), "old"));
    ASSERT_TRUE(Put(Fixed("key", 2), "new"));
    std::string first(3, '?'), second(3, '?');
    ASSERT_TRUE(Get(Fixed("key", 1), first));
    ASSERT_TRUE(Get(Fixed("key", 2), second));
    EXPECT_EQ(first, "old");
    EXPECT_EQ(second, "new");

    for (const auto& [physical, blob] : device->values) {
        NvmeKvObjectHeader header;
        std::string_view identity, payload;
        ASSERT_TRUE(ParseNvmeKvObjectBlob(blob, header, identity, payload));
        EXPECT_EQ(header.object_type,
                  static_cast<uint32_t>(NvmeKvObjectType::kInline));
    }
}

INSTANTIATE_TEST_SUITE_P(QueueDepth, SharedNvmeKvObjectStoreTest,
                         ::testing::Values(1u, 8u));

}  // namespace
}  // namespace mooncake::test
