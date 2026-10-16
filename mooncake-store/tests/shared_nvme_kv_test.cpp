#include "shared_nvme_kv.h"
#include "shared_nvme_kv_key.h"

#include <gtest/gtest.h>
#include <map>

namespace mooncake {
namespace {
class MemoryExecutor final : public NvmeKvCommandExecutor {
   public:
    tl::expected<void, ErrorCode> Store(const PhysicalKey& key,
                                        std::string value) override {
        if (!values.emplace(key, std::move(value)).second)
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        return {};
    }
    tl::expected<std::string, ErrorCode> Retrieve(
        const PhysicalKey& key, uint32_t size_hint) const override {
        auto it = values.find(key);
        if (it == values.end())
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        if (it->second.size() > size_hint)
            return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
        return it->second;
    }
    void RetrieveIntoBatch(
        std::vector<RetrieveIntoRequest>& requests) const override {
        for (auto& request : requests) {
            auto value = Retrieve(request.key, request.size);
            if (!value) {
                request.result = tl::make_unexpected(value.error());
                continue;
            }
            std::memcpy(request.data, value->data(), value->size());
            request.result = static_cast<uint32_t>(value->size());
        }
    }
    tl::expected<void, ErrorCode> Delete(const PhysicalKey& key) override {
        values.erase(key);
        return {};
    }
    const Capabilities& GetCapabilities() const override { return caps; }
    Capabilities caps;
    std::map<PhysicalKey, std::string> values;
};

class SharedNvmeKvTest : public ::testing::Test {
   protected:
    SharedNvmeKvTest() {
        auto executor = std::make_unique<MemoryExecutor>();
        device = executor.get();
        backend = std::make_unique<SharedNvmeKv>(std::move(executor));
    }
    MemoryExecutor* device;
    std::unique_ptr<SharedNvmeKv> backend;
};

TEST_F(SharedNvmeKvTest, RecreatedLogicalKeyHasIndependentPhysicalStorage) {
    ASSERT_TRUE(backend->Put("key", 1, std::string("data")));
    ASSERT_TRUE(backend->Put("key", 2, std::string("data")));
    EXPECT_EQ(device->values.size(), 2);
    ASSERT_TRUE(device->Delete(EncodeSharedNvmeKvPhysicalKey(1)));
    std::string output(4, '\0');
    EXPECT_FALSE(backend->Get("key", 1, output.data(), output.size()));
    EXPECT_TRUE(backend->Get("key", 2, output.data(), output.size()));
    EXPECT_EQ(output, "data");
}

TEST_F(SharedNvmeKvTest, RejectsWrongIdentityAndMisplacedAllocation) {
    ASSERT_TRUE(backend->Put("key", 1, std::string("data")));
    std::string output(4, 'x');
    EXPECT_FALSE(backend->Get("bad", 1, output.data(), output.size()));
    EXPECT_EQ(output, "xxxx");
    device->values[EncodeSharedNvmeKvPhysicalKey(2)] =
        device->values.at(EncodeSharedNvmeKvPhysicalKey(1));
    auto read = backend->Get("key", 2, output.data(), output.size());
    ASSERT_FALSE(read);
    EXPECT_EQ(static_cast<int>(read.error()),
              static_cast<int>(ErrorCode::INVALID_KEY));
    EXPECT_EQ(output, "xxxx");
}

TEST_F(SharedNvmeKvTest, RetryChecksExistingValueAndDoesNotDeleteOnFailure) {
    ASSERT_TRUE(backend->Put("key", 1, std::string("data")));
    ASSERT_TRUE(backend->Put("key", 1, std::string("data")));
    auto& value = device->values.at(EncodeSharedNvmeKvPhysicalKey(1));
    value.back() ^= 1;
    auto retry = backend->Put("key", 1, std::string("data"));
    ASSERT_FALSE(retry);
    EXPECT_EQ(static_cast<int>(retry.error()),
              static_cast<int>(ErrorCode::CHECKSUM_MISMATCH));
    EXPECT_EQ(device->values.size(), 1);
}

TEST_F(SharedNvmeKvTest, RejectsShortValueBeforeCopyingToOutput) {
    ASSERT_TRUE(backend->Put("key", 1, std::string("data")));
    std::string output(8, 'x');
    auto result = backend->Get("key", 1, output.data(), output.size());
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::CHECKSUM_MISMATCH);
    EXPECT_EQ(output, "xxxxxxxx");
}

TEST_F(SharedNvmeKvTest, ShortValueDoesNotPreventOtherBatchReads) {
    ASSERT_TRUE(backend->Put("short", 1, std::string("data")));
    ASSERT_TRUE(backend->Put("exact", 2, std::string("payload8")));
    std::string short_output(8, 'x'), exact_output(8, 'y');
    std::vector<SharedNvmeKv::ReadRequest> reads{
        {"short", 1, {short_output.data(), short_output.size()}},
        {"exact", 2, {exact_output.data(), exact_output.size()}}};
    backend->GetBatch(reads);
    ASSERT_FALSE(reads[0].result);
    EXPECT_EQ(reads[0].result.error(), ErrorCode::CHECKSUM_MISMATCH);
    EXPECT_EQ(short_output, "xxxxxxxx");
    ASSERT_TRUE(reads[1].result);
    EXPECT_EQ(*reads[1].result, exact_output.size());
    EXPECT_EQ(exact_output, "payload8");
}

TEST_F(SharedNvmeKvTest, EnforcesEncodedLimitWithoutAlignmentOrZeroId) {
    auto size = backend->EncodedSize("key", 5);
    ASSERT_TRUE(size);
    device->caps.effective_max_value_size = *size;
    EXPECT_TRUE(backend->Put("key", 1, std::string("12345")));
    EXPECT_EQ(device->values.begin()->second.size(), *size);
    EXPECT_FALSE(backend->Put("key", 2, std::string("123456")));
    EXPECT_FALSE(backend->Put("key", 0, std::string("12345")));
    EXPECT_EQ(device->values.size(), 1);
}

TEST_F(SharedNvmeKvTest, BatchFailureDoesNotCancelOtherObjects) {
    const std::string data = "data";
    std::vector<SharedNvmeKv::WriteRequest> writes{
        {"a", 1, data}, {"b", 0, data}, {"c", 3, data}};
    backend->PutBatch(writes);
    EXPECT_TRUE(writes[0].result);
    EXPECT_FALSE(writes[1].result);
    EXPECT_TRUE(writes[2].result);
    EXPECT_EQ(device->values.size(), 2);
}
}  // namespace
}  // namespace mooncake
