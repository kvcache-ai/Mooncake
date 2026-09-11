#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <utility>

namespace mooncake::test {
namespace {

class NoFNamespaceQueryTest : public ::testing::Test {
   protected:
    static constexpr size_t kNamespaceSize = 64 * 1024 * 1024;
    inline static const std::string kEndpoint = "namespace-query-test";

    static void SetUpTestSuite() {
        google::InitGoogleLogging("NoFNamespaceQueryTest");
        FLAGS_logtostderr = true;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        auto config = MasterServiceConfig::builder()
                          .set_memory_allocator(BufferAllocatorType::OFFSET)
                          .set_nof_heartbeat_interval_sec(60)
                          .build();
        service_ = std::make_unique<MasterService>(config);
        service_->SetNoFProbeFnForTesting(
            [](const std::string&, uint32_t, std::string*) { return true; });
        service_->SetNoFNamespaceQueryFnForTesting(
            [](const std::string&, NoFNamespaceInfo& info, std::string*) {
                info.block_size = 4096;
                info.num_blocks = kNamespaceSize / info.block_size;
                info.size = kNamespaceSize;
                return true;
            });
    }

    NoFSegment MakeSegment(size_t base, size_t size) {
        NoFSegment segment;
        segment.id = generate_uuid();
        segment.name = kEndpoint;
        segment.te_endpoint = kEndpoint;
        segment.base = base;
        segment.size = size;
        return segment;
    }

    void ExpectOnlySegment(const NoFSegment& expected) {
        auto segments = service_->GetAllNoFSegments();
        ASSERT_TRUE(segments.has_value());
        ASSERT_EQ(segments->size(), 1u);
        const auto& actual = segments->front();
        EXPECT_EQ(actual.id, expected.id);
        EXPECT_EQ(actual.te_endpoint, expected.te_endpoint);
        EXPECT_EQ(actual.base, expected.base);
        EXPECT_EQ(actual.size, expected.size);
    }

    std::unique_ptr<MasterService> service_;
};

TEST_F(NoFNamespaceQueryTest, RegistersFullNamespaceAndRetriesIdempotently) {
    int query_calls = 0;
    service_->SetNoFNamespaceQueryFnForTesting(
        [&query_calls](const std::string& endpoint, NoFNamespaceInfo& info,
                       std::string*) {
            EXPECT_EQ(endpoint, kEndpoint);
            ++query_calls;
            info.size = kNamespaceSize;
            return true;
        });

    ASSERT_TRUE(service_->QueryAndMountNoFSegment(kEndpoint, generate_uuid())
                    .has_value());
    auto segments = service_->GetAllNoFSegments();
    ASSERT_TRUE(segments.has_value());
    ASSERT_EQ(segments->size(), 1u);
    const auto original = segments->front();
    EXPECT_EQ(original.te_endpoint, kEndpoint);
    EXPECT_EQ(original.base, 0u);
    EXPECT_EQ(original.size, kNamespaceSize);

    ASSERT_TRUE(service_->QueryAndMountNoFSegment(kEndpoint, generate_uuid())
                    .has_value());
    EXPECT_EQ(query_calls, 2);
    ExpectOnlySegment(original);
}

TEST_F(NoFNamespaceQueryTest, QueryFailureDoesNotChangeRegistrations) {
    service_->SetNoFNamespaceQueryFnForTesting(
        [](const std::string&, NoFNamespaceInfo&, std::string* reason) {
            *reason = "target unreachable";
            return false;
        });

    auto result = service_->QueryAndMountNoFSegment(kEndpoint, generate_uuid());
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(service_->GetMountedNoFSegmentCountForTesting(), 0u);

    auto original = MakeSegment(0, kNamespaceSize);
    ASSERT_TRUE(
        service_->MountNoFSegment(original, generate_uuid()).has_value());

    result = service_->QueryAndMountNoFSegment(kEndpoint, generate_uuid());
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
    ExpectOnlySegment(original);
}

TEST_F(NoFNamespaceQueryTest, RejectsMismatchWithoutChangingRegistration) {
    const std::pair<size_t, size_t> ranges[] = {{4096, kNamespaceSize},
                                                {0, kNamespaceSize / 2}};
    for (const auto& [base, size] : ranges) {
        SCOPED_TRACE(::testing::Message()
                     << "base=" << base << ", size=" << size);
        const auto client_id = generate_uuid();
        auto original = MakeSegment(base, size);
        ASSERT_TRUE(service_->MountNoFSegment(original, client_id).has_value());

        auto result =
            service_->QueryAndMountNoFSegment(kEndpoint, generate_uuid());
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
        ExpectOnlySegment(original);

        ASSERT_TRUE(
            service_->UnmountNoFSegment(original.id, client_id).has_value());
    }
}

}  // namespace
}  // namespace mooncake::test
