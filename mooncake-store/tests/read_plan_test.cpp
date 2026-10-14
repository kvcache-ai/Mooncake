#include "read_plan.h"
#include "dummy_client.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>

namespace mooncake {
namespace {
using namespace std::chrono_literals;

class ReadPlanTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (const char* value = std::getenv("MOONCAKE_READ_PLAN_PIPELINE"))
            pipeline_ = value;
        unsetenv("MOONCAKE_READ_PLAN_PIPELINE");
    }
    void TearDown() override {
        if (pipeline_)
            setenv("MOONCAKE_READ_PLAN_PIPELINE", pipeline_->c_str(), 1);
        else
            unsetenv("MOONCAKE_READ_PLAN_PIPELINE");
    }

   private:
    std::optional<std::string> pipeline_;
};

struct Backend : DummyClient {
    std::atomic<int> starts{0}, reads{0}, ends{0};
    bool bad_start = false, short_read = false, bad_end = false;
    int block_call = 0;
    std::promise<void> entered, resume;
    std::shared_future<void> gate = resume.get_future().share();
    std::vector<int> batch_get_session_start(
        const std::vector<std::string>& keys) override {
        ++starts;
        return std::vector<int>(keys.size(), bad_start ? -1 : 0);
    }
    std::vector<int> batch_get_into_multi_buffer_ranges(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& buffers,
        const std::vector<std::vector<size_t>>& sizes,
        const std::vector<std::vector<size_t>>& offsets) override {
        int call = ++reads;
        if (call == block_call) {
            entered.set_value();
            if (gate.wait_for(3s) != std::future_status::ready)
                throw std::runtime_error("test gate timed out");
        }
        std::vector<int> out;
        for (size_t i = 0; i < keys.size(); ++i) {
            size_t total = 0;
            for (size_t j = 0; j < buffers[i].size(); ++j) {
                auto dst = static_cast<unsigned char*>(buffers[i][j]);
                for (size_t k = 0; k < sizes[i][j]; ++k)
                    dst[k] = (offsets[i][j] + k + keys[i][0]) % 251;
                total += sizes[i][j];
            }
            out.push_back(int(total) - (short_read ? 1 : 0));
        }
        return out;
    }
    int batch_get_session_end(const std::vector<std::string>&) override {
        ++ends;
        return bad_end ? -1 : 0;
    }
};
std::vector<ReadLayout> layout(unsigned char* dst, std::string key = "a") {
    return {{{key, "b"},
             {1, 3},
             true,
             {{{reinterpret_cast<size_t>(dst), 32, 8, 2}},
              {{reinterpret_cast<size_t>(dst) + 8, 32, 8, 10}}}}};
}

TEST_F(ReadPlanTest, PackedReadsAndOneShotExecution) {
    unsigned char dst[160]{};
    auto b = std::make_shared<Backend>();
    ReadPlan plan(b, layout(dst), 2);
    ASSERT_NO_THROW(plan.run());
    EXPECT_NO_THROW(plan.wait(1));
    for (auto row : {1, 3})
        for (int j = 0; j < 16; ++j)
            EXPECT_EQ(dst[row * 32 + j],
                      (2 + j + (row == 1 ? 'a' : 'b')) % 251);
    EXPECT_EQ(plan.stats(), std::vector<uint64_t>({2, 4, 32}));
    EXPECT_EQ(b->starts.load(), 1);
    EXPECT_EQ(b->ends.load(), 1);
    EXPECT_THROW(plan.run(), std::runtime_error);
}

TEST_F(ReadPlanTest, RejectsOverlappingPlansUntilCleanup) {
    unsigned char dst[160]{};
    auto b = std::make_shared<Backend>();
    b->block_call = 2;
    ReadPlan first(b, layout(dst), 2), overlap(b, layout(dst), 2);
    auto done = std::async(std::launch::async, [&] { first.run(); });
    ASSERT_EQ(b->entered.get_future().wait_for(3s), std::future_status::ready);
    EXPECT_NO_THROW(first.wait(0));
    EXPECT_THROW(first.stats(), std::runtime_error);
    auto last = std::async(std::launch::async, [&] { first.wait(1); });
    EXPECT_EQ(last.wait_for(20ms), std::future_status::timeout);
    EXPECT_THROW(overlap.run(), std::runtime_error);
    EXPECT_THROW(overlap.wait(0), std::runtime_error);
    EXPECT_EQ(b->starts.load(), 1);
    EXPECT_EQ(b->ends.load(), 0);
    b->resume.set_value();
    EXPECT_NO_THROW(done.get());
    EXPECT_NO_THROW(last.get());
    ReadPlan next(b, layout(dst), 2);
    EXPECT_NO_THROW(next.run());
    EXPECT_EQ(b->ends.load(), 2);
}

TEST_F(ReadPlanTest, UnpackedReadsSkipEmptyGroups) {
    unsigned char dst[160]{};
    auto b = std::make_shared<Backend>();
    const auto base = reinterpret_cast<size_t>(dst);
    ReadPlan plan(b,
                  {{{"c", "d", "e", "f"},
                    {0, 2},
                    false,
                    {{{base, 32, 4, 1}, {base + 4, 32, 4, 5}},
                     {},
                     {{base + 8, 32, 4, 9}, {base + 12, 32, 4, 13}}}}},
                  3);
    ASSERT_NO_THROW(plan.run());
    EXPECT_NO_THROW(plan.wait(2));
    for (int row : {0, 2})
        for (int j = 0; j < 16; ++j) {
            const char key = (row == 0 ? 'c' : 'e') + ((j / 4) % 2);
            EXPECT_EQ(dst[row * 32 + j], (key + j + 1) % 251);
        }
    EXPECT_EQ(plan.stats(), std::vector<uint64_t>({2, 8, 32}));
}

TEST_F(ReadPlanTest, CleansUpAfterStartReadAndEndFailures) {
    unsigned char dst[160]{};
    for (int failure = 0; failure < 3; ++failure) {
        SCOPED_TRACE(failure);
        auto b = std::make_shared<Backend>();
        b->bad_start = failure == 0;
        b->short_read = failure == 1;
        b->bad_end = failure == 2;
        ReadPlan plan(b, layout(dst), 2);
        EXPECT_THROW(plan.run(), std::runtime_error);
        EXPECT_THROW(plan.wait(1), std::runtime_error);
        EXPECT_EQ(b->ends.load(), 1);
        EXPECT_TRUE(plan.is_finished());
        // Cleanup failure cannot revoke an already published group.
        if (failure == 2) {
            EXPECT_NO_THROW(plan.wait(0));
        }
        if (failure == 0) {
            EXPECT_EQ(b->reads.load(), 0);
        }
    }
}

TEST_F(ReadPlanTest, RejectsInvalidGroupsAndAddressOverflow) {
    unsigned char dst[160]{};
    auto b = std::make_shared<Backend>();
    EXPECT_THROW(ReadPlan(b, layout(dst), 0), std::invalid_argument);
    auto bad = layout(dst);
    std::get<1>(bad[0]) = {std::numeric_limits<size_t>::max(), 3};
    EXPECT_THROW(ReadPlan(b, bad, 2), std::overflow_error);
    EXPECT_EQ(b->starts.load(), 0);
}

TEST_F(ReadPlanTest, EmptyPlanCompletesWithoutReads) {
    auto b = std::make_shared<Backend>();
    ReadPlan empty(b, {}, 2);
    EXPECT_NO_THROW(empty.run());
    EXPECT_NO_THROW(empty.wait(1));
    EXPECT_EQ(empty.stats(), std::vector<uint64_t>({0, 0, 0}));
}

TEST_F(ReadPlanTest, AcceptsAdjacentRangesAcrossLayouts) {
    unsigned char dst[160]{};
    auto b = std::make_shared<Backend>();
    const auto base = reinterpret_cast<size_t>(dst);
    std::vector<ReadLayout> pools = {
        {{"a"}, {0}, true, {{{base, 0, 8, 0}}, {}}},
        {{"b"}, {0}, true, {{}, {{base + 8, 0, 8, 0}}}}};
    ReadPlan adjacent(b, pools, 2);
    EXPECT_NO_THROW(adjacent.run());
    EXPECT_NO_THROW(adjacent.wait(0));
    EXPECT_NO_THROW(adjacent.wait(1));
}

}  // namespace
}  // namespace mooncake
