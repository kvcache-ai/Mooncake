#include "piece_put_state.h"
#include "random.h"

#include <chrono>
#include <cstdint>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <ylt/struct_pack.hpp>

namespace mooncake {
namespace {

using Clock = PiecePutState::Clock;
using TimePoint = PiecePutState::TimePoint;
using namespace std::chrono_literals;

const UUID kClientA{1, 1};
const UUID kClientB{2, 2};
const TimePoint kNow{100s};
const TimePoint kDeadline = kNow + 10s;

PiecePutStartRequest Request(uint64_t offset, uint64_t length,
                             uint64_t operation_seq, uint64_t total_size = 12) {
    return {.key = "key",
            .total_size = total_size,
            .offset = offset,
            .length = length,
            .write_id = "write-1",
            .operation_seq = operation_seq};
}

TEST(PiecePutStateTest, ValidatesBoundsAndBoundedWriteIdentity) {
    auto request = Request(0, 12, 1);
    EXPECT_TRUE(PiecePutState::ValidateStartRequest(request));

    request.offset = 13;
    EXPECT_FALSE(PiecePutState::ValidateStartRequest(request));
    request = Request(0, 0, 1);
    EXPECT_FALSE(PiecePutState::ValidateStartRequest(request));
    request = Request(0, 12, 1);
    request.write_id.clear();
    EXPECT_FALSE(PiecePutState::ValidateStartRequest(request));
    request.write_id.assign(PiecePutState::kMaxWriteIdBytes + 1, 'x');
    EXPECT_FALSE(PiecePutState::ValidateStartRequest(request));

    const auto maximum = std::numeric_limits<uint64_t>::max();
    request = Request(maximum - 4, 4, 1, maximum);
    EXPECT_TRUE(PiecePutState::ValidateStartRequest(request));
    request.length = 5;
    EXPECT_FALSE(PiecePutState::ValidateStartRequest(request));

    request = Request(0, 12, 1);
    request.token = PiecePutToken{{1, 2}, 3, 4, ""};
    EXPECT_FALSE(PiecePutState::ValidateStartRequest(request));
    request.lookup_only = true;
    EXPECT_TRUE(PiecePutState::ValidateStartRequest(request));
}

TEST(PiecePutStateTest, OutOfOrderRangesPublishOnlyAtFullCoverage) {
    PiecePutState state("write-1", kDeadline);
    const auto middle = state.Claim(Request(4, 5, 1), kClientB, 12, kNow);
    const auto first = state.Claim(Request(0, 4, 1), kClientA, 12, kNow);
    const auto last = state.Claim(Request(9, 3, 2), kClientA, 12, kNow);
    ASSERT_TRUE(middle);
    ASSERT_TRUE(first);
    ASSERT_TRUE(last);
    EXPECT_EQ(middle->slot, 0u);
    EXPECT_EQ(first->slot, 1u);
    EXPECT_EQ(last->slot, 2u);

    auto finish = state.Finish(last->slot, kClientA, 2,
                               PiecePutFinishOutcome::APPLIED, 12, kNow);
    ASSERT_TRUE(finish);
    EXPECT_FALSE(finish->ready_to_publish);
    EXPECT_EQ(state.completed_bytes(), 3u);
    finish = state.Finish(middle->slot, kClientB, 1,
                          PiecePutFinishOutcome::APPLIED, 12, kNow);
    ASSERT_TRUE(finish);
    EXPECT_FALSE(finish->ready_to_publish);
    EXPECT_EQ(state.completed_bytes(), 8u);
    finish = state.Finish(first->slot, kClientA, 1,
                          PiecePutFinishOutcome::APPLIED, 12, kNow);
    ASSERT_TRUE(finish);
    EXPECT_TRUE(finish->ready_to_publish);
    EXPECT_EQ(state.completed_bytes(), 12u);

    // Publication is the service's separate decision. A retry can still
    // drive that decision if the original Finish has not published yet.
    finish = state.Finish(first->slot, kClientA, 1,
                          PiecePutFinishOutcome::APPLIED, 12, kNow);
    ASSERT_TRUE(finish);
    EXPECT_TRUE(finish->ready_to_publish);
    EXPECT_EQ(state.completed_bytes(), 12u);
    EXPECT_FALSE(state.CanReclaim());
}

TEST(PiecePutStateTest, AGapNeverCountsAsComplete) {
    PiecePutState state("write-1", kDeadline);
    ASSERT_TRUE(state.Claim(Request(0, 4, 1, 10), kClientA, 10, kNow));
    ASSERT_TRUE(state.Claim(Request(6, 4, 2, 10), kClientA, 10, kNow));
    ASSERT_TRUE(
        state.Finish(0, kClientA, 1, PiecePutFinishOutcome::APPLIED, 10, kNow));
    const auto finish =
        state.Finish(1, kClientA, 2, PiecePutFinishOutcome::APPLIED, 10, kNow);
    ASSERT_TRUE(finish);
    EXPECT_FALSE(finish->ready_to_publish);
    EXPECT_EQ(state.completed_bytes(), 8u);
}

TEST(PiecePutStateTest, CloseOfRejectedRoundMismatchHasNoOriginalClaim) {
    PiecePutState state("write-1", kDeadline);
    const auto original = Request(0, 4, 1);
    ASSERT_TRUE(state.Claim(original, kClientA, 12, kNow));
    auto rejected = Request(4, 4, 2);
    rejected.write_id = "different-write";
    const auto start = state.Claim(rejected, kClientA, 12, kNow);
    ASSERT_FALSE(start);
    EXPECT_EQ(start.error(), ErrorCode::INVALID_PARAMS);
    rejected.lookup_only = true;
    const auto lookup = state.Claim(rejected, kClientA, 12, kNow);
    ASSERT_FALSE(lookup);
    EXPECT_EQ(lookup.error(), ErrorCode::INVALID_PARAMS);

    rejected.close_no_post = true;
    const auto close = state.Claim(rejected, kClientA, 12, kNow, false);
    ASSERT_FALSE(close);
    EXPECT_EQ(close.error(), ErrorCode::OBJECT_NOT_FOUND);
    rejected.write_id = "write-1";
    rejected.total_size = 13;
    const auto wrong_size = state.Claim(rejected, kClientA, 12, kNow, false);
    ASSERT_FALSE(wrong_size);
    EXPECT_EQ(wrong_size.error(), ErrorCode::OBJECT_NOT_FOUND);
    ASSERT_EQ(state.fragments().size(), 1u);
    EXPECT_EQ(state.fragments()[0].state, PiecePutPieceState::EXPOSED);
    EXPECT_EQ(state.round_state(), PiecePutRoundState::OPEN);
    EXPECT_EQ(state.completed_bytes(), 0u);
}

TEST(PiecePutStateTest, CloseWithOriginalIdentityRejectsChangedFingerprint) {
    PiecePutState state("write-1", kDeadline);
    auto original = Request(0, 4, 1);
    original.admission.session_id = 7;
    ASSERT_TRUE(state.Claim(original, kClientA, 12, kNow));
    auto check = [&](PiecePutStartRequest changed) {
        changed.close_no_post = true;
        const auto close = state.Claim(changed, kClientA, 12, kNow, false);
        ASSERT_FALSE(close);
        EXPECT_EQ(close.error(), ErrorCode::INVALID_PARAMS);
    };
    auto changed = original;
    changed.write_id = "different-write";
    check(changed);
    changed = original;
    changed.total_size = 13;
    check(changed);
    changed = original;
    changed.offset = 4;
    check(changed);
    ASSERT_EQ(state.fragments().size(), 1u);
    EXPECT_EQ(state.fragments()[0].state, PiecePutPieceState::EXPOSED);
    EXPECT_EQ(state.fragments()[0].offset, original.offset);
    EXPECT_EQ(state.round_state(), PiecePutRoundState::OPEN);
    EXPECT_EQ(state.completed_bytes(), 0u);
}

TEST(PiecePutStateTest, ReplayKeepsSlotAndRejectsChangedFingerprint) {
    PiecePutState state("write-1", kDeadline);
    const auto request = Request(0, 4, 7);
    const auto initial = state.Claim(request, kClientA, 12, kNow);
    ASSERT_TRUE(initial);
    const auto replay = state.Claim(request, kClientA, 12, kNow + 1s);
    ASSERT_TRUE(replay);
    EXPECT_EQ(replay->slot, initial->slot);
    EXPECT_TRUE(replay->allow_write);
    EXPECT_EQ(state.fragments().size(), 1u);

    auto changed = request;
    changed.offset = 4;
    auto result = state.Claim(changed, kClientA, 12, kNow);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    changed = request;
    changed.write_id = "write-2";
    EXPECT_FALSE(state.Claim(changed, kClientA, 12, kNow));
    changed = request;
    changed.total_size = 13;
    EXPECT_FALSE(state.Claim(changed, kClientA, 12, kNow));
    EXPECT_EQ(state.fragments().size(), 1u);

    auto overlap = state.Claim(Request(2, 4, 8), kClientA, 12, kNow);
    ASSERT_FALSE(overlap);
    EXPECT_EQ(overlap.error(), ErrorCode::INVALID_WRITE);
    overlap = state.Claim(request, kClientB, 12, kNow);
    ASSERT_FALSE(overlap);
    EXPECT_EQ(overlap.error(), ErrorCode::INVALID_WRITE);
    EXPECT_EQ(state.round_state(), PiecePutRoundState::OPEN);
}

TEST(PiecePutStateTest, LookupNeverCreatesOrGrantsWritePermission) {
    PiecePutState state("write-1", kDeadline);
    auto request = Request(0, 4, 1);
    request.lookup_only = true;
    const auto missing = state.Claim(request, kClientA, 12, kNow);
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error(), ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_TRUE(state.fragments().empty());

    request.lookup_only = false;
    ASSERT_TRUE(state.Claim(request, kClientA, 12, kNow));
    request.lookup_only = true;
    const auto found = state.Claim(request, kClientA, 12, kNow);
    ASSERT_TRUE(found);
    EXPECT_EQ(found->slot, 0u);
    EXPECT_EQ(found->piece_state, PiecePutPieceState::EXPOSED);
    EXPECT_FALSE(found->allow_write);
    EXPECT_EQ(state.fragments().size(), 1u);
}

TEST(PiecePutStateTest, TailReplayKeepsIdentityAheadOfOverlap) {
    PiecePutState state("write-1", kDeadline);
    ASSERT_TRUE(state.Claim(Request(0, 4, 1), kClientA, 12, kNow));
    ASSERT_TRUE(state.Claim(Request(4, 4, 2), kClientA, 12, kNow));
    const auto tail = Request(8, 4, 3);
    ASSERT_TRUE(state.Claim(tail, kClientA, 12, kNow));

    const auto replay = state.Claim(tail, kClientA, 12, kNow, false);
    ASSERT_TRUE(replay);
    EXPECT_EQ(replay->slot, 2u);
    EXPECT_TRUE(replay->allow_write);

    // This changes the tail operation's geometry to overlap an earlier slot.
    // It must remain an invalid replay, not be mistaken for a new overlap.
    auto changed = tail;
    changed.offset = 0;
    const auto invalid = state.Claim(changed, kClientA, 12, kNow);
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(state.fragments().size(), 3u);
    EXPECT_EQ(state.completed_bytes(), 0u);
}

TEST(PiecePutStateTest, ClosedClaimsKeepTheirErrorAheadOfOverlap) {
    PiecePutState state("write-1", kDeadline);
    ASSERT_TRUE(state.Claim(Request(0, 4, 1), kClientA, 12, kNow));
    auto request = Request(0, 4, 2);
    request.lookup_only = true;
    auto missing = state.Claim(request, kClientA, 12, kNow);
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error(), ErrorCode::OBJECT_NOT_FOUND);

    request.lookup_only = false;
    missing = state.Claim(request, kClientA, 12, kNow, false);
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error(), ErrorCode::OBJECT_NOT_FOUND);

    const auto expired = state.Claim(request, kClientA, 12, kDeadline);
    ASSERT_FALSE(expired);
    EXPECT_EQ(expired.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_EQ(state.fragments().size(), 1u);
    EXPECT_EQ(state.fragments().front().state, PiecePutPieceState::EXPOSED);
    EXPECT_FALSE(state.CanReclaim());
}

TEST(PiecePutStateTest, RandomArrivalAndCompletionKeepClaimIdentity) {
    RandomEngine random(20261111);
    auto shuffle = [&](std::vector<size_t>& order) {
        for (size_t remaining = order.size(); remaining > 1; --remaining) {
            std::swap(order[remaining - 1],
                      order[randomIndex(remaining, random)]);
        }
    };

    for (const size_t piece_count : {2u, 8u, 16u, 32u}) {
        SCOPED_TRACE(::testing::Message() << "pieces=" << piece_count);
        constexpr uint64_t kPieceSize = 4;
        const uint64_t total = piece_count * kPieceSize;
        PiecePutState state("write-1", kDeadline);
        std::vector<PiecePutStartRequest> requests;
        std::vector<size_t> order;
        std::vector<uint32_t> slots(piece_count);
        std::vector<bool> applied(piece_count, false);
        for (size_t piece = 0; piece < piece_count; ++piece) {
            auto request =
                Request(piece * kPieceSize, kPieceSize, piece + 1, total);
            request.admission.session_id = piece % 2 + 1;
            requests.push_back(std::move(request));
            order.push_back(piece);
        }
        shuffle(order);
        uint64_t completed = 0;
        for (size_t arrival = 0; arrival < piece_count; ++arrival) {
            const auto piece = order[arrival];
            const auto& client = piece % 2 == 0 ? kClientA : kClientB;
            const auto claim =
                state.Claim(requests[piece], client, total, kNow);
            ASSERT_TRUE(claim);
            slots[piece] = claim->slot;
            // Later claims must preserve both settled and exposed grants.
            if (arrival % 2 == 0) {
                const auto finish = state.Finish(
                    slots[piece], client, requests[piece].operation_seq,
                    PiecePutFinishOutcome::APPLIED, total, kNow,
                    requests[piece].admission.session_id);
                ASSERT_TRUE(finish);
                EXPECT_FALSE(finish->ready_to_publish);
                applied[piece] = true;
                completed += kPieceSize;
            }
            EXPECT_EQ(state.completed_bytes(), completed);
        }

        ASSERT_EQ(state.fragments().size(), piece_count);
        for (size_t piece = 0; piece < piece_count; ++piece) {
            const auto& client = piece % 2 == 0 ? kClientA : kClientB;
            const auto replay =
                state.Claim(requests[piece], client, total, kNow, false);
            ASSERT_TRUE(replay);
            EXPECT_EQ(replay->slot, slots[piece]);
            EXPECT_EQ(replay->allow_write, !applied[piece]);
        }

        shuffle(order);
        for (const auto piece : order) {
            const auto& client = piece % 2 == 0 ? kClientA : kClientB;
            const auto finish = state.Finish(
                slots[piece], client, requests[piece].operation_seq,
                PiecePutFinishOutcome::APPLIED, total, kNow,
                requests[piece].admission.session_id);
            ASSERT_TRUE(finish);
            if (!applied[piece]) {
                applied[piece] = true;
                completed += kPieceSize;
            }
            EXPECT_EQ(state.completed_bytes(), completed);
            EXPECT_EQ(finish->ready_to_publish, completed == total);
        }
        EXPECT_EQ(completed, total);
        state.Retire();
        EXPECT_TRUE(state.CanReclaim());
    }
}

TEST(PiecePutStateTest, AppliedReplayNeverHandsOutAnotherWriteAddress) {
    PiecePutState state("write-1", kDeadline);
    const auto request = Request(0, 4, 1);
    ASSERT_TRUE(state.Claim(request, kClientA, 12, kNow));
    ASSERT_TRUE(
        state.Finish(0, kClientA, 1, PiecePutFinishOutcome::APPLIED, 12, kNow));
    const auto replay = state.Claim(request, kClientA, 12, kNow);
    ASSERT_TRUE(replay);
    EXPECT_EQ(replay->piece_state, PiecePutPieceState::APPLIED);
    EXPECT_FALSE(replay->allow_write);
    EXPECT_EQ(state.completed_bytes(), 4u);
}

TEST(PiecePutStateTest, DeadlineIsFixedAndExpiredClaimsAllowOnlyCleanup) {
    PiecePutState state("write-1", kDeadline);
    const auto request = Request(0, 4, 1);
    ASSERT_TRUE(state.Claim(request, kClientA, 12, kNow));
    ASSERT_TRUE(state.Claim(Request(4, 4, 2), kClientA, 12, kDeadline - 1s));
    EXPECT_EQ(state.round_deadline(), kDeadline);

    const auto replay = state.Claim(request, kClientA, 12, kDeadline);
    ASSERT_TRUE(replay);
    EXPECT_EQ(replay->slot, 0u);
    EXPECT_EQ(replay->round_state, PiecePutRoundState::RETIRING);
    EXPECT_FALSE(replay->allow_write);
    const auto new_claim =
        state.Claim(Request(8, 4, 3), kClientA, 12, kDeadline);
    ASSERT_FALSE(new_claim);
    EXPECT_EQ(new_claim.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_EQ(state.fragments().size(), 2u);
    EXPECT_FALSE(state.CanReclaim());

    ASSERT_TRUE(state.Finish(0, kClientA, 1, PiecePutFinishOutcome::NO_POST, 12,
                             kDeadline));
    const auto settled = state.Finish(
        1, kClientA, 2, PiecePutFinishOutcome::APPLIED, 12, kDeadline);
    ASSERT_TRUE(settled);
    EXPECT_FALSE(settled->ready_to_publish);
    EXPECT_EQ(state.completed_bytes(), 0u);
    EXPECT_TRUE(state.CanReclaim());
}

TEST(PiecePutStateTest, WrongFinishIdentityCannotChangeAnyState) {
    PiecePutState state("write-1", kDeadline);
    ASSERT_TRUE(state.Claim(Request(0, 12, 1), kClientA, 12, kNow));

    auto result =
        state.Finish(0, kClientB, 1, PiecePutFinishOutcome::APPLIED, 12, kNow);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::ILLEGAL_CLIENT);
    result =
        state.Finish(0, kClientA, 2, PiecePutFinishOutcome::NO_POST, 12, kNow);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::INVALID_WRITE);
    EXPECT_FALSE(
        state.Finish(1, kClientA, 1, PiecePutFinishOutcome::APPLIED, 12, kNow));
    EXPECT_FALSE(state.Finish(
        0, kClientA, 1, static_cast<PiecePutFinishOutcome>(255), 12, kNow));
    EXPECT_EQ(state.round_state(), PiecePutRoundState::OPEN);
    EXPECT_EQ(state.completed_bytes(), 0u);
    EXPECT_EQ(state.fragments().front().state, PiecePutPieceState::EXPOSED);
}

TEST(PiecePutStateTest, UncertainCannotUndoAnAppliedGrant) {
    PiecePutState state("write-1", kDeadline);
    ASSERT_TRUE(state.Claim(Request(0, 4, 1), kClientA, 12, kNow));
    ASSERT_TRUE(state.Claim(Request(4, 8, 1), kClientB, 12, kNow));
    ASSERT_TRUE(
        state.Finish(0, kClientA, 1, PiecePutFinishOutcome::APPLIED, 12, kNow));
    const auto delayed = state.Finish(
        0, kClientA, 1, PiecePutFinishOutcome::UNCERTAIN, 12, kNow);
    ASSERT_TRUE(delayed);
    EXPECT_EQ(delayed->piece_state, PiecePutPieceState::APPLIED);
    EXPECT_EQ(state.round_state(), PiecePutRoundState::OPEN);
    EXPECT_EQ(state.completed_bytes(), 4u);

    ASSERT_TRUE(state.Finish(1, kClientB, 1, PiecePutFinishOutcome::UNCERTAIN,
                             12, kNow));
    EXPECT_EQ(state.round_state(), PiecePutRoundState::RETIRING);
    EXPECT_FALSE(state.CanReclaim());
    const auto resolved =
        state.Finish(1, kClientB, 1, PiecePutFinishOutcome::APPLIED, 12, kNow);
    ASSERT_TRUE(resolved);
    EXPECT_FALSE(resolved->ready_to_publish);
    EXPECT_EQ(state.completed_bytes(), 4u);
    EXPECT_TRUE(state.CanReclaim());
}

TEST(PiecePutStateTest, WithdrawnAndAppliedAreIrreversible) {
    PiecePutState state("write-1", kDeadline);
    const auto request = Request(0, 4, 1);
    ASSERT_TRUE(state.Claim(request, kClientA, 12, kNow));
    ASSERT_TRUE(
        state.Finish(0, kClientA, 1, PiecePutFinishOutcome::NO_POST, 12, kNow));
    EXPECT_TRUE(state.CanReclaim());
    const auto replay = state.Claim(request, kClientA, 12, kNow);
    ASSERT_TRUE(replay);
    EXPECT_EQ(replay->piece_state, PiecePutPieceState::WITHDRAWN);
    EXPECT_FALSE(replay->allow_write);
    ASSERT_TRUE(
        state.Finish(0, kClientA, 1, PiecePutFinishOutcome::NO_POST, 12, kNow));
    EXPECT_FALSE(
        state.Finish(0, kClientA, 1, PiecePutFinishOutcome::APPLIED, 12, kNow));
    EXPECT_EQ(state.fragments().front().state, PiecePutPieceState::WITHDRAWN);

    PiecePutState applied("write-1", kDeadline);
    ASSERT_TRUE(applied.Claim(request, kClientA, 12, kNow));
    ASSERT_TRUE(applied.Finish(0, kClientA, 1, PiecePutFinishOutcome::APPLIED,
                               12, kNow));
    EXPECT_FALSE(applied.Finish(0, kClientA, 1, PiecePutFinishOutcome::NO_POST,
                                12, kNow));
    EXPECT_EQ(applied.round_state(), PiecePutRoundState::OPEN);
    EXPECT_EQ(applied.completed_bytes(), 4u);
}

TEST(PiecePutStateTest, FragmentLimitDoesNotRejectOriginalReplay) {
    PiecePutState state("write-1", kDeadline);
    constexpr uint64_t total = PiecePutState::kMaxFragments + 1;
    for (uint64_t i = 0; i < PiecePutState::kMaxFragments; ++i) {
        const auto claim =
            state.Claim(Request(i, 1, i, total), kClientA, total, kNow);
        ASSERT_TRUE(claim);
        EXPECT_EQ(claim->slot, i);
    }
    ASSERT_TRUE(state.Claim(Request(0, 1, 0, total), kClientA, total, kNow));
    const auto tail =
        state.Claim(Request(PiecePutState::kMaxFragments - 1, 1,
                            PiecePutState::kMaxFragments - 1, total),
                    kClientA, total, kNow, false);
    ASSERT_TRUE(tail);
    EXPECT_EQ(tail->slot, PiecePutState::kMaxFragments - 1);
    const auto extra =
        state.Claim(Request(PiecePutState::kMaxFragments, 1, total, total),
                    kClientA, total, kNow);
    ASSERT_FALSE(extra);
    EXPECT_EQ(extra.error(), ErrorCode::INVALID_PARAMS);
    const auto overlapping_extra =
        state.Claim(Request(0, 1, total, total), kClientA, total, kNow);
    ASSERT_FALSE(overlapping_extra);
    EXPECT_EQ(overlapping_extra.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(state.fragments().size(), PiecePutState::kMaxFragments);
}

TEST(PiecePutStateTest, SessionSeparatesReusedNoncesAndFinishIdentity) {
    PiecePutState state("write-1", kDeadline);
    auto first = Request(0, 4, 1);
    first.admission.session_id = 11;
    ASSERT_TRUE(state.Claim(first, kClientA, 12, kNow));
    auto next = Request(4, 8, 1);
    next.admission.session_id = 12;
    ASSERT_TRUE(state.Claim(next, kClientA, 12, kNow));
    const auto wrong = state.Finish(
        0, kClientA, 1, PiecePutFinishOutcome::NO_POST, 12, kNow, 12);
    ASSERT_FALSE(wrong);
    EXPECT_EQ(wrong.error(), ErrorCode::INVALID_WRITE);
    EXPECT_EQ(state.fragments()[0].state, PiecePutPieceState::EXPOSED);
    ASSERT_TRUE(state.Finish(0, kClientA, 1, PiecePutFinishOutcome::APPLIED, 12,
                             kNow, 11));
    const auto done = state.Finish(
        1, kClientA, 1, PiecePutFinishOutcome::APPLIED, 12, kNow, 12);
    ASSERT_TRUE(done);
    EXPECT_TRUE(done->ready_to_publish);
}

TEST(PiecePutStateTest, SpentNonceOnlyRecognizesTheExistingClaim) {
    PiecePutState state("write-1", kDeadline);
    const auto first = Request(0, 4, 1);
    EXPECT_FALSE(state.Claim(first, kClientA, 12, kNow, false));
    EXPECT_TRUE(state.fragments().empty());
    ASSERT_TRUE(state.Claim(first, kClientA, 12, kNow));
    const auto retry = state.Claim(first, kClientA, 12, kNow, false);
    ASSERT_TRUE(retry);
    EXPECT_TRUE(retry->allow_write);
    EXPECT_EQ(retry->slot, 0u);
    EXPECT_FALSE(state.Claim(Request(4, 8, 2), kClientA, 12, kNow, false));
    EXPECT_EQ(state.fragments().size(), 1u);
}

TEST(PiecePutStateTest, PublicRequestsAndResultsRoundTrip) {
    auto request = Request(4, 8, 19);
    request.lookup_only = true;
    request.admission = PiecePutAdmission{{3, 4}, 91, 4096};
    request.close_no_post = true;
    request.token = PiecePutToken{{3, 4}, 55, 7, "tenant-round-trip", 91};
    const auto decoded = struct_pack::deserialize<PiecePutStartRequest>(
        struct_pack::serialize(request));
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->key, request.key);
    EXPECT_EQ(decoded->write_id, request.write_id);
    EXPECT_EQ(decoded->operation_seq, request.operation_seq);
    EXPECT_EQ(decoded->token, request.token);
    EXPECT_TRUE(decoded->lookup_only);
    EXPECT_TRUE(decoded->close_no_post);
    EXPECT_EQ(decoded->admission, request.admission);

    PiecePutStartResponse response;
    response.piece_state = PiecePutPieceState::EXPOSED;
    response.round_state = PiecePutRoundState::RETIRING;
    response.token = request.token;
    response.tcp_instance = "target-instance";
    response.admission_closed = true;
    const auto restored = struct_pack::deserialize<PiecePutStartResponse>(
        struct_pack::serialize(response));
    ASSERT_TRUE(restored);
    EXPECT_EQ(restored->piece_state, response.piece_state);
    EXPECT_EQ(restored->round_state, response.round_state);
    EXPECT_EQ(restored->token, response.token);
    EXPECT_FALSE(restored->replica);
    EXPECT_EQ(restored->tcp_instance, response.tcp_instance);
    EXPECT_TRUE(restored->admission_closed);

    static_assert(!std::is_copy_constructible_v<PiecePutState>);
    static_assert(std::is_nothrow_move_constructible_v<PiecePutState>);
}

}  // namespace
}  // namespace mooncake
