#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "piece_put_types.h"

namespace mooncake {

struct PiecePutFragment {
    uint64_t offset;
    uint64_t length;
    UUID client_id;
    uint64_t operation_seq;
    uint64_t admission_session_id{0};
    PiecePutPieceState state{PiecePutPieceState::EXPOSED};
};

struct PiecePutClaimResult {
    uint32_t slot;
    PiecePutPieceState piece_state;
    PiecePutRoundState round_state;
    bool allow_write;
};

struct PiecePutFinishResult {
    PiecePutPieceState piece_state;
    PiecePutRoundState round_state;
    bool ready_to_publish;
};

// The owning ObjectRoute key guard serializes every access. The containing
// ObjectMetadata supplies the immutable total size; registration lifetime,
// publication and physical drain proofs belong to the Master service.
class PiecePutState {
   public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    static constexpr size_t kMaxFragments = 128;
    static constexpr size_t kMaxWriteIdBytes = 256;

    PiecePutState(std::string write_id, TimePoint round_deadline)
        : write_id_(std::move(write_id)), round_deadline_(round_deadline) {
        // Charge and reserve the entire bounded metadata budget up front.
        fragments_.reserve(kMaxFragments);
    }

    PiecePutState(const PiecePutState&) = delete;
    PiecePutState& operator=(const PiecePutState&) = delete;
    PiecePutState(PiecePutState&&) noexcept = default;
    PiecePutState& operator=(PiecePutState&&) noexcept = default;

    static tl::expected<void, ErrorCode> ValidateStartRequest(
        const PiecePutStartRequest& request) {
        if (request.key.empty() || request.write_id.empty() ||
            request.write_id.size() > kMaxWriteIdBytes ||
            request.total_size == 0 || request.length == 0 ||
            request.offset > request.total_size ||
            request.length > request.total_size - request.offset ||
            (request.token && !request.lookup_only && !request.close_no_post)) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        return {};
    }

    tl::expected<PiecePutClaimResult, ErrorCode> Claim(
        const PiecePutStartRequest& request, const UUID& client_id,
        uint64_t actual_total_size, TimePoint now, bool allow_new = true) {
        auto valid = ValidateStartRequest(request);
        if (!valid) {
            return tl::make_unexpected(valid.error());
        }
        if (request.total_size != actual_total_size ||
            request.write_id != write_id_) {
            // A rejected Start may lose its reply. The service fences this
            // nonce before close_no_post; a different current round cannot
            // prevent closing an operation that never obtained a claim here.
            if (request.close_no_post &&
                std::none_of(fragments_.begin(), fragments_.end(),
                             [&](const PiecePutFragment& fragment) {
                                 return fragment.client_id == client_id &&
                                        fragment.operation_seq ==
                                            request.operation_seq &&
                                        fragment.admission_session_id ==
                                            request.admission.session_id;
                             })) {
                return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            }
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        // Recognize the original operation before checking capacity, overlap
        // or closure. Its token must remain available for cleanup.
        for (size_t slot = 0; slot < fragments_.size(); ++slot) {
            const auto& fragment = fragments_[slot];
            if (fragment.client_id != client_id ||
                fragment.operation_seq != request.operation_seq ||
                fragment.admission_session_id != request.admission.session_id) {
                continue;
            }
            if (fragment.offset != request.offset ||
                fragment.length != request.length) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            Expire(now);
            return PiecePutClaimResult{
                static_cast<uint32_t>(slot), fragment.state, round_state_,
                !request.lookup_only && !request.close_no_post &&
                    round_state_ == PiecePutRoundState::OPEN &&
                    fragment.state == PiecePutPieceState::EXPOSED};
        }

        Expire(now);
        if (request.lookup_only || request.close_no_post || !allow_new) {
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        if (round_state_ != PiecePutRoundState::OPEN) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        if (fragments_.size() == kMaxFragments) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        const uint64_t end = request.offset + request.length;
        for (const auto& fragment : fragments_) {
            if (request.offset < fragment.offset + fragment.length &&
                fragment.offset < end) {
                return tl::make_unexpected(ErrorCode::INVALID_WRITE);
            }
        }

        // push_back can allocate. Nothing is exposed and no counters change
        // until it has succeeded.
        fragments_.push_back({request.offset, request.length, client_id,
                              request.operation_seq,
                              request.admission.session_id});
        return PiecePutClaimResult{static_cast<uint32_t>(fragments_.size() - 1),
                                   PiecePutPieceState::EXPOSED, round_state_,
                                   true};
    }

    tl::expected<PiecePutFinishResult, ErrorCode> Finish(
        uint32_t slot, const UUID& client_id, uint64_t operation_seq,
        PiecePutFinishOutcome outcome, uint64_t actual_total_size,
        TimePoint now, uint64_t admission_session_id = 0) {
        if (slot >= fragments_.size() || actual_total_size == 0 ||
            (outcome != PiecePutFinishOutcome::APPLIED &&
             outcome != PiecePutFinishOutcome::NO_POST &&
             outcome != PiecePutFinishOutcome::UNCERTAIN)) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        auto& fragment = fragments_[slot];
        if (fragment.client_id != client_id) {
            return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
        }
        if (fragment.operation_seq != operation_seq ||
            fragment.admission_session_id != admission_session_id) {
            return tl::make_unexpected(ErrorCode::INVALID_WRITE);
        }
        if (fragment.offset > actual_total_size ||
            fragment.length > actual_total_size - fragment.offset ||
            completed_bytes_ > actual_total_size) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if ((fragment.state == PiecePutPieceState::APPLIED &&
             outcome == PiecePutFinishOutcome::NO_POST) ||
            (fragment.state == PiecePutPieceState::WITHDRAWN &&
             outcome == PiecePutFinishOutcome::APPLIED)) {
            return tl::make_unexpected(ErrorCode::INVALID_WRITE);
        }
        Expire(now);

        // A delayed UNCERTAIN cannot undo an already settled grant.
        if (fragment.state != PiecePutPieceState::EXPOSED) {
            return FinishResult(fragment, actual_total_size);
        }
        if (outcome == PiecePutFinishOutcome::UNCERTAIN) {
            Retire();
            return FinishResult(fragment, actual_total_size);
        }
        if (outcome == PiecePutFinishOutcome::NO_POST) {
            fragment.state = PiecePutPieceState::WITHDRAWN;
            Retire();
            return FinishResult(fragment, actual_total_size);
        }
        if (round_state_ == PiecePutRoundState::OPEN) {
            if (fragment.length > actual_total_size - completed_bytes_) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            completed_bytes_ += fragment.length;
        }
        fragment.state = PiecePutPieceState::APPLIED;
        return FinishResult(fragment, actual_total_size);
    }

    void Retire() noexcept { round_state_ = PiecePutRoundState::RETIRING; }

    void Expire(TimePoint now) noexcept {
        if (now >= round_deadline_) {
            Retire();
        }
    }

    // Physical endpoint drain is a separate, registration-owned proof. This
    // predicate covers only the path where all issued grants have settled.
    [[nodiscard]] bool CanReclaim() const noexcept {
        return round_state_ == PiecePutRoundState::RETIRING &&
               std::all_of(fragments_.begin(), fragments_.end(),
                           [](const PiecePutFragment& fragment) {
                               return fragment.state !=
                                      PiecePutPieceState::EXPOSED;
                           });
    }

    [[nodiscard]] const std::string& write_id() const noexcept {
        return write_id_;
    }

    [[nodiscard]] TimePoint round_deadline() const noexcept {
        return round_deadline_;
    }

    [[nodiscard]] PiecePutRoundState round_state() const noexcept {
        return round_state_;
    }

    [[nodiscard]] uint64_t completed_bytes() const noexcept {
        return completed_bytes_;
    }

    [[nodiscard]] const std::vector<PiecePutFragment>& fragments()
        const noexcept {
        return fragments_;
    }

   private:
    [[nodiscard]] PiecePutFinishResult FinishResult(
        const PiecePutFragment& fragment, uint64_t total_size) const noexcept {
        return {fragment.state, round_state_,
                round_state_ == PiecePutRoundState::OPEN &&
                    completed_bytes_ == total_size};
    }

    std::string write_id_;
    TimePoint round_deadline_;
    PiecePutRoundState round_state_{PiecePutRoundState::OPEN};
    uint64_t completed_bytes_{0};
    std::vector<PiecePutFragment> fragments_;
};

}  // namespace mooncake
