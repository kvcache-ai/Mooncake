#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "replica.h"
#include "types.h"

namespace mooncake {

// One producer admission window. Nonces are in [1, capacity]. A window
// only controls new grants; expiry or rotation never proves physical drain.
struct PiecePutAdmission {
    UUID master_instance{};
    uint64_t session_id{0};
    uint32_t capacity{4096};

    bool operator==(const PiecePutAdmission&) const = default;
};
YLT_REFL(PiecePutAdmission, master_instance, session_id, capacity);

enum class PiecePutPieceState : uint8_t {
    UNKNOWN,
    EXPOSED,
    APPLIED,
    WITHDRAWN,
};

enum class PiecePutRoundState : uint8_t {
    UNKNOWN,
    OPEN,
    RETIRING,
    ROUND_PUBLISHED,
};

enum class PiecePutFinishOutcome : uint8_t {
    APPLIED,
    NO_POST,
    UNCERTAIN,
};

// A route generation is unique only inside one Master instance and
// namespace. Bind the canonical tenant explicitly, including after completion.
struct PiecePutToken {
    UUID master_instance{};
    uint64_t generation{0};
    uint32_t slot{0};
    std::string resolved_tenant;
    uint64_t admission_session_id{0};

    bool operator==(const PiecePutToken&) const = default;
};
YLT_REFL(PiecePutToken, master_instance, generation, slot, resolved_tenant,
         admission_session_id);

struct PiecePutStartRequest {
    std::string key;
    uint64_t total_size{0};
    uint64_t offset{0};
    uint64_t length{0};
    std::string write_id;
    uint64_t operation_seq{0};
    bool lookup_only{false};
    // A lookup with an original token can establish ROUND_PUBLISHED after
    // the per-piece state was released. A normal Start never supplies one.
    std::optional<PiecePutToken> token;
    PiecePutAdmission admission;
    // Producer attests that this operation never posted a physical write.
    // Atomically fences a delayed Start and settles its original grant, if any.
    bool close_no_post{false};
};
YLT_REFL(PiecePutStartRequest, key, total_size, offset, length, write_id,
         operation_seq, lookup_only, token, admission, close_no_post);

struct PiecePutStartResponse {
    PiecePutPieceState piece_state{PiecePutPieceState::UNKNOWN};
    PiecePutRoundState round_state{PiecePutRoundState::UNKNOWN};
    std::optional<PiecePutToken> token;
    // Present only for an OPEN, unexpired EXPOSED claim, never for lookup.
    std::optional<Replica::Descriptor> replica;
    std::string tcp_instance;
    // New grants for this nonce are fenced. This is not a physical drain proof.
    bool admission_closed{false};
};
YLT_REFL(PiecePutStartResponse, piece_state, round_state, token, replica,
         tcp_instance, admission_closed);

struct PiecePutFinishRequest {
    std::string key;
    PiecePutToken token;
    uint64_t operation_seq{0};
    PiecePutFinishOutcome outcome{PiecePutFinishOutcome::UNCERTAIN};
};
YLT_REFL(PiecePutFinishRequest, key, token, operation_seq, outcome);

struct PiecePutFinishResponse {
    PiecePutPieceState piece_state{PiecePutPieceState::UNKNOWN};
    PiecePutRoundState round_state{PiecePutRoundState::UNKNOWN};
};
YLT_REFL(PiecePutFinishResponse, piece_state, round_state);

}  // namespace mooncake
