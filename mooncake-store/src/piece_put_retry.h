#pragma once

#include <cstddef>
#include <chrono>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "piece_put.h"

namespace mooncake::piece_put_detail {

constexpr size_t kMetadataAttempts = 3;

inline bool IsUncertain(ErrorCode error) {
    return error == ErrorCode::RPC_FAIL || error == ErrorCode::RPC_TIMEOUT;
}

inline bool GrantExpired(const PiecePutStartResponse& response,
                         std::chrono::steady_clock::time_point sent_at,
                         std::chrono::steady_clock::time_point now) {
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(now - sent_at)
            .count();
    return response.remaining_ms == 0 ||
           (elapsed >= 0 &&
            static_cast<uint64_t>(elapsed) >= response.remaining_ms);
}

// Retry only uncertain metadata results, keeping each operation's identity.
// The SDK invokes its data phase separately and never through this helper.
template <typename Response, typename Request, typename Send>
std::vector<tl::expected<Response, ErrorCode>> RetryMetadata(
    const std::vector<Request>& requests, Send&& send,
    std::vector<std::chrono::steady_clock::time_point>* send_times = nullptr) {
    if (send_times) send_times->assign(requests.size(), {});
    std::vector<tl::expected<Response, ErrorCode>> results(
        requests.size(), tl::unexpected(ErrorCode::RPC_FAIL));
    std::vector<size_t> pending;
    pending.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) pending.push_back(i);
    for (size_t attempt = 0; attempt < kMetadataAttempts && !pending.empty();
         ++attempt) {
        std::vector<Request> retry;
        retry.reserve(pending.size());
        for (auto index : pending) retry.push_back(requests[index]);
        // Only Start asks for timing. Finish/Commit allocate no timestamp
        // array and do not read the clock here.
        const auto sent_at = send_times
                                 ? std::chrono::steady_clock::now()
                                 : std::chrono::steady_clock::time_point{};
        auto responses = send(retry);
        std::vector<size_t> next;
        for (size_t i = 0; i < pending.size(); ++i) {
            auto& result = results[pending[i]];
            result = responses.size() == pending.size()
                         ? std::move(responses[i])
                         : tl::expected<Response, ErrorCode>(
                               tl::unexpected(ErrorCode::RPC_FAIL));
            if (send_times && result) (*send_times)[pending[i]] = sent_at;
            if (!result && IsUncertain(result.error()))
                next.push_back(pending[i]);
        }
        pending = std::move(next);
    }
    return results;
}

// A handle is recoverable only after successful data transfer. Terminal
// business errors must not be disguised as an unconfirmed successful write.
inline tl::expected<PiecePutItemResult, ErrorCode> MapFinishResult(
    const PiecePutCommitHandle& handle,
    const tl::expected<PiecePutFinishResponse, ErrorCode>& confirmation,
    ErrorCode data_error = ErrorCode::OK) {
    if (data_error != ErrorCode::OK) return tl::unexpected(data_error);
    if (confirmation) {
        return PiecePutItemResult{confirmation->piece_completed,
                                  confirmation->object_published, std::nullopt};
    }
    if (IsUncertain(confirmation.error()))
        return PiecePutItemResult{false, false, handle};
    return tl::unexpected(confirmation.error());
}

}  // namespace mooncake::piece_put_detail
