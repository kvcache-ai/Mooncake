#include "p2p_client_meta.h"

#include <algorithm>
#include <glog/logging.h>
#include <limits>

namespace mooncake {
P2PClientMeta::P2PClientMeta(const UUID& client_id,
                             const std::string& ip_address, uint16_t rpc_port)
    : ClientMeta(client_id), ip_address_(ip_address), rpc_port_(rpc_port) {
    segment_manager_ = std::make_shared<P2PSegmentManager>();
    segment_manager_->SetSegmentChangeCallbacks(
        [this](const Segment& segment) {
            // OnSegmentAddedCallback
            SpinRWLockLocker lock(&capacity_mutex_);
            client_capacity_ += segment.size;
            client_usage_ += segment.GetP2PExtra().usage;
        },
        [this](const Segment& segment) {
            // OnSegmentRemovedCallback
            SpinRWLockLocker lock(&capacity_mutex_);
            client_capacity_ -= segment.size;
            client_usage_ -= segment.GetP2PExtra().usage;
        });
}

std::shared_ptr<SegmentManager> P2PClientMeta::GetSegmentManager() {
    return segment_manager_;
}

tl::expected<std::vector<std::string>, ErrorCode> P2PClientMeta::QueryIp(
    const UUID& client_id) {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return tl::make_unexpected(check_ret.error());
    }
    return std::vector<std::string>{ip_address_};
}

SyncSegmentMetaResult P2PClientMeta::UpdateSegmentUsages(
    const std::vector<TierUsageInfo>& usages) {
    SyncSegmentMetaResult result;
    SpinRWLockLocker lock(&capacity_mutex_);
    for (const auto& usage : usages) {
        SyncSegmentMetaResult::SubResult sub_res;
        sub_res.segment_id = usage.segment_id;

        auto old_usage =
            segment_manager_->UpdateSegmentUsage(usage.segment_id, usage.usage);
        if (!old_usage.has_value()) {
            LOG(WARNING) << "fail to update segment usage"
                         << ", client_id: " << client_id_
                         << ", segment_id: " << usage.segment_id
                         << ", usage: " << usage.usage
                         << ", error: " << old_usage.error();
            sub_res.error = old_usage.error();
            result.sub_results.push_back(sub_res);
            continue;
        }

        client_usage_ = client_usage_ - old_usage.value() + usage.usage;
        sub_res.error = ErrorCode::OK;
        result.sub_results.push_back(sub_res);
    }
    return result;
}

size_t P2PClientMeta::GetAvailableCapacity() const {
    SpinRWLockLocker lock(&capacity_mutex_, shared_lock);
    if (client_capacity_ <= client_usage_) return 0;
    return client_capacity_ - client_usage_;
}

P2PClientMeta::CapacityStat P2PClientMeta::GetWriteScoreCapacity(
    const std::vector<std::string>& tag_filters, int priority_limit,
    bool top_tier_only) const {
    // A segment is eligible for scoring if it carries no filtered tag and its
    // priority is >= priority_limit.
    auto eligible = [&](const P2PSegmentExtraData& extra) -> bool {
        if (extra.priority < priority_limit) return false;
        for (const auto& tag : tag_filters) {
            if (std::find(extra.tags.begin(), extra.tags.end(), tag) !=
                extra.tags.end()) {
                return false;
            }
        }
        return true;
    };

    CapacityStat all, top;
    int max_priority = std::numeric_limits<int>::min();
    segment_manager_->ForEachSegment([&](const Segment& seg) -> bool {
        const auto& extra = seg.GetP2PExtra();
        if (!eligible(extra)) return false;
        const size_t free = seg.size > extra.usage ? seg.size - extra.usage : 0;
        all.total += seg.size;
        all.free += free;
        if (extra.priority > max_priority) {
            max_priority = extra.priority;
            top = {free, seg.size};
        } else if (extra.priority == max_priority) {
            top.total += seg.size;
            top.free += free;
        }
        return false;  // always continue to next segment
    });
    return top_tier_only ? top : all;
}

// Returns std::nullopt when this client is not a write-route candidate
std::optional<WriteCandidate> P2PClientMeta::GetWriteRouteCandidate(
    const WriteRouteRequest& req) {
    SharedMutexLocker lock(&client_mutex_, shared_lock);

    // Check health status under lock protection.
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(WARNING) << "client could not route"
                     << ", client_id: " << client_id_;
        // Unhealthy is not an error: the client is simply not a candidate.
        return std::nullopt;
    }

    // Client-granular routing: the master routes to a client; the client picks
    // the concrete segment/tier. The score is the raw free ratio; the master
    // multiplies it by (1-w) for local or w for remote.
    const CapacityStat cap =
        GetWriteScoreCapacity(req.config.tag_filters, req.config.priority_limit,
                              req.config.top_tier_only);
    if (cap.total == 0) return std::nullopt;  // no eligible tier
    if (cap.free < req.size)
        return std::nullopt;  // cannot hold (master's view)

    const double free_ratio = static_cast<double>(cap.free) / cap.total;

    WriteCandidate candidate;
    candidate.client_id = client_id_;
    candidate.ip_address = ip_address_;
    candidate.rpc_port = rpc_port_;
    candidate.available_capacity = cap.free;
    candidate.score = free_ratio;
    return candidate;
}

}  // namespace mooncake
