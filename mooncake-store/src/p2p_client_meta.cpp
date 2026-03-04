#include "p2p_client_meta.h"

#include <algorithm>
#include <glog/logging.h>

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
    return std::vector<std::string>{ip_address_};
}

void P2PClientMeta::UpdateSegmentUsages(
    const std::vector<TierUsageInfo>& usages) {
    SpinRWLockLocker lock(&capacity_mutex_);
    for (const auto& usage : usages) {
        auto old_usage =
            segment_manager_->UpdateSegmentUsage(usage.segment_id, usage.usage);
        if (!old_usage.has_value()) {
            LOG(WARNING)
                << "fail to update segment usage, segment doesn't exist"
                << ", segment_id: " << usage.segment_id;
            continue;
        }
        client_usage_ = client_usage_ - old_usage.value() + usage.usage;
    }
}

size_t P2PClientMeta::GetAvailableCapacity() const {
    SpinRWLockLocker lock(&capacity_mutex_, shared_lock);
    if (client_capacity_ <= client_usage_) return 0;
    return client_capacity_ - client_usage_;
}

auto P2PClientMeta::CollectWriteRouteCandidates(
    const WriteRouteRequest& req, std::vector<WriteCandidate>& candidates)
    -> tl::expected<bool, ErrorCode> {
    SharedMutexLocker lock(&client_mutex_, shared_lock);

    // Check health status under lock protection
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(WARNING) << "client could not route"
                     << ", client_id: " << client_id_;
        return false;  // skip unhealthy client, candidaes are not enough
    }

    // localhost is not allowed, skip current client
    if (!req.config.allow_local && client_id_ == req.client_id)
        return false;  // candidaes are not enough

    // iterate segments to find candidates
    bool trigger_stop_early = false;
    segment_manager_->ForEachSegment([&](const Segment& seg) -> bool {
        // In ForEachSegment callback:
        // 1. return false means continue to process next segment
        //    (skip current segment or finish processing)
        // 2. return true means early stop
        size_t usage = seg.GetP2PExtra().usage;
        if (seg.size - usage < req.size)
            return false;  // usage does not enough, candidaes are not enough

        const auto& p2p_extra = seg.GetP2PExtra();

        // exclude segments that contain all tags in tag_filters
        bool hit_fillter_tag = false;
        for (const auto& tag : req.config.tag_filters) {
            if (std::find(p2p_extra.tags.begin(), p2p_extra.tags.end(), tag) ==
                p2p_extra.tags.end()) {
                hit_fillter_tag = true;
                break;
            }
        }
        if (hit_fillter_tag)
            return false;  // hit excluding tag, candidaes are not enough

        if (p2p_extra.priority < req.config.priority_limit)
            return false;  // priority does not enough, candidaes are not enough

        int priority = p2p_extra.priority;
        if (req.config.prefer_local && client_id_ == req.client_id) {
            // hit localhost and prefer local, add infinite priority
            priority += INF_PRIORITY;
        }

        WriteCandidate candidate;
        candidate.available_capacity = seg.size - usage;
        candidate.priority = priority;
        candidate.replica.client_id = client_id_;
        candidate.replica.segment_id = seg.id;
        candidate.replica.ip_address = ip_address_;
        candidate.replica.rpc_port = rpc_port_;

        candidates.push_back(std::move(candidate));
        if (req.config.early_return &&
            candidates.size() >= req.config.max_candidates &&
            req.config.max_candidates !=
                WriteRouteRequestConfig::RETURN_ALL_CANDIDATES) {
            // current candidates are enough, early stop
            trigger_stop_early = true;
            return true;  // stop ForEachSegment
        }
        return false;  // process next segment
    });

    // early stop, if trigger_stop_early == true, stop ForEachClient
    return trigger_stop_early;
}

}  // namespace mooncake
