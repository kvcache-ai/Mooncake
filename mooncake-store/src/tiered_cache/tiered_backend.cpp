#include "tiered_cache/tiered_backend.h"
#include "tiered_cache/cache_tier.h"

#include <glog/logging.h>
#include <fstream>
#include <vector>
#include <algorithm>
#include <json/value.h>
#include <cctype>

namespace mooncake {

TieredBackend::TieredBackend() = default;

bool TieredBackend::Init(Json::Value root, TransferEngine* engine) {
    // Initialize the DataCopier
    try {
        DataCopierBuilder builder;
        data_copier_ = builder.Build();
    } catch (const std::logic_error& e) {
        LOG(FATAL) << "Failed to build DataCopier: " << e.what();
        return false;
    }

    // Create CacheTier instances and store their static info
    if (!root.isMember("tiers")) {
        LOG(ERROR) << "Tiered cache config is missing 'tiers' array.";
        return false;
    }

    for (const auto& tier_config : root["tiers"]) {
        uint64_t id = tier_config["id"].asUInt();
        std::string type = tier_config["type"].asString();
        // size_t capacity = tier_config["capacity"].asInt();
        int priority = tier_config["priority"].asInt();
        std::vector<std::string> tags;
        if (tier_config.isMember("tags") && tier_config["tags"].isArray()) {
            for (const auto& tag_node : tier_config["tags"]) {
                tags.push_back(tag_node.asString());
            }
        }
        std::unique_ptr<CacheTier> tier;

        // TODO: add specific cache tier types init logic here
        /*
        if (!tier->Init(this, engine)) {
            LOG(ERROR) << "Failed to initialize tier " << id;
            return false;
        }
        */

        tiers_[id] = std::move(tier);
        tier_info_[id] = {priority, tags};
    }

    LOG(INFO) << "TieredBackend initialized successfully with " << tiers_.size()
              << " tiers.";
    return true;
}

std::vector<TierView> TieredBackend::GetTierViews() const {
    std::vector<TierView> views;
    views.reserve(tiers_.size());
    for (const auto& [id, tier] : tiers_) {
        const auto& info = tier_info_.at(id);
        views.push_back({tier->GetTierId(), tier->GetMemoryType(),
                         tier->GetCapacity(), tier->GetUsage(), info.priority,
                         info.tags});
    }
    return views;
}

bool TieredBackend::Get(const std::string& key, void*& data, size_t& size) {
    auto maybe_tier_id = FindKey(key);
    if (!maybe_tier_id) {
        return false;
    }
    return tiers_.at(*maybe_tier_id)->Get(key, data, size);
}

bool TieredBackend::Put(const std::string& key, uint64_t target_tier_id,
                        const DataSource& source) {
    auto it = tiers_.find(target_tier_id);
    if (it == tiers_.end()) {
        LOG(ERROR) << "Put failed: Invalid target tier ID " << target_tier_id;
        return false;
    }
    auto& target_tier = it->second;

    if (target_tier->Put(key, source)) {
        std::unique_lock<std::shared_mutex> lock(map_mutex_);
        key_to_tier_map_[key] = target_tier_id;
        return true;
    }

    return false;
}

bool TieredBackend::Delete(const std::string& key) {
    std::optional<uint64_t> tier_id_opt;
    {
        std::unique_lock<std::shared_mutex> lock(map_mutex_);
        auto it = key_to_tier_map_.find(key);
        if (it != key_to_tier_map_.end()) {
            tier_id_opt = it->second;
            key_to_tier_map_.erase(it);
        }
    }

    if (tier_id_opt) {
        return DeleteFromTier(key, *tier_id_opt);
    }

    return false;
}

bool TieredBackend::MoveData(const std::string& key, uint64_t src_tier_id,
                             uint64_t dest_tier_id) {
    VLOG(1) << "Moving key '" << key << "' from tier " << src_tier_id << " to "
            << dest_tier_id;

    auto src_it = tiers_.find(src_tier_id);
    auto dest_it = tiers_.find(dest_tier_id);
    if (src_it == tiers_.end() || dest_it == tiers_.end()) {
        LOG(ERROR) << "MoveData failed: Invalid tier ID. Source: "
                   << src_tier_id << ", Dest: " << dest_tier_id;
        return false;
    }
    auto& src_tier = src_it->second;
    auto& dest_tier = dest_it->second;

    std::unique_lock<std::shared_mutex> lock(map_mutex_);

    auto key_it = key_to_tier_map_.find(key);
    if (key_it == key_to_tier_map_.end() || key_it->second != src_tier_id) {
        LOG(WARNING) << "MoveData failed: Key '" << key
                     << "' is not in the expected source tier " << src_tier_id;
        return false;
    }

    DataSource source = src_tier->AsDataSource(key);
    if (source.ptr == nullptr) {
        LOG(WARNING) << "Key '" << key << "' disappeared from tier "
                     << src_tier_id << " during move operation.";
        return false;
    }

    if (!dest_tier->Put(key, source)) {
        LOG(WARNING) << "Could not move key '" << key
                     << "' to destination tier " << dest_tier_id
                     << " (likely full).";
        return false;
    }

    if (!src_tier->Delete(key)) {
        LOG(ERROR) << "CRITICAL INCONSISTENCY: Moved key '" << key
                   << "' to tier " << dest_tier_id
                   << " but failed to delete from source " << src_tier_id
                   << ". Attempting rollback.";
        // Attempt to roll back by deleting the key from the destination tier.
        if (!dest_tier->Delete(key)) {
            LOG(FATAL) << "Rollback failed. Data for key '" << key
                       << "' is now duplicated in tiers " << src_tier_id
                       << " and " << dest_tier_id
                       << ". Manual intervention required.";
        }
        // Even if rollback succeeds, the original move operation failed.
        return false;
    }

    key_it->second = dest_tier_id;

    return true;
}

bool TieredBackend::DeleteFromTier(const std::string& key, uint64_t tier_id) {
    auto it = tiers_.find(tier_id);
    if (it == tiers_.end()) {
        return false;
    }
    return it->second->Delete(key);
}

std::optional<uint64_t> TieredBackend::FindKey(const std::string& key) const {
    std::shared_lock<std::shared_mutex> lock(map_mutex_);
    auto it = key_to_tier_map_.find(key);
    if (it != key_to_tier_map_.end()) {
        return it->second;
    }
    return std::nullopt;
}

const CacheTier* TieredBackend::GetTier(uint64_t tier_id) const {
    auto it = tiers_.find(tier_id);
    if (it != tiers_.end()) {
        return it->second.get();
    }
    return nullptr;
}

const DataCopier& TieredBackend::GetDataCopier() const { return *data_copier_; }

}  // namespace mooncake