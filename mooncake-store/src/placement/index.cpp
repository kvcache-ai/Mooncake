#include "segment/catalog.h"
#include "placement/index.h"

#include <algorithm>
#include <unordered_set>
#include <memory_resource>
#include <functional>
#include <iterator>

namespace mooncake {
PlacementIndex::Entry* PlacementIndex::EntryMap::Find(std::string_view name) {
    auto it = by_name_.find(name);
    return it == by_name_.end() ? nullptr : it->second.get();
}

const PlacementIndex::Entry* PlacementIndex::EntryMap::Find(
    std::string_view name) const {
    auto it = by_name_.find(name);
    return it == by_name_.end() ? nullptr : it->second.get();
}

void PlacementIndex::EntryMap::Insert(std::unique_ptr<Entry> entry) noexcept {
    auto [it, inserted] =
        by_name_.emplace(entry->segment_name, std::move(entry));
    DCHECK(inserted);
    active_.push_back(it->second.get());
}

void PlacementIndex::EntryMap::Erase(const Entry& entry) {
    auto active_it = std::find(active_.begin(), active_.end(), &entry);
    DCHECK(active_it != active_.end());
    *active_it = active_.back();
    active_.pop_back();
    by_name_.erase(entry.segment_name);
}

void PlacementIndex::EntryMap::Clear() {
    active_.clear();
    by_name_.clear();
}

bool PlacementIndex::AddCandidate(std::string_view segment_name,
                                  const AllocationCandidate& candidate,
                                  std::string_view host_id) {
    if (segment_name.empty()) {
        return false;
    }
    const size_t kind_index = KindIndex(candidate.Kind());
    if (kind_index >= kAllocationCandidateKindCount) {
        return false;
    }
    auto& entries = entries_by_kind_[kind_index];
    if (auto* entry = entries.Find(segment_name)) {
        if (!entry->candidates.insert(&candidate).second) {
            return false;
        }
        AddHostMember(host_id, segment_name);
        return true;
    }

    auto entry = std::make_unique<Entry>(
        Entry{std::string(segment_name), candidate.Kind(), {&candidate}});
    entries.Insert(std::move(entry));
    AddHostMember(host_id, segment_name);
    return true;
}

bool PlacementIndex::RemoveCandidate(std::string_view segment_name,
                                     const AllocationCandidate& candidate,
                                     std::string_view host_id) {
    const size_t kind_index = KindIndex(candidate.Kind());
    if (kind_index >= kAllocationCandidateKindCount) {
        return false;
    }
    auto& entries = entries_by_kind_[kind_index];
    auto* entry = entries.Find(segment_name);
    if (!entry) {
        return false;
    }
    auto& candidates = entry->candidates;
    if (candidates.erase(&candidate) == 0) {
        return false;
    }
    RemoveHostMember(host_id, segment_name);
    if (!candidates.empty()) {
        return true;
    }

    entries.Erase(*entry);
    return true;
}

bool PlacementIndex::ReplaceCandidate(std::string_view segment_name,
                                      const AllocationCandidate& expected,
                                      const AllocationCandidate& replacement,
                                      std::string_view host_id) {
    if (expected.Kind() != replacement.Kind()) {
        if (!AddCandidate(segment_name, replacement, host_id)) {
            return false;
        }
        if (RemoveCandidate(segment_name, expected, host_id)) {
            return true;
        }
        (void)RemoveCandidate(segment_name, replacement, host_id);
        return false;
    }

    const size_t kind_index = KindIndex(expected.Kind());
    if (kind_index >= kAllocationCandidateKindCount) {
        return false;
    }
    auto& entries = entries_by_kind_[kind_index];
    auto* entry = entries.Find(segment_name);
    if (!entry) {
        return false;
    }
    auto& candidates = entry->candidates;
    auto candidate_it = candidates.find(&expected);
    if (candidate_it == candidates.end()) {
        return false;
    }
    if (&expected == &replacement) {
        return true;
    }
    if (candidates.find(&replacement) != candidates.end()) {
        return false;
    }
    candidates.erase(candidate_it);
    candidates.insert(&replacement);
    return true;
}

void PlacementIndex::Clear() {
    segments_by_host_.clear();
    for (auto& entries : entries_by_kind_) {
        entries.Clear();
    }
}

const PlacementIndex::Entry* PlacementIndex::Find(
    std::string_view segment_name, AllocationCandidateKind kind) const {
    const size_t kind_index = KindIndex(kind);
    if (kind_index >= kAllocationCandidateKindCount) {
        return nullptr;
    }
    const auto& entries = entries_by_kind_[kind_index];
    return entries.Find(segment_name);
}

void PlacementIndex::GetActiveSegmentNames(
    AllocationCandidateKind kind, std::vector<std::string>& names) const {
    names.clear();
    const size_t kind_index = KindIndex(kind);
    if (kind_index >= kAllocationCandidateKindCount) {
        return;
    }
    const auto active_entries = entries_by_kind_[kind_index].ActiveEntries();
    names.reserve(active_entries.size());
    for (const auto* entry : active_entries) {
        names.push_back(entry->segment_name);
    }
}

void PlacementIndex::AddHostMember(std::string_view host_id,
                                   std::string_view segment_name) {
    if (!host_id.empty()) {
        ++segments_by_host_[std::string(host_id)][std::string(segment_name)];
    }
}

void PlacementIndex::RemoveHostMember(std::string_view host_id,
                                      std::string_view segment_name) {
    if (host_id.empty()) return;
    auto host = segments_by_host_.find(host_id);
    DCHECK(host != segments_by_host_.end());
    auto name = host->second.find(segment_name);
    DCHECK(name != host->second.end());
    auto& member_count = name->second;
    if (--member_count == 0) {
        host->second.erase(name);
        if (host->second.empty()) segments_by_host_.erase(host);
    }
}

void PlacementIndex::VisitHostOrderedSegmentNames(
    std::string_view writer_host_id, std::string_view key,
    const std::function<bool(std::string_view)>& visit) const {
    if (writer_host_id.empty() || segments_by_host_.empty()) return;
    auto start = segments_by_host_.lower_bound(writer_host_id);
    if (start == segments_by_host_.end()) start = segments_by_host_.begin();

    const size_t key_hash = std::hash<std::string_view>{}(key);
    // The same name can occur on multiple hosts. Batch per-visit dedup storage.
    std::pmr::monotonic_buffer_resource scratch;
    std::pmr::unordered_set<std::string_view> visited_names(&scratch);
    auto host = start;
    do {
        const auto& names = host->second;
        const size_t offset = key_hash % names.size();
        for (size_t i = 0; i < names.size(); ++i) {
            const auto& name = names.nth((offset + i) % names.size())->first;
            if (visited_names.insert(name).second && visit(name)) return;
        }
        if (++host == segments_by_host_.end()) host = segments_by_host_.begin();
    } while (host != start);
}

std::optional<UUID> ScopedPlacementReadAccess::GetOwnerClientId(
    std::string_view segment_name) const {
    return catalog_.FindOwnerClientId(segment_name);
}

}  // namespace mooncake
