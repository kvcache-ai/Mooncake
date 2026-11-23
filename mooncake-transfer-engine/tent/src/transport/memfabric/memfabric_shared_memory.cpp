// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#include "tent/transport/memfabric/memfabric_shared_memory.h"
#include <algorithm>
#include <bitset>
#include <iterator>
#include <random>
#include <sstream>
#include <string_view>
namespace mooncake {
namespace tent {
namespace {
constexpr size_t kMaxDescriptors = 4096;
constexpr size_t kMaxBlobSize = 1U << 18;
constexpr uint16_t kEntityIdLimit = 511;
// HYBM deduplicates native entities by ID within a process. Keep the ID
// reserved until the native entity is destroyed, including during quarantine.
class EntityIds {
   public:
    static EntityIds& instance() {
        static auto* ids = new EntityIds;
        return *ids;
    }
    std::optional<uint16_t> acquire(uint16_t preferred) {
        std::lock_guard<std::mutex> lock(mutex_);
        const size_t start = preferred >= kFirst && preferred < kEntityIdLimit
                                 ? preferred - kFirst
                                 : 0;
        for (size_t i = 0; i < used_.size(); ++i) {
            const auto index = (start + i) % used_.size();
            if (!used_[index]) {
                used_.set(index);
                return static_cast<uint16_t>(index + kFirst);
            }
        }
        return std::nullopt;
    }
    void release(uint16_t id) {
        std::lock_guard<std::mutex> lock(mutex_);
        used_.reset(id - kFirst);
    }

   private:
    static constexpr uint16_t kFirst = 64;
    std::mutex mutex_;
    std::bitset<kEntityIdLimit - kFirst> used_;
};
Status pending(const std::string& why) {
    return Status::TooManyRequests("MemFabric GVA cleanup pending: " + why);
}
Status pending(const char* why, int code) {
    return pending(std::string(why) + ", ret=" + std::to_string(code));
}
Status nativeError(const char* op, int code = 0) {
    return Status::InternalError(std::string("HYBM GVA ") + op +
                                 " failed, ret=" + std::to_string(code));
}
Status invalidDescriptorField(const char* field, const char* reason) {
    return Status::InvalidArgument(std::string("GVA descriptor field '") +
                                   field + "' " + reason);
}
bool blobValid(const nlohmann::json& descriptor) {
    if (!descriptor.is_array() || descriptor.empty() ||
        descriptor.size() > kMaxBlobSize) {
        return false;
    }
    for (const auto& byte : descriptor) {
        if (!byte.is_number_unsigned() || byte.get<uint64_t>() > 255) {
            return false;
        }
    }
    return true;
}
bool containsRange(uint64_t base, uint64_t length, uint64_t address,
                   size_t size) {
    return address >= base && address - base < length &&
           size <= length - (address - base);
}
}  // namespace
MemFabricSharedMemory::MemFabricSharedMemory(MemFabricGvaConfig config,
                                             std::string segment_name)
    : config_(std::move(config)),
      segment_name_(std::move(segment_name)),
      available_(config_.enabled && MemFabricGvaApi::LoadLibrary()) {
    std::random_device random;
    std::ostringstream generation;
    for (int index = 0; index < 4; ++index) {
        generation << std::hex << random() << '-';
    }
    generation_ = generation.str();
}
MemFabricSharedMemory::~MemFabricSharedMemory() { (void)close(); }
Status MemFabricSharedMemory::validateConfig() const {
    if (!available_) {
        return Status::NotImplemented(
            "MemFabric GVA requires HYBM headers and library symbols");
    }
    for (auto pair : {std::make_pair(config_.hbm_size, config_.hbm_window),
                      std::make_pair(config_.dram_size, config_.dram_window)}) {
        if (pair.first > pair.second ||
            (config_.rank_count &&
             pair.second > UINT64_MAX / config_.rank_count)) {
            return Status::InvalidArgument(
                "GVA budget/window arithmetic overflow");
        }
    }
    return Status::OK();
}
Status MemFabricSharedMemory::bindDevice() {
    const auto ret = MemFabricGvaApi::bind(config_.device);
    if (ret) return nativeError("bind device", ret);
    return Status::OK();
}
Status MemFabricSharedMemory::initialize() {
    std::lock_guard<std::mutex> lock(mutex_);
    CHECK_STATUS(validateConfig());
    CHECK_STATUS(bindDevice());
    if (init_state_ == InitState::Ready) return Status::OK();
    if (init_state_ == InitState::Uninitialized) {
        const auto ret = MemFabricGvaApi::initialize(config_.device);
        if (ret) return nativeError("initialize", ret);
        init_state_ = InitState::RuntimeInitialized;
    }
    if (!entity_) {
        auto id = EntityIds::instance().acquire(config_.entity);
        if (!id) {
            return Status::InvalidArgument("MemFabric GVA entity id exhausted");
        }
        config_.entity = *id;
        entity_ = MemFabricGvaApi::create(config_);
        if (!entity_) {
            EntityIds::instance().release(*id);
            return nativeError("create entity");
        }
        const auto ret = MemFabricGvaApi::reserve(entity_);
        if (ret) {
            destroyEntity();
            return nativeError("reserve windows", ret);
        }
    }
    for (size_t index = 0; index < window_bases_.size(); ++index) {
        const auto window =
            index == 0 ? config_.hbm_window : config_.dram_window;
        if (!window) continue;
        window_bases_[index] =
            MemFabricGvaApi::base(entity_, index + kGvaMemoryTypeDevice);
        if (!window_bases_[index] ||
            config_.rank_count > (UINT64_MAX - window_bases_[index]) / window) {
            return Status::InvalidArgument("Invalid gva window base/range");
        }
    }
    std::vector<uint8_t> blob;
    const auto ret = MemFabricGvaApi::exportBlob(entity_, nullptr, blob);
    if (ret) return nativeError("export entity", ret);
    entity_blob_ = blob;
    init_state_ = InitState::Ready;
    return Status::OK();
}
void MemFabricSharedMemory::destroyEntity() {
    MemFabricGvaApi::destroy(entity_);
    entity_ = nullptr;
    EntityIds::instance().release(config_.entity);
}
MemFabricSharedMemory::Json MemFabricSharedMemory::makeIdentity() const {
    return Json{{"protocol", config_.protocol},
                {"address_mode", "identity"},
                {"segment_name", segment_name_},
                {"generation", generation_},
                {"rank", config_.rank},
                {"ranks", config_.rank_count},
                {"entity", config_.entity},
                {"device", config_.device},
                {"hbm_window", config_.hbm_window},
                {"dram_window", config_.dram_window},
                {"hbm_size", config_.hbm_size},
                {"dram_size", config_.dram_size},
                {"entity_blob", entity_blob_}};
}
Status MemFabricSharedMemory::allocate(void** out, size_t size,
                                       uint32_t memory_type,
                                       MemoryOptions& options) {
    if (!out) return Status::InvalidArgument("Null GVA output");
    *out = nullptr;
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_copies_ || copy_failed_) {
        return pending("GVA copies retain resources");
    }
    if (memory_type != kGvaMemoryTypeDevice &&
        memory_type != kGvaMemoryTypeHost) {
        return Status::InvalidArgument("Unknown GVA memory type");
    }
    if (slices_.size() >= kMaxDescriptors) {
        return Status::InvalidArgument("Too many local GVA allocations");
    }
    const auto index = memory_type - kGvaMemoryTypeDevice;
    const auto budget = index == 0 ? config_.hbm_size : config_.dram_size;
    const auto window = index == 0 ? config_.hbm_window : config_.dram_window;
    if (!size || consumed_[index] > budget ||
        size > budget - consumed_[index]) {
        return Status::InvalidArgument(
            "GVA allocation exceeds remaining native capacity");
    }
    if (init_state_ != InitState::Ready) {
        return Status::InvalidArgument(
            "MemFabric GVA context is not initialized");
    }
    void* slice = MemFabricGvaApi::allocate(entity_, memory_type, size);
    if (!slice) return nativeError("allocate slice (null handle)");
    auto rollback = [this, slice]() {
        const auto ret = MemFabricGvaApi::free(entity_, slice);
        if (!ret) return Status::OK();
        rollback_.push_back(slice);
        return nativeError("allocation rollback (resources retained)", ret);
    };
    const auto gva = MemFabricGvaApi::address(entity_, slice);
    const auto slice_size = MemFabricGvaApi::size(entity_, slice);
    const auto rank_base = window_bases_[index] + config_.rank * window;
    const bool in_window = gva >= rank_base;
    const auto offset = in_window ? gva - rank_base : 0;
    // HYBM's bump cursor is not rewound by free, including failed fixed-address
    // validation. Account for actual offsets, rather than assuming offset zero.
    consumed_[index] =
        in_window ? std::max(consumed_[index], offset + slice_size) : budget;
    // offset <= budget and slice_size <= budget - offset keep offset +
    // slice_size inside the rank window, so no separate overflow check is
    // needed here.
    const bool range = in_window && offset <= budget && slice_size &&
                       slice_size <= budget - offset;
    // Network DRAM is accessed through its host VA; HYBM may assign a
    // different device alias when registering that host memory.
    const uint32_t address_type = memory_type == kGvaMemoryTypeHost &&
                                          (config_.protocol == "device_rdma" ||
                                           config_.protocol == "device_urma")
                                      ? kGvaMemoryTypeHost
                                      : kGvaMemoryTypeDevice;
    uint64_t local_address = 0;
    const auto first_ret =
        range ? MemFabricGvaApi::translate(gva, address_type, local_address)
              : 0;
    bool valid = range && slice_size == size && !first_ret &&
                 local_address == gva &&
                 (!options.global_address || options.global_address == gva);
    if (!valid) {
        CHECK_STATUS(rollback());
        if (first_ret) {
            return nativeError("translate allocation start", first_ret);
        }
        return Status::InvalidArgument(
            "Native allocation must fit its rank window and requested "
            "identical VA");
    }
    Slice record;
    record.handle = slice;
    record.memory_type = memory_type;
    record.length = size;
    record.descriptor = makeIdentity();
    record.descriptor["type"] = memory_type;
    record.descriptor["address"] = gva;
    record.descriptor["size"] = size;
    record.descriptor["window_base"] = window_bases_[index];
    record.descriptor["offset"] = offset;
    record.descriptor["slice_generation"] = ++slice_generation_;
    if (!slices_.emplace(gva, std::move(record)).second) {
        CHECK_STATUS(rollback());
        return Status::InvalidArgument("Native GVA address is already owned");
    }
    options.global_address = gva;
    options.type = MEMFABRIC;
    options.location = "npu:" + std::to_string(config_.device);
    *out = reinterpret_cast<void*>(gva);
    return Status::OK();
}
Status MemFabricSharedMemory::publish(
    const std::vector<std::pair<void*, size_t>>& ranges,
    std::vector<std::string>& descriptors) {
    CHECK_STATUS(bindDevice());
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<Slice*> selected;
    selected.reserve(ranges.size());
    for (const auto& [ptr, size] : ranges) {
        const auto found = slices_.find(reinterpret_cast<uint64_t>(ptr));
        if (found == slices_.end() || found->second.length != size) {
            return Status::InvalidArgument(
                "Only a complete trusted GVA slice can be registered");
        }
        selected.push_back(&found->second);
    }
    std::vector<Json> staged;
    std::vector<std::string> serialized;
    staged.reserve(selected.size());
    serialized.reserve(selected.size());
    for (const auto* slice : selected) {
        auto descriptor = slice->descriptor;
        if (!descriptor.contains("slice_blob")) {
            std::vector<uint8_t> blob;
            const auto ret =
                MemFabricGvaApi::exportBlob(entity_, slice->handle, blob);
            if (ret) return nativeError("export slice", ret);
            descriptor["slice_blob"] = blob;
        }
        serialized.push_back(descriptor.dump());
        staged.push_back(std::move(descriptor));
    }
    for (size_t index = 0; index < selected.size(); ++index) {
        selected[index]->descriptor.swap(staged[index]);
        selected[index]->published = true;
    }
    descriptors.swap(serialized);
    return Status::OK();
}
Status MemFabricSharedMemory::unpublish(void* ptr,
                                        std::optional<size_t> size) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_copies_ || copy_failed_) {
        return pending("GVA copies retain resources");
    }
    auto slice = slices_.find(reinterpret_cast<uint64_t>(ptr));
    if (slice == slices_.end() || (size && slice->second.length != *size)) {
        return Status::InvalidArgument("Unknown GVA slice or size");
    }
    slice->second.published = false;
    return Status::OK();
}
Status MemFabricSharedMemory::free(void* ptr, size_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_copies_ || copy_failed_) {
        return pending("GVA copies retain resources");
    }
    auto slice = slices_.find(reinterpret_cast<uint64_t>(ptr));
    if (slice == slices_.end() || slice->second.length != size) {
        return Status::InvalidArgument("Unknown GVA slice or size");
    }
    if (slice->second.published) return pending("slice is still registered");
    const auto ret = MemFabricGvaApi::free(entity_, slice->second.handle);
    if (ret) return nativeError("free slice", ret);
    slices_.erase(slice);
    return Status::OK();
}
Status MemFabricSharedMemory::validateDescriptor(
    const Json& descriptor, const std::string& peer_segment_name) const {
    if (!descriptor.is_object()) {
        return Status::InvalidArgument("GVA descriptor must be a JSON object");
    }
    for (const char* key :
         {"rank", "ranks", "hbm_window", "dram_window", "hbm_size", "dram_size",
          "type", "address", "size", "window_base", "offset"}) {
        auto field = descriptor.find(key);
        if (field == descriptor.end()) {
            return invalidDescriptorField(key, "is missing");
        }
        if (!field->is_number_unsigned()) {
            return invalidDescriptorField(key, "must be an unsigned integer");
        }
    }
    for (const char* key :
         {"address_mode", "protocol", "segment_name", "generation"}) {
        auto field = descriptor.find(key);
        if (field == descriptor.end()) {
            return invalidDescriptorField(key, "is missing");
        }
        if (!field->is_string()) {
            return invalidDescriptorField(key, "must be a string");
        }
    }
    if (peer_segment_name.empty()) {
        return invalidDescriptorField("segment_name",
                                      "has no expected segment name");
    }
    const std::pair<const char*, std::string_view> identity[] = {
        {"address_mode", "identity"},
        {"protocol", config_.protocol},
        {"segment_name", peer_segment_name},
    };
    for (const auto& [key, expected] : identity) {
        if (descriptor.at(key).get_ref<const std::string&>() != expected) {
            return invalidDescriptorField(key,
                                          "does not match the expected value");
        }
    }
    const std::pair<const char*, uint64_t> layout[] = {
        {"ranks", config_.rank_count},
        {"hbm_window", config_.hbm_window},
        {"dram_window", config_.dram_window},
        {"hbm_size", config_.hbm_size},
        {"dram_size", config_.dram_size},
    };
    for (const auto& [key, expected] : layout) {
        if (descriptor.at(key).get<uint64_t>() != expected) {
            return invalidDescriptorField(key,
                                          "does not match the local layout");
        }
    }
    const auto rank = descriptor.at("rank").get<uint64_t>();
    if (rank >= config_.rank_count || rank == config_.rank) {
        return invalidDescriptorField("rank", "is outside the peer ranks");
    }
    const auto type = descriptor.at("type").get<uint64_t>();
    if (type != kGvaMemoryTypeDevice && type != kGvaMemoryTypeHost) {
        return invalidDescriptorField("type", "is not a supported memory type");
    }
    const auto address = descriptor.at("address").get<uint64_t>();
    const auto size = descriptor.at("size").get<uint64_t>();
    const auto offset = descriptor.at("offset").get<uint64_t>();
    const auto base = descriptor.at("window_base").get<uint64_t>();
    const auto budget =
        type == kGvaMemoryTypeDevice ? config_.hbm_size : config_.dram_size;
    const auto window =
        type == kGvaMemoryTypeDevice ? config_.hbm_window : config_.dram_window;
    if (!size) {
        return invalidDescriptorField("size", "must be nonzero");
    }
    if (base != window_bases_[type - kGvaMemoryTypeDevice]) {
        return invalidDescriptorField("window_base",
                                      "does not match the local base");
    }
    const auto rank_base = base + rank * window;
    if (offset > budget) {
        return invalidDescriptorField("offset", "exceeds the rank window");
    }
    if (size > budget - offset) {
        return invalidDescriptorField(
            "size", "exceeds the remaining allocation budget");
    }
    if (address != rank_base + offset) {
        return invalidDescriptorField(
            "address", "does not match the rank base and offset");
    }
    for (const char* key : {"entity_blob", "slice_blob"}) {
        auto field = descriptor.find(key);
        if (field == descriptor.end()) {
            return invalidDescriptorField(key, "is missing");
        }
        if (!blobValid(*field)) {
            return invalidDescriptorField(
                key, "must be a nonempty bounded byte array");
        }
    }
    return Status::OK();
}
MemFabricSharedMemory::RemoteSlice& MemFabricSharedMemory::recordSlice(
    ImportedRank& peer, const Json& descriptor) {
    const auto address = descriptor.at("address").get<uint64_t>();
    auto& slice = peer.slices[address];
    if (slice.descriptor.is_null()) slice.descriptor = descriptor;
    return slice;
}
Status MemFabricSharedMemory::importPeer(ImportedRank& peer,
                                         const Json& descriptor) {
    auto& slice = recordSlice(peer, descriptor);
    if (slice.imported) return Status::OK();
    const auto ret = MemFabricGvaApi::importBlob(
        entity_, false,
        descriptor.at("slice_blob").get<std::vector<uint8_t>>());
    if (ret) {
        return nativeError("import slice (rank retained for cleanup)", ret);
    }
    slice.imported = true;
    return Status::OK();
}
Status MemFabricSharedMemory::verifyPeerMapping(uint32_t rank,
                                                const RemoteSlice& slice) {
    uint32_t reach = 0;
    const auto ret = MemFabricGvaApi::reach(entity_, rank, reach);
    if (ret) return nativeError("query direct reachability", ret);
    if (!(reach & 1u)) {
        return Status::NotImplemented(
            "Peer lacks HYBM MTE direct device access");
    }
    const auto address = slice.descriptor.at("address").get<uint64_t>();
    const auto size = slice.descriptor.at("size").get<uint64_t>();
    uint64_t first = 0, last = 0;
    auto code =
        MemFabricGvaApi::translate(address, kGvaMemoryTypeDevice, first);
    if (code) return nativeError("translate peer start", code);
    code = MemFabricGvaApi::translate(address + size - 1, kGvaMemoryTypeDevice,
                                      last);
    if (code) return nativeError("translate peer end", code);
    if (first != address || last != address + size - 1) {
        return Status::InvalidArgument(
            "Peer allocation has no identical contiguous device VA");
    }
    return Status::OK();
}
Status MemFabricSharedMemory::map(const std::string& peer_segment_name,
                                  const std::string& descriptor,
                                  uint64_t address, size_t size) {
    return mapOrPrepare(peer_segment_name, descriptor, address, size, true);
}
Status MemFabricSharedMemory::prepare(const std::string& peer_segment_name,
                                      const std::string& descriptor,
                                      uint64_t address, size_t size) {
    return mapOrPrepare(peer_segment_name, descriptor, address, size, false);
}
Status MemFabricSharedMemory::mapOrPrepare(const std::string& peer_segment_name,
                                           const std::string& descriptor,
                                           uint64_t address, size_t size,
                                           bool direct) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_copies_ || copy_failed_) {
        return pending("GVA copies retain resources");
    }
    if (init_state_ != InitState::Ready) {
        return Status::InvalidArgument(
            "MemFabric GVA context is not initialized");
    }
    if (direct && config_.protocol != "device_sdma") {
        return Status::NotImplemented(
            "Network GVA does not provide a direct device mapping");
    }
    if (descriptor.size() > (16U << 20)) {
        return Status::InvalidArgument("GVA descriptor bundle exceeds 16 MiB");
    }
    try {
        auto descriptors = Json::parse(descriptor);
        if (!descriptors.is_array()) descriptors = Json::array({descriptors});
        if (descriptors.size() > kMaxDescriptors) {
            return Status::InvalidArgument("Invalid GVA allocation bundle");
        }
        const Json* requested = nullptr;
        std::map<uint64_t, uint64_t> ranges;
        // An empty bundle throws here and is reported as malformed below.
        const auto& first = descriptors.at(0);
        for (const auto& desc : descriptors) {
            CHECK_STATUS(validateDescriptor(desc, peer_segment_name));
            for (const char* key : {"rank", "generation", "entity_blob"}) {
                if (desc.at(key) != first.at(key)) {
                    return invalidDescriptorField(
                        key, "differs between allocations in the bundle");
                }
            }
            const auto base = desc.at("address").get<uint64_t>();
            const auto length = desc.at("size").get<uint64_t>();
            if (!ranges.emplace(base, length).second) {
                return Status::InvalidArgument("Duplicate GVA allocation");
            }
            if (base == address && length == size) requested = &desc;
        }
        if (!requested) {
            return Status::InvalidArgument(
                "Map requires a complete published allocation");
        }
        const auto rank = first.at("rank").get<uint32_t>();
        uint64_t end = 0;
        for (const auto& [base, length] : ranges) {
            if (base < end) {
                return Status::InvalidArgument("Overlapping GVA allocations");
            }
            end = base + length;
        }
        // Validate the entire bundle against retained descriptors before any
        // native import, preserving other allocations' active state on failure.
        CHECK_STATUS(validateRetainedPeers(rank, descriptors, peer_segment_name,
                                           *requested));
        return importBundle(rank, descriptors, peer_segment_name, *requested);
    } catch (const Json::exception&) {
        return Status::InvalidArgument("Malformed GVA allocation bundle");
    }
}
Status MemFabricSharedMemory::validateRetainedPeers(
    uint32_t rank, const Json& descriptors,
    const std::string& peer_segment_name, const Json& requested) const {
    const auto existing = imported_ranks_.find(rank);
    if (existing == imported_ranks_.end() || existing->second.cleaned) {
        return Status::OK();
    }
    const auto& peer = existing->second;
    if (peer.segment_name != peer_segment_name ||
        peer.generation != requested.at("generation").get<std::string>() ||
        peer.entity_blob != requested.at("entity_blob")) {
        return pending("old peer generation must exit first");
    }
    const auto& slices = peer.slices;
    size_t retained = slices.size();
    for (const auto& desc : descriptors) {
        const auto base = desc.at("address").get<uint64_t>();
        const auto next = slices.lower_bound(base);
        if (next != slices.end() && next->first == base) {
            if (desc != next->second.descriptor) {
                return pending("peer allocation descriptor changed");
            }
            continue;
        }
        if (++retained > kMaxDescriptors) {
            return Status::InvalidArgument(
                "Too many retained peer allocations");
        }
        const auto length = desc.at("size").get<uint64_t>();
        const auto prev =
            next == slices.begin() ? slices.end() : std::prev(next);
        if ((next != slices.end() && next->first - base < length) ||
            (prev != slices.end() &&
             base - prev->first <
                 prev->second.descriptor.at("size").get<uint64_t>())) {
            return Status::InvalidArgument(
                "New allocation overlaps retained peer allocation");
        }
    }
    return Status::OK();
}
Status MemFabricSharedMemory::importBundle(uint32_t rank,
                                           const Json& descriptors,
                                           const std::string& peer_segment_name,
                                           const Json& requested) {
    auto& peer = imported_ranks_[rank];
    if (peer.cleaned) peer = ImportedRank{};
    if (peer.segment_name.empty()) {
        peer.segment_name = peer_segment_name;
        peer.generation = requested.at("generation").get<std::string>();
        peer.entity_blob = requested.at("entity_blob");
    }
    // Record the requested allocation before HYBM can acquire resources.
    auto& slice = recordSlice(peer, requested);
    slice.active = false;
    const auto import_ret = MemFabricGvaApi::importBlob(
        entity_, true, peer.entity_blob.get<std::vector<uint8_t>>());
    if (import_ret) return nativeError("import entity", import_ret);
    for (const auto& desc : descriptors) {
        CHECK_STATUS(importPeer(peer, desc));
    }
    auto ret = MemFabricGvaApi::map(entity_);
    if (ret) {
        return pending(
            "map peer allocations (retry when reciprocal imports are ready)",
            ret);
    }
    if (config_.protocol == "device_sdma") {
        CHECK_STATUS(verifyPeerMapping(rank, slice));
    } else {
        ret = MemFabricGvaApi::connect(entity_, rank);
        if (ret) return pending("connect peer (retry when ready)", ret);
    }
    slice.publication = requested.dump();
    slice.active = true;
    return Status::OK();
}
Status MemFabricSharedMemory::cleanupPeer(uint32_t rank, ImportedRank& peer) {
    if (peer.cleaned) return Status::OK();
    for (const auto& [base, slice] : peer.slices) {
        if (slice.active) return pending("another allocation is active");
    }
    CHECK_STATUS(bindDevice());
    auto ret = MemFabricGvaApi::remove(entity_, rank);
    if (ret) return pending("remove imported rank", ret);
    ret = MemFabricGvaApi::unmap(entity_, rank);
    if (ret) return pending("unmap rank", ret);
    for (const auto& [base, slice] : peer.slices) {
        const auto size = slice.descriptor.at("size").get<uint64_t>();
        uint64_t va = 0;
        for (const auto address : {base, base + size - 1}) {
            const auto code =
                MemFabricGvaApi::translate(address, kGvaMemoryTypeDevice, va);
            if (code != -12) {
                return pending("GVA registry still contains imported mappings",
                               code);
            }
        }
    }
    peer.cleaned = true;
    return Status::OK();
}
Status MemFabricSharedMemory::unmap(const std::string& peer_segment_name,
                                    uint64_t address) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_copies_ || copy_failed_) {
        return pending("GVA copies retain resources");
    }
    for (auto& [rank, peer] : imported_ranks_) {
        if (peer.segment_name != peer_segment_name) continue;
        auto slice = peer.slices.find(address);
        if (slice == peer.slices.end()) continue;
        slice->second.active = false;
        return cleanupPeer(rank, peer);
    }
    return Status::InvalidArgument("Unknown GVA mapping");
}
uint32_t MemFabricSharedMemory::memoryTypeForRange(uint64_t address,
                                                   size_t size) const {
    for (const auto& [base, slice] : slices_) {
        if (slice.published &&
            containsRange(base, slice.length, address, size)) {
            return slice.memory_type;
        }
    }
    for (const auto& [rank, peer] : imported_ranks_) {
        if (peer.cleaned) continue;
        for (const auto& [base, slice] : peer.slices) {
            if (slice.active &&
                containsRange(base, slice.descriptor.at("size").get<uint64_t>(),
                              address, size)) {
                return slice.descriptor.at("type").get<uint32_t>();
            }
        }
    }
    return 0;
}
Status MemFabricSharedMemory::validateCopyTarget(
    const std::string& peer_segment_name, uint64_t address,
    const std::string& descriptor) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [rank, peer] : imported_ranks_) {
        if (peer.cleaned || peer.segment_name != peer_segment_name) continue;
        const auto it = peer.slices.find(address);
        if (it == peer.slices.end()) continue;
        const auto& slice = it->second;
        if (slice.active && slice.publication == descriptor) {
            return Status::OK();
        }
    }
    return Status::InvalidArgument(
        "Remote GVA publication differs from prepared allocation");
}
Status MemFabricSharedMemory::beginCopy(uint64_t source, uint64_t destination,
                                        size_t size, uint32_t& direction) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!size || size > UINT64_MAX - source ||
        size > UINT64_MAX - destination) {
        return Status::InvalidArgument("Invalid GVA copy range");
    }
    if (copy_failed_) return pending("native copy completion remains unknown");
    const auto src_type = memoryTypeForRange(source, size);
    const auto dst_type = memoryTypeForRange(destination, size);
    if (!src_type || !dst_type) {
        return Status::InvalidArgument(
            "GVA copy requires published local or active mapped peer slices");
    }
    // HYBM_GLOBAL_{HOST,DEVICE}_TO_GLOBAL_{HOST,DEVICE}.
    direction =
        (src_type == kGvaMemoryTypeDevice ? kGvaCopySourceDevice
                                          : kGvaCopySourceHost) +
        (dst_type == kGvaMemoryTypeDevice ? kGvaCopyDestinationDevice
                                          : kGvaCopyDestinationHost);
    ++pending_copies_;
    return Status::OK();
}
template <typename Copy>
Status MemFabricSharedMemory::runPinnedCopy(size_t count, Copy copy,
                                            const char* operation) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (copy_failed_) {
            auto status =
                pending("previous native copy completion remains unknown");
            pending_copies_ -= count;
            return status;
        }
    }
    // beginCopy pins the resources. Blocking native calls leave mutex_ free.
    const auto bound = MemFabricGvaApi::bind(config_.device);
    const auto result = bound ? bound : copy();
    const bool drained = bound || !result ||
                         (config_.protocol == "device_sdma" &&
                          MemFabricGvaApi::wait(entity_) == 0);
    auto status = result ? nativeError(operation, result) : Status::OK();
    releaseCopy(count, !drained);
    return status;
}
Status MemFabricSharedMemory::runBatchCopy(
    const std::vector<MemFabricGvaCopy>& copies, uint32_t direction) {
    if (copies.empty()) return Status::OK();
    return runPinnedCopy(
        copies.size(),
        [&] { return MemFabricGvaApi::batchCopy(entity_, copies, direction); },
        "synchronous batch copy");
}
void MemFabricSharedMemory::releaseCopy(size_t count, bool completion_unknown) {
    std::lock_guard<std::mutex> lock(mutex_);
    // beginCopy pinned exactly count; every caller releases exactly once.
    pending_copies_ -= count;
    copy_failed_ |= completion_unknown;
}
Status MemFabricSharedMemory::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_copies_ || copy_failed_) {
        return pending("GVA copies retain resources");
    }
    if (!slices_.empty()) return pending("local slices still allocated");
    for (const auto& entry : imported_ranks_) {
        if (!entry.second.cleaned) return pending("peer mappings remain");
    }
    if (init_state_ == InitState::Uninitialized) return Status::OK();
    CHECK_STATUS(bindDevice());
    for (auto iterator = rollback_.begin(); iterator != rollback_.end();) {
        const auto ret = MemFabricGvaApi::free(entity_, *iterator);
        if (ret) return pending("allocation rollback failed", ret);
        iterator = rollback_.erase(iterator);
    }
    if (entity_) {
        const auto ret = MemFabricGvaApi::unreserve(entity_);
        if (ret) return pending("window release failed", ret);
        destroyEntity();
        entity_blob_ = Json{};
    }
    MemFabricGvaApi::uninitialize();
    init_state_ = InitState::Uninitialized;
    consumed_ = {{0, 0}};
    window_bases_ = {{0, 0}};
    imported_ranks_.clear();
    return Status::OK();
}
}  // namespace tent
}  // namespace mooncake
