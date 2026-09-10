#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "p2p/common/p2p_types.h"
#include "types.h"
#include "utils.h"

namespace mooncake {

/**
 * @brief Unsynchronized in-memory index for one P2P route shard.
 */
class P2PRouteTable final {
   private:
    using RouteMap = std::unordered_map<std::string, P2PRouteEntry, StringHash,
                                        std::equal_to<>>;

   public:
    struct MutationResult {
        bool created_key{false};
        bool removed_key{false};
    };

    struct CleanupResult {
        size_t removed_routes{0};
        size_t removed_key_count{0};
    };

    using Mutation = tl::expected<MutationResult, ErrorCode>;
    using PreWithdraw = std::function<ErrorCode()>;

    auto Publish(std::string_view key, uint64_t object_size,
                 const P2PRouteLocation& location,
                 uint64_t max_client_per_key = 0) -> Mutation;

    auto Withdraw(std::string_view key, const P2PRouteLocation& location)
        -> Mutation;
    auto Withdraw(std::string_view key, const P2PRouteLocation& location,
                  const PreWithdraw& pre_withdraw) -> Mutation;

    bool RouteExists(std::string_view key) const;
    std::optional<P2PRouteEntry> GetRoute(std::string_view key) const;
    std::vector<std::string> ListRouteKeys() const;
    size_t GetRouteKeyCount() const;

    CleanupResult RemoveLocation(const P2PRouteLocation& location);
    bool RemoveKey(std::string_view key);
    size_t Clear();

   private:
    static size_t CountOwnerClients(const P2PRouteEntry& entry);
    void RemoveReverseIndex(std::string_view key,
                            const P2PRouteLocation& location);
    void RemoveAllReverseIndexes(std::string_view key,
                                 const P2PRouteEntry& entry);

    RouteMap routes_;
    std::unordered_map<P2PRouteLocation, std::unordered_set<std::string_view>,
                       P2PRouteLocationHash>
        keys_by_location_;
};

}  // namespace mooncake
