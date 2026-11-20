#pragma once

#include <json/json.h>

#include <mutex>
#include <shared_mutex>
#include <string>

#include "p2p/common/p2p_rpc_types.h"
#include "p2p/common/p2p_types.h"

namespace mooncake {

class RuntimeConfigStore {
   public:
    RuntimeConfigStore() = default;

    P2PWriteRouteConfig getDefaultWriteConfig() const;
    P2PReadRouteConfig getDefaultReadConfig() const;
    Json::Value exportConfig() const;

    bool loadFromJson(const Json::Value& root);
    bool updateWriteConfig(const Json::Value& json);
    void updateReadConfig(const Json::Value& json);

   private:
    static bool applyPatch(P2PWriteRouteConfig& config,
                           const Json::Value& json);
    static void applyPatch(P2PReadRouteConfig& config, const Json::Value& json);

    static Json::Value toJson(const P2PWriteRouteConfig& config);
    static Json::Value toJson(const P2PReadRouteConfig& config);

    mutable std::shared_mutex mu_;
    P2PWriteRouteConfig write_;
    P2PReadRouteConfig read_;
};

}  // namespace mooncake
