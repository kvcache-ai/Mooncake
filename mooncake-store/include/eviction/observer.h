#pragma once

// EvictionObserver: what is told about an eviction run without taking part in
// it. An observer sees objects read-only, cannot change what the run does,
// and a run without any observer is complete. The per-object calls run under
// the object's write lock, so they must not block or take another key's lock.

#include <string>

#include "eviction/policy.h"
#include "eviction/report.h"
#include "object_metadata.h"
#include "tenant_id.h"

namespace mooncake {
namespace eviction {

// Which namespaces a run covers: the whole pool, or one tenant's.
class Scope {
   public:
    static Scope All() { return Scope(nullptr); }
    // `tenant_id` outlives the scope.
    static Scope Of(const TenantId& tenant_id) { return Scope(&tenant_id); }

    // Null for the whole pool.
    [[nodiscard]] const TenantId* tenant_id() const { return tenant_id_; }

   private:
    explicit Scope(const TenantId* tenant_id) : tenant_id_(tenant_id) {}
    const TenantId* tenant_id_;
};

// An object as an observer sees it.
struct ObjectView {
    const TenantId& tenant_id;
    const std::string& key;
    const ObjectMetadata& metadata;
};

class EvictionObserver {
   public:
    // An object was reclaimed; `view` shows it as reclaiming left it.
    virtual void OnApplied(const ObjectView& /*view*/,
                           const Effect& /*effect*/) {}
    // An object the run reached was left alone.
    virtual void OnSkipped(const ObjectView& /*view*/, SkipReason /*reason*/) {}
    // The run is over and holds no key lock.
    virtual void OnRunEnd(const Scope& /*scope*/, const Report& /*report*/) {}

   protected:
    ~EvictionObserver() = default;
};

}  // namespace eviction
}  // namespace mooncake
