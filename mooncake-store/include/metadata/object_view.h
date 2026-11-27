#pragma once

// ObjectView / ConstObjectView: stack-only, non-owning views that bind a
// published ObjectMetadata (and, for mutable views, its route::ObjectState)
// to the authoritative TenantId and key owned by its metadata::Namespace and
// route::KeySlot.
//
// Only metadata::Namespace can construct these views from a held guard or
// cursor position, so helpers that take a view instead of loose
// (ObjectMetadata&, key, tenant_id) arguments cannot be passed a mismatched
// key or an unnormalized request TenantId — while ObjectMetadata itself stores
// neither string.

#include <string>

#include "metadata/object_state.h"
#include "object_metadata.h"
#include "tenant_id.h"

namespace mooncake {
namespace metadata {

class Namespace;
class ObjectView;

class ConstObjectView {
   public:
    [[nodiscard]] const TenantId& tenant_id() const noexcept {
        return *tenant_id_;
    }
    [[nodiscard]] const std::string& key() const noexcept { return *key_; }
    [[nodiscard]] const ObjectMetadata& metadata() const noexcept {
        return *metadata_;
    }

   private:
    friend class Namespace;
    friend class ObjectView;

    ConstObjectView(const TenantId& tenant_id, const std::string& key,
                    const ObjectMetadata& metadata) noexcept
        : tenant_id_(&tenant_id), key_(&key), metadata_(&metadata) {}

    const TenantId* tenant_id_;
    const std::string* key_;
    const ObjectMetadata* metadata_;
};

class ObjectView {
   public:
    [[nodiscard]] const TenantId& tenant_id() const noexcept {
        return *tenant_id_;
    }
    [[nodiscard]] const std::string& key() const noexcept { return *key_; }
    [[nodiscard]] ObjectMetadata& metadata() const noexcept {
        return *metadata_;
    }
    [[nodiscard]] route::ObjectState& state() const noexcept { return *state_; }

    operator ConstObjectView() const noexcept {
        return ConstObjectView(*tenant_id_, *key_, *metadata_);
    }

   private:
    friend class Namespace;

    ObjectView(const TenantId& tenant_id, const std::string& key,
               ObjectMetadata& metadata, route::ObjectState& state) noexcept
        : tenant_id_(&tenant_id),
          key_(&key),
          metadata_(&metadata),
          state_(&state) {}

    const TenantId* tenant_id_;
    const std::string* key_;
    ObjectMetadata* metadata_;
    route::ObjectState* state_;
};

}  // namespace metadata
}  // namespace mooncake
