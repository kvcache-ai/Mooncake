#pragma once

// ObjectView: stack-only, immovable, read-only view that binds a published
// ObjectMetadata to the authoritative TenantId and key owned by its
// metadata::Namespace and route::KeySlot.
//
// Only metadata::Namespace can construct an ObjectView from a held guard or
// cursor position, so helpers that take a view instead of loose
// (ObjectMetadata&, key, tenant_id) arguments cannot be passed a mismatched
// key or an unnormalized request TenantId — while ObjectMetadata itself stores
// neither string.
//
// Lifetime: every reference inside an ObjectView is non-owning. An ObjectView
// is invalidated as soon as any of the following occurs:
//   - the Guard / MetadataAccessor that produced it is released or destroyed;
//   - TearDown() / Erase() is called on the held WriteGuard /
//     MetadataAccessorRW (destroying the publication's ObjectMetadata even
//     while the key slot remains locked);
//   - the ObjectRoute::Cursor advances past the visited object or is destroyed;
//   - the owning Namespace is destroyed.
//
// Copy, move, assignment, and heap allocation are deleted so views rely on
// C++17/20 guaranteed copy elision at call boundaries and cannot be stored in
// containers, class members, or asynchronous callbacks.

#include <cstddef>
#include <string>

#include "object_metadata.h"
#include "tenant_id.h"

namespace mooncake {
namespace metadata {

class Namespace;

class ObjectView {
   public:
    ObjectView(const ObjectView&) = delete;
    ObjectView& operator=(const ObjectView&) = delete;
    ObjectView(ObjectView&&) = delete;
    ObjectView& operator=(ObjectView&&) = delete;

    static void* operator new(std::size_t) = delete;
    static void* operator new[](std::size_t) = delete;

    [[nodiscard]] const TenantId& tenant_id() const noexcept {
        return tenant_id_;
    }
    [[nodiscard]] const std::string& key() const noexcept { return key_; }
    [[nodiscard]] const ObjectMetadata& metadata() const noexcept {
        return metadata_;
    }

   private:
    friend class Namespace;

    ObjectView(const TenantId& tenant_id, const std::string& key,
               const ObjectMetadata& metadata) noexcept
        : tenant_id_(tenant_id), key_(key), metadata_(metadata) {}

    const TenantId& tenant_id_;
    const std::string& key_;
    const ObjectMetadata& metadata_;
};

}  // namespace metadata
}  // namespace mooncake
