#pragma once

// MetadataPort: how code reaches object metadata without holding the service
// that owns it.
//
// The port resolves a tenant's namespace in the NamespaceTable and holds one
// key of its route through a handle: a WriteHandle holds the key exclusively,
// a ReadHandle holds the object shared. A handle is held for its scope and
// lets go of the key when it is destroyed, which is when the route reports a
// write release to the namespace (see route::RouteObserver).
//
// What the service still does around an access it is told through
// MetadataEffects, which the port is built with: dropping the invalid replicas
// of an object a default WriteHandle holds, and erasing an object together
// with every record keyed to it. The port keeps no state of its own besides
// the two references, so it is as cheap to pass around as the table.
//
// Locking: the route's rules hold for every handle. A thread holds one key at
// a time, so nothing in a handle's scope may reach another key through another
// handle; a handle may not be taken inside an object cursor's loop body.

#include <cassert>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "metadata/namespace.h"
#include "metadata/namespace_table.h"
#include "metadata/object_route.h"
#include "tenant_id.h"
#include "types.h"

namespace mooncake {
namespace metadata {

// One object's name: the tenant it is resolved under and its key.
struct ObjectIdentity {
    TenantId tenant_id;
    std::string user_key;
};

// Whether an erase takes the object down for good or makes way for a
// replacement published under the same key lock.
enum class EraseMode {
    kFull,
    kReplace,
};

class WriteHandle;
class ReadHandle;

// What the owner of the metadata does around an access. Both run under the
// held key's lock, and only a WriteHandle calls them: the owner overrides
// them, but nothing else can run them on a guard it happens to hold.
class MetadataEffects {
   protected:
    ~MetadataEffects() = default;

   private:
    friend class WriteHandle;

    // A default WriteHandle holds a published object: drop the replicas that
    // are no longer valid, which may erase the object through `handle`.
    virtual void CleanupInvalidReplicas(WriteHandle& handle) = 0;

    // Erase the object `guard` holds in `ns` and every record keyed to it,
    // ending with the route teardown. The key stays held.
    virtual void Erase(Namespace& ns, const route::WriteGuard& guard,
                       EraseMode erase_mode,
                       const std::vector<std::string>& previous_media_hint) = 0;
};

// The way in to the metadata. Keys are held only through a WriteHandle or a
// ReadHandle built on the port; the namespaces and the effects behind it are
// reachable from those handles alone, so a holder of the port cannot take a
// route lock, or erase an object, around them.
class MetadataPort {
   public:
    // Both outlive the port and every handle taken through it.
    MetadataPort(NamespaceTable& namespaces, MetadataEffects& effects)
        : namespaces_(namespaces), effects_(effects) {}
    MetadataPort(const MetadataPort&) = delete;
    MetadataPort& operator=(const MetadataPort&) = delete;

    // The member keys of `group_id` in the tenant's route, as a snapshot to
    // resolve again through a handle; empty if the tenant or the group is
    // unregistered.
    [[nodiscard]] std::vector<std::string> GroupMembers(
        const TenantId& tenant_id, std::string_view group_id) const {
        const Namespace* ns = namespaces_.Lookup(tenant_id);
        return ns == nullptr ? std::vector<std::string>{}
                             : ns->objects.GroupMembers(group_id);
    }

    // Objects the tenant's namespace holds right now; zero when it does not
    // exist.
    [[nodiscard]] size_t ObjectCount(const TenantId& tenant_id) const {
        const Namespace* ns = namespaces_.Lookup(tenant_id);
        return ns == nullptr ? 0 : ns->objects.ObjectCount();
    }

   private:
    friend class WriteHandle;
    friend class ReadHandle;

    NamespaceTable& namespaces_;
    MetadataEffects& effects_;
};

// Holds one key exclusively for its scope.
class WriteHandle {
   public:
    struct AsIs {};
    // Holds the key only while it holds an object, and leaves that object
    // as it is, without the invalid-replica cleanup: for a pass that acts
    // on what the object's own state says, such as an expiry sweep.
    WriteHandle(const MetadataPort& port, ObjectIdentity object_id, AsIs)
        : port_(port),
          object_id_(std::move(object_id)),
          ns_(port_.namespaces_.Lookup(object_id_.tenant_id)),
          guard_(ns_ == nullptr ? std::optional<route::WriteGuard>()
                                : ns_->objects.Write(object_id_.user_key)) {}

    // Holds the key only while it holds an object; invalid replicas go
    // first, which may tear the object down.
    WriteHandle(const MetadataPort& port, ObjectIdentity object_id)
        : WriteHandle(port, std::move(object_id), AsIs{}) {
        if (IsPublished()) {
            port_.effects_.CleanupInvalidReplicas(*this);
        }
    }

    struct CreateKey {
        bool cleanup_invalid = true;
    };
    // Holds the key whether or not it holds an object, creating the
    // namespace if needed, so the caller can publish one under it.
    WriteHandle(const MetadataPort& port, ObjectIdentity object_id,
                CreateKey options)
        : port_(port),
          object_id_(std::move(object_id)),
          ns_(&port_.namespaces_.GetOrCreate(object_id_.tenant_id)),
          guard_(ns_->objects.WriteKey(object_id_.user_key)) {
        if (options.cleanup_invalid && IsPublished()) {
            port_.effects_.CleanupInvalidReplicas(*this);
        }
    }

    // Holds the key only while `ref` is still its current publication: a
    // key republished since `ref` was taken, or emptied, is not held.
    // The held object is left as it is, without the invalid-replica
    // cleanup: the caller comes back to act on what it judged, and the
    // cleanup's KV sync would bump the tenant epoch a RemoveAll compares
    // its `cleared` against.
    WriteHandle(const MetadataPort& port, const TenantId& tenant_id,
                const route::ObjectRef& ref)
        : port_(port),
          object_id_{tenant_id, ref.key},
          ns_(port_.namespaces_.Lookup(tenant_id)),
          guard_(ns_ == nullptr ? std::optional<route::WriteGuard>()
                                : ns_->objects.Write(ref)) {}

    WriteHandle(const WriteHandle&) = delete;
    WriteHandle& operator=(const WriteHandle&) = delete;

    // Check if metadata exists
    bool Exists() const {
        return IsPublished() && guard_->metadata().IsValid();
    }

    // Holds an object, whether or not a replica is still valid.
    bool IsPublished() const {
        return guard_.has_value() && guard_->has_object();
    }

    bool InProcessing() const {
        return IsPublished() && guard_->state().is_processing;
    }

    bool HasReplicationTask() const {
        return IsPublished() && guard_->state().replication_task;
    }

    const std::string& GetKey() const { return object_id_.user_key; }
    // The tenant the object was resolved under.
    const TenantId& GetTenantId() const { return object_id_.tenant_id; }
    // The held publication; like `Get()`, only while one is held.
    route::ObjectRef GetRef() const { return guard_->ref(); }

    // The namespace policy's say on the held key taking `bytes` more
    // memory, asked before it is allocated; zero bytes asks only whether
    // the namespace admits writes. Only while a key is held. A caller that
    // fails after growing needs no rollback: releasing the key settles the
    // account to what the object holds then, which gives the growth back.
    [[nodiscard]] tl::expected<void, ErrorCode> Grow(uint64_t bytes) const {
        return ns_->Grow(*guard_, bytes);
    }

    // Tells the namespace policy the held object is about to be replaced
    // under this same lock (see NamespacePolicy::OnReplace).
    void BeforeReplace() const { ns_->BeforeReplace(*guard_); }

    // Get metadata (only call when IsPublished() is true)
    ObjectMetadata& Get() { return guard_->metadata(); }

    // The tasks in flight for the object (only call when IsPublished()
    // is true)
    route::ObjectState& GetState() { return guard_->state(); }

    ReplicationTask& GetReplicationTask() {
        return *guard_->state().replication_task;
    }

    // Publishes an object under the held key, which must hold none:
    // builds its metadata in place from `metadata_args`, ObjectMetadata's
    // constructor arguments, with an empty state, and wires a grouped
    // object to its group's shared lease. Only a CreateKey handle holds
    // a key with no object. Returns the new publication's generation.
    template <typename... Args>
    route::Generation Publish(Args&&... metadata_args) {
        assert(guard_.has_value() && !guard_->has_object());
        return guard_->Publish(std::forward<Args>(metadata_args)...);
    }

    // Erases the held object and everything keyed to it; the key stays
    // held, so the caller may publish a replacement under it.
    void Erase(const std::vector<std::string>& previous_media_hint = {}) {
        Erase(EraseMode::kFull, previous_media_hint);
    }
    void Erase(EraseMode erase_mode,
               const std::vector<std::string>& previous_media_hint = {}) {
        port_.effects_.Erase(*ns_, *guard_, erase_mode, previous_media_hint);
    }

    void EraseFromProcessing() {
        if (IsPublished()) {
            guard_->state().is_processing = false;
        }
    }

    void EraseReplicationTask() {
        if (IsPublished()) {
            guard_->state().replication_task.reset();
        }
    }

   private:
    const MetadataPort& port_;
    ObjectIdentity object_id_;
    // Null when the namespace does not exist; a namespace stays put once
    // created, so the pointer outlives the guard.
    Namespace* ns_;
    std::optional<route::WriteGuard> guard_;
};

// Holds one object shared for its scope.
class ReadHandle {
   public:
    // Reads the object without copying its identity, which a batch read
    // would otherwise pay for once per key.
    ReadHandle(const MetadataPort& port, const TenantId& tenant_id,
               const std::string& user_key)
        : guard_(Hold(port.namespaces_.Lookup(tenant_id), user_key)) {}
    ReadHandle(const MetadataPort& port, const ObjectIdentity& object_id)
        : ReadHandle(port, object_id.tenant_id, object_id.user_key) {}

    // Check if metadata exists
    bool Exists() const {
        return IsPublished() && guard_->metadata().IsValid();
    }

    // Holds an object, whether or not a replica is still valid.
    bool IsPublished() const { return guard_.has_value(); }

    bool InProcessing() const {
        return IsPublished() && guard_->state().is_processing;
    }

    // Get metadata (only call when Exists() is true)
    const ObjectMetadata& Get() const { return guard_->metadata(); }

    // The tasks in flight for the object (only call when IsPublished()
    // is true)
    const route::ObjectState& GetState() const { return guard_->state(); }

   private:
    static std::optional<route::ReadGuard> Hold(const Namespace* ns,
                                                const std::string& user_key) {
        if (ns == nullptr) {
            return std::nullopt;
        }
        return ns->objects.Read(user_key);
    }

    std::optional<route::ReadGuard> guard_;
};

}  // namespace metadata
}  // namespace mooncake
