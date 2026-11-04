// Copyright 2026 Mooncake Authors

#ifndef MOONCAKE_LOCAL_BUFFER_REGISTRATIONS_H_
#define MOONCAKE_LOCAL_BUFFER_REGISTRATIONS_H_

#include <cstddef>
#include <unordered_map>

#include "error.h"

namespace mooncake {

// Local bookkeeping for transports that may skip registration or publish an
// allocation base rather than the caller's address. Callers hold their register
// mutex. The callbacks commit local metadata separately from publication, so a
// publication failure can be retried without deleting a peer's descriptor.
class LocalBufferRegistrations {
   public:
    template <class Add, class Publish>
    int add(void* address, void* base, size_t length, const Add& add_local,
            bool update_metadata, const Publish& publish) {
        if (addresses_.count(address)) return ERR_ADDRESS_OVERLAPPED;
        auto entry = bases_.find(base);
        if (entry != bases_.end()) {
            if (entry->second.removed || entry->second.length != length)
                return ERR_INVALID_ARGUMENT;
            ++entry->second.references;
        } else {
            const int rc = add_local();
            if (rc != 0) return rc;
            bases_.emplace(base, Publication{length, 1, false});
        }
        addresses_.emplace(address, base);
        return update_metadata ? publish() : 0;
    }

    template <class Remove, class Publish>
    int remove(void* address, const Remove& remove_local, bool update_metadata,
               const Publish& publish) {
        auto registration = addresses_.find(address);
        if (registration == addresses_.end()) return 0;
        auto entry = bases_.find(registration->second);
        auto& publication = entry->second;
        if (publication.references == 1 && !publication.removed) {
            const int rc = remove_local(entry->first);
            if (rc != 0) return rc;
            publication.removed = true;
        }
        if (update_metadata) {
            const int rc = publish();
            if (rc != 0) return rc;
        }
        if (--publication.references == 0) bases_.erase(entry);
        addresses_.erase(registration);
        return 0;
    }

    bool containsBase(void* base) const { return bases_.count(base) != 0; }

    void* baseFor(void* address) const {
        auto entry = addresses_.find(address);
        return entry == addresses_.end() ? nullptr : entry->second;
    }

   private:
    struct Publication {
        size_t length;
        size_t references;
        bool removed;
    };
    std::unordered_map<void*, void*> addresses_;
    std::unordered_map<void*, Publication> bases_;
};

}  // namespace mooncake

#endif  // MOONCAKE_LOCAL_BUFFER_REGISTRATIONS_H_
