#pragma once

// TenantQuotaBinding: one tenant's side of the quota, as the data plane uses
// it. It names the account the tenant is bound to in the quota table, or none
// for an unmetered tenant, in which case every charge succeeds and every
// release is a no-op. How large the account is belongs to TenantQuotaManager;
// what one object owes is kept in that object's own ledger.
//
// The binding is a value: copying it copies the reference to the account, and
// the account outlives every copy because the quota table keeps one stable
// account per tenant id. The tenant registry hands it out with the tenant it
// belongs to, so a caller resolves both in one lookup and the tenant itself
// knows nothing of quotas.

#include <cstdint>
#include <limits>
#include <utility>

#include <glog/logging.h>
#include <ylt/util/tl/expected.hpp>

#include "object_metadata.h"
#include "tenant_quota.h"
#include "types.h"

namespace mooncake {

class TenantQuotaBinding {
   public:
    // A charge held for an operation still in flight. It is given back when
    // the reservation goes out of scope, unless Commit() handed it on to what
    // accounts for it from then on: a task's pending bytes or an object's
    // ledger.
    class Reservation {
       public:
        Reservation(Reservation&& other) noexcept
            : account_(other.account_),
              bytes_(std::exchange(other.bytes_, 0)) {}
        Reservation(const Reservation&) = delete;
        Reservation& operator=(const Reservation&) = delete;
        Reservation& operator=(Reservation&&) = delete;
        ~Reservation() { ReleaseTo(account_, bytes_); }

        [[nodiscard]] uint64_t bytes() const { return bytes_; }
        // The bytes, now owed by the caller instead of this reservation.
        [[nodiscard]] uint64_t Commit() { return std::exchange(bytes_, 0); }

       private:
        friend class TenantQuotaBinding;
        Reservation(TenantQuotaAccount* account, uint64_t bytes)
            : account_(account), bytes_(bytes) {}

        TenantQuotaAccount* account_;
        uint64_t bytes_;
    };

    // Unmetered: quotas are off for this tenant.
    TenantQuotaBinding() = default;

    // Metered, bound to the tenant's account in the quota table.
    explicit TenantQuotaBinding(TenantQuotaAccount& account)
        : account_(&account) {}

    // Charges `bytes` to the account. Zero bytes charges nothing and only
    // checks that the tenant still admits writes.
    [[nodiscard]] tl::expected<void, ErrorCode> Charge(uint64_t bytes) const {
        if (account_ == nullptr) {
            return {};
        }
        auto result = account_->TryCharge(bytes);
        if (result) {
            return {};
        }
        switch (result.error().error) {
            case TenantQuotaError::kTenantNotRegistered:
                return tl::make_unexpected(ErrorCode::TENANT_NOT_REGISTERED);
            case TenantQuotaError::kQuotaExceeded:
                return tl::make_unexpected(ErrorCode::TENANT_QUOTA_EXCEEDED);
            case TenantQuotaError::kInvalidArgument:
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            default:
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }

    // Charge, held by a reservation the caller commits or drops.
    [[nodiscard]] tl::expected<Reservation, ErrorCode> Reserve(
        uint64_t bytes) const {
        auto charged = Charge(bytes);
        if (!charged) {
            return tl::make_unexpected(charged.error());
        }
        return Reservation(account_, bytes);
    }

    void Release(uint64_t bytes) const { ReleaseTo(account_, bytes); }

    // Null only for an unmetered tenant, which the quota ledger takes as
    // nothing to charge.
    [[nodiscard]] TenantQuotaHandle Account() const { return account_; }

    // What an object charges once written: its size for every completed
    // MEMORY replica, saturating at the 64-bit range.
    [[nodiscard]] static uint64_t MemoryCharge(const ObjectMetadata& metadata) {
        const auto completed_replicas =
            metadata.CountReplicas([](const Replica& replica) {
                return replica.is_memory_replica() && replica.is_completed();
            });
        const unsigned __int128 charge =
            static_cast<unsigned __int128>(metadata.size) * completed_replicas;
        return charge > std::numeric_limits<uint64_t>::max()
                   ? std::numeric_limits<uint64_t>::max()
                   : static_cast<uint64_t>(charge);
    }

   private:
    static void ReleaseTo(TenantQuotaAccount* account, uint64_t bytes) {
        if (account == nullptr || bytes == 0) {
            return;
        }
        if (!account->Release(bytes)) {
            LOG(ERROR) << "tenant quota release mismatch bytes=" << bytes;
        }
    }

    TenantQuotaAccount* account_ = nullptr;
};

}  // namespace mooncake
