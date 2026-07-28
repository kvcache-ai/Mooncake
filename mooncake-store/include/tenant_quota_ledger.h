#pragma once

#include <cstdint>

#include "tenant_quota.h"

namespace mooncake {

// Per-object accounting state for quota bytes already charged to a stable
// TenantQuotaAccount. The ledger never owns the account and never releases
// quota implicitly; every mutation receives the account handle explicitly.
class TenantQuotaLedger {
   public:
    TenantQuotaLedger() = default;
    TenantQuotaLedger(const TenantQuotaLedger&) = delete;
    TenantQuotaLedger& operator=(const TenantQuotaLedger&) = delete;
    TenantQuotaLedger(TenantQuotaLedger&&) = delete;
    TenantQuotaLedger& operator=(TenantQuotaLedger&&) = delete;
    ~TenantQuotaLedger() = default;

    // Records bytes that the caller has already charged with TryCharge().
    TenantQuotaResult AdoptPendingCharge(TenantQuotaHandle account,
                                         uint64_t bytes);

    // Settles the initial object allocation. Bytes not retained by
    // actual_bytes are returned to the account.
    TenantQuotaResult SettleInitial(TenantQuotaHandle account,
                                    uint64_t actual_bytes);

    // Settles a temporary task charge into this object's committed bytes. The
    // caller erases or clears the task field only after this succeeds.
    TenantQuotaResult SettleAdditional(TenantQuotaHandle account,
                                       uint64_t pending_bytes,
                                       uint64_t actual_bytes);

    TenantQuotaResult RefundPending(TenantQuotaHandle account);
    TenantQuotaResult ReleaseCommitted(TenantQuotaHandle account,
                                       uint64_t bytes);

    // Transfers this ledger's committed/replacement contribution into the
    // destination's replacement bucket without changing global charged bytes.
    TenantQuotaResult TransferReplacementCharge(TenantQuotaHandle account,
                                                TenantQuotaLedger& destination);
    TenantQuotaResult ReleaseReplacement(TenantQuotaHandle account);
    TenantQuotaResult ReleaseAll(TenantQuotaHandle account);

    // Rebuild is only valid while data-plane charge/release is quiescent. It
    // changes local state only; the caller rebuilds the account separately.
    TenantQuotaResult Rebuild(TenantQuotaHandle account,
                              uint64_t committed_bytes);

    uint64_t PendingBytes() const { return pending_bytes_; }
    uint64_t CommittedBytes() const { return committed_bytes_; }
    uint64_t ReplacedBytes() const { return replaced_bytes_; }
    uint64_t TotalChargedBytes() const;

   private:
    uint64_t pending_bytes_{0};
    uint64_t committed_bytes_{0};
    uint64_t replaced_bytes_{0};
};

}  // namespace mooncake
