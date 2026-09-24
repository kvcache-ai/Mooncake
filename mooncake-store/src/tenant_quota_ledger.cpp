#include "tenant_quota_ledger.h"

#include <limits>

namespace mooncake {
namespace {

TenantQuotaResult InvalidArgument() {
    return tl::make_unexpected(TenantQuotaError::kInvalidArgument);
}

TenantQuotaResult AccountingMismatch() {
    return tl::make_unexpected(TenantQuotaError::kAccountingMismatch);
}

bool AddOverflows(uint64_t lhs, uint64_t rhs) {
    return rhs > TenantQuotaAccount::kMaxChargedBytes ||
           lhs > TenantQuotaAccount::kMaxChargedBytes - rhs;
}

}  // namespace

TenantQuotaResult TenantQuotaLedger::AdoptPendingCharge(
    TenantQuotaHandle account, uint64_t bytes) {
    if (account == nullptr || bytes > TenantQuotaAccount::kMaxChargedBytes) {
        return InvalidArgument();
    }
    if (pending_bytes_ != 0 || AddOverflows(TotalChargedBytes(), bytes)) {
        return AccountingMismatch();
    }
    pending_bytes_ = bytes;
    return {};
}

TenantQuotaResult TenantQuotaLedger::SettlePrimaryWrite(
    TenantQuotaHandle account, uint64_t actual_committed_bytes) {
    if (account == nullptr ||
        actual_committed_bytes > TenantQuotaAccount::kMaxChargedBytes) {
        return InvalidArgument();
    }
    if (AddOverflows(pending_bytes_, committed_bytes_)) {
        return AccountingMismatch();
    }
    const uint64_t primary_bytes = pending_bytes_ + committed_bytes_;
    if (actual_committed_bytes > primary_bytes ||
        AddOverflows(primary_bytes, replaced_bytes_)) {
        return AccountingMismatch();
    }

    const uint64_t total_bytes = primary_bytes + replaced_bytes_;
    auto release_result =
        account->Release(total_bytes - actual_committed_bytes);
    if (!release_result) {
        return release_result;
    }
    pending_bytes_ = 0;
    committed_bytes_ = actual_committed_bytes;
    replaced_bytes_ = 0;
    return {};
}

TenantQuotaResult TenantQuotaLedger::SettleAdditional(TenantQuotaHandle account,
                                                      uint64_t pending_bytes,
                                                      uint64_t actual_bytes) {
    if (account == nullptr ||
        pending_bytes > TenantQuotaAccount::kMaxChargedBytes ||
        actual_bytes > TenantQuotaAccount::kMaxChargedBytes) {
        return InvalidArgument();
    }
    if (actual_bytes > pending_bytes ||
        AddOverflows(committed_bytes_, actual_bytes) ||
        AddOverflows(TotalChargedBytes(), actual_bytes)) {
        return AccountingMismatch();
    }

    auto release_result = account->Release(pending_bytes - actual_bytes);
    if (!release_result) {
        return release_result;
    }
    committed_bytes_ += actual_bytes;
    return {};
}

TenantQuotaResult TenantQuotaLedger::RefundPending(TenantQuotaHandle account) {
    if (account == nullptr) {
        return InvalidArgument();
    }
    if (pending_bytes_ == 0) {
        return AccountingMismatch();
    }
    auto release_result = account->Release(pending_bytes_);
    if (!release_result) {
        return release_result;
    }
    pending_bytes_ = 0;
    return {};
}

TenantQuotaResult TenantQuotaLedger::ReleaseCommitted(TenantQuotaHandle account,
                                                      uint64_t bytes) {
    if (account == nullptr || bytes > TenantQuotaAccount::kMaxChargedBytes) {
        return InvalidArgument();
    }
    if (bytes == 0) {
        return {};
    }
    if (bytes > committed_bytes_) {
        return AccountingMismatch();
    }
    auto release_result = account->Release(bytes);
    if (!release_result) {
        return release_result;
    }
    committed_bytes_ -= bytes;
    return {};
}

TenantQuotaResult TenantQuotaLedger::TransferReplacementCharge(
    TenantQuotaHandle account, TenantQuotaLedger& destination) {
    if (account == nullptr || this == &destination) {
        return InvalidArgument();
    }
    if (pending_bytes_ != 0 || destination.replaced_bytes_ != 0 ||
        AddOverflows(committed_bytes_, replaced_bytes_)) {
        return AccountingMismatch();
    }

    const uint64_t transfer_bytes = committed_bytes_ + replaced_bytes_;
    if (transfer_bytes == 0 ||
        AddOverflows(destination.TotalChargedBytes(), transfer_bytes)) {
        return AccountingMismatch();
    }
    destination.replaced_bytes_ = transfer_bytes;
    committed_bytes_ = 0;
    replaced_bytes_ = 0;
    return {};
}

TenantQuotaResult TenantQuotaLedger::ReleaseReplacement(
    TenantQuotaHandle account) {
    if (account == nullptr) {
        return InvalidArgument();
    }
    if (replaced_bytes_ == 0) {
        return AccountingMismatch();
    }
    auto release_result = account->Release(replaced_bytes_);
    if (!release_result) {
        return release_result;
    }
    replaced_bytes_ = 0;
    return {};
}

TenantQuotaResult TenantQuotaLedger::ReleaseAll(TenantQuotaHandle account) {
    if (account == nullptr) {
        return InvalidArgument();
    }
    const uint64_t total_bytes = TotalChargedBytes();
    if (total_bytes == 0) {
        return AccountingMismatch();
    }
    auto release_result = account->Release(total_bytes);
    if (!release_result) {
        return release_result;
    }
    pending_bytes_ = 0;
    committed_bytes_ = 0;
    replaced_bytes_ = 0;
    return {};
}

TenantQuotaResult TenantQuotaLedger::Rebuild(TenantQuotaHandle account,
                                             uint64_t committed_bytes) {
    if (account == nullptr ||
        committed_bytes > TenantQuotaAccount::kMaxChargedBytes) {
        return InvalidArgument();
    }
    pending_bytes_ = 0;
    committed_bytes_ = committed_bytes;
    replaced_bytes_ = 0;
    return {};
}

uint64_t TenantQuotaLedger::TotalChargedBytes() const {
    if (AddOverflows(pending_bytes_, committed_bytes_)) {
        return std::numeric_limits<uint64_t>::max();
    }
    const uint64_t pending_and_committed = pending_bytes_ + committed_bytes_;
    if (AddOverflows(pending_and_committed, replaced_bytes_)) {
        return std::numeric_limits<uint64_t>::max();
    }
    return pending_and_committed + replaced_bytes_;
}

}  // namespace mooncake
