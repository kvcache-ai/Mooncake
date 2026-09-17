// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "rdma_error_classifier.h"

namespace mooncake::adaptive_congestion_control {
namespace {

Classification makeClassification(OutcomeClass outcome, FailureScope scope,
                                  bool root_failure,
                                  uint32_t vendor_error = 0) {
    return {outcome, scope, root_failure, vendor_error};
}

}  // namespace

Classification classifyCompletion(ibv_wc_status status, uint32_t vendor_error) {
    switch (status) {
        case IBV_WC_SUCCESS:
            return makeClassification(OutcomeClass::kSuccess,
                                      FailureScope::kOperation, false,
                                      vendor_error);
        case IBV_WC_RNR_RETRY_EXC_ERR:
            return makeClassification(OutcomeClass::kReceiverPressure,
                                      FailureScope::kQp, true, vendor_error);
        case IBV_WC_RETRY_EXC_ERR:
        case IBV_WC_RESP_TIMEOUT_ERR:
            return makeClassification(OutcomeClass::kRouteTimeout,
                                      FailureScope::kQp, true, vendor_error);
        case IBV_WC_LOC_LEN_ERR:
        case IBV_WC_LOC_PROT_ERR:
        case IBV_WC_MW_BIND_ERR:
        case IBV_WC_LOC_ACCESS_ERR:
            return makeClassification(OutcomeClass::kLocalConfiguration,
                                      FailureScope::kOperation, true,
                                      vendor_error);
        case IBV_WC_LOC_QP_OP_ERR:
        case IBV_WC_LOC_EEC_OP_ERR:
        case IBV_WC_LOC_RDD_VIOL_ERR:
        case IBV_WC_INV_EECN_ERR:
        case IBV_WC_INV_EEC_STATE_ERR:
            return makeClassification(OutcomeClass::kLocalConfiguration,
                                      FailureScope::kQp, true, vendor_error);
        case IBV_WC_BAD_RESP_ERR:
        case IBV_WC_REM_INV_REQ_ERR:
        case IBV_WC_REM_ACCESS_ERR:
        case IBV_WC_REM_OP_ERR:
        case IBV_WC_REM_INV_RD_REQ_ERR:
        case IBV_WC_REM_ABORT_ERR:
            return makeClassification(OutcomeClass::kRemoteMetadata,
                                      FailureScope::kOperation, true,
                                      vendor_error);
        case IBV_WC_WR_FLUSH_ERR:
            return makeClassification(OutcomeClass::kDerivedFlush,
                                      FailureScope::kQp, false, vendor_error);
        case IBV_WC_FATAL_ERR:
        case IBV_WC_GENERAL_ERR:
        case IBV_WC_TM_ERR:
        case IBV_WC_TM_RNDV_INCOMPLETE:
            return makeClassification(OutcomeClass::kFatal, FailureScope::kQp,
                                      true, vendor_error);
    }
    return makeClassification(OutcomeClass::kFatal, FailureScope::kQp, true,
                              vendor_error);
}

std::optional<Classification> classifyAsyncEvent(ibv_event_type event) {
    switch (event) {
        case IBV_EVENT_QP_FATAL:
        case IBV_EVENT_QP_REQ_ERR:
        case IBV_EVENT_QP_ACCESS_ERR:
        case IBV_EVENT_WQ_FATAL:
        case IBV_EVENT_SRQ_ERR:
            return makeClassification(OutcomeClass::kFatal, FailureScope::kQp,
                                      true);
        case IBV_EVENT_CQ_ERR:
            return makeClassification(OutcomeClass::kFatal, FailureScope::kCq,
                                      true);
        case IBV_EVENT_PATH_MIG_ERR:
            return makeClassification(OutcomeClass::kFatal,
                                      FailureScope::kRoute, true);
        case IBV_EVENT_PORT_ERR:
            return makeClassification(OutcomeClass::kFatal, FailureScope::kPort,
                                      true);
        case IBV_EVENT_DEVICE_FATAL:
            return makeClassification(OutcomeClass::kFatal,
                                      FailureScope::kDevice, true);
        default:
            return std::nullopt;
    }
}

}  // namespace mooncake::adaptive_congestion_control
