/*
 * Copyright (c) 2025, Alibaba Group Holding Limited;
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include <infiniband/verbs.h>

#include <string>
#include <system_error>

namespace coro_io {
class ib_error_code_category : public std::error_category {
 public:
  const char* name() const noexcept override { return "ibverbs_error_code"; }

  std::string message(int status) const override {
    using namespace std::string_literals;
    switch (static_cast<ibv_wc_status>(status)) {
      case IBV_WC_SUCCESS:
        return "Success";
      case IBV_WC_LOC_LEN_ERR:
        return "Local Length Error";
      case IBV_WC_LOC_QP_OP_ERR:
        return "Local QP Operation Error";
      case IBV_WC_LOC_EEC_OP_ERR:
        return "Local EE Context Operation Error";
      case IBV_WC_LOC_PROT_ERR:
        return "Local Protection Error";
      case IBV_WC_WR_FLUSH_ERR:
        return "Work Request Flushed Error";
      case IBV_WC_MW_BIND_ERR:
        return "Memory Window Bind Error";
      case IBV_WC_BAD_RESP_ERR:
        return "Bad Response Error";
      case IBV_WC_LOC_ACCESS_ERR:
        return "Local Access Error";
      case IBV_WC_REM_INV_REQ_ERR:
        return "Remote Invalid Request Error";
      case IBV_WC_REM_ACCESS_ERR:
        return "Remote Access Error";
      case IBV_WC_REM_OP_ERR:
        return "Remote Operation Error";
      case IBV_WC_RETRY_EXC_ERR:
        return "Retry Exhausted Error";
      case IBV_WC_RNR_RETRY_EXC_ERR:
        return "Receiver Not Ready Retry Exhausted Error";
      case IBV_WC_LOC_RDD_VIOL_ERR:
        return "Local RDD Violation Error";
      case IBV_WC_REM_INV_RD_REQ_ERR:
        return "Remote Invalid RD Request Error";
      case IBV_WC_REM_ABORT_ERR:
        return "Remote Aborted Error";
      case IBV_WC_INV_EECN_ERR:
        return "Invalid EE Context Number Error";
      case IBV_WC_INV_EEC_STATE_ERR:
        return "Invalid EE Context State Error";
      case IBV_WC_FATAL_ERR:
        return "Fatal Error";
      case IBV_WC_RESP_TIMEOUT_ERR:
        return "Response Timeout Error";
      case IBV_WC_GENERAL_ERR:
        return "General Error";
      default:
        return "Unknown Status";
    }
  }
  static auto& instance() {
    static ib_error_code_category instance;
    return instance;
  }
};

inline std::error_code make_error_code(ibv_wc_status e) {
  return {static_cast<int>(e), ib_error_code_category::instance()};
}
}  // namespace coro_io