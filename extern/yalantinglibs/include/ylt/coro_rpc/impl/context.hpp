/*
 * Copyright (c) 2023, Alibaba Group Holding Limited;
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

#include <async_simple/coro/Lazy.h>

#include <any>
#include <cstdint>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>
#include <variant>
#include <ylt/easylog.hpp>

#include "coro_connection.hpp"
#include "ylt/coro_rpc/impl/errno.h"
#include "ylt/util/type_traits.h"
#include "ylt/util/utils.hpp"

namespace coro_rpc {
/*!
 *
 * @tparam return_msg_type
 * @tparam coro_connection
 */
template <typename return_msg_type, typename rpc_protocol>
class context_base {
 protected:
  std::shared_ptr<context_info_t<rpc_protocol>> self_;
  typename rpc_protocol::req_header &get_req_head() { return self_->req_head_; }

  bool check_status() {
    auto old_flag = self_->status_.exchange(context_status::start_response);
    if (old_flag != context_status::init)
      AS_UNLIKELY {
        ELOG_ERROR << "response message more than one time";
        return false;
      }

    if (self_->has_closed())
      AS_UNLIKELY {
        ELOG_DEBUG << "response_msg failed: connection has been closed";
        return false;
      }
    return true;
  }

 public:
  /*!
   * Construct a context by a share pointer of context Concept
   * instance
   * @param a share pointer for coro_connection
   */
  context_base(std::shared_ptr<context_info_t<rpc_protocol>> context_info)
      : self_(std::move(context_info)) {
    if (self_->conn_) {
      self_->conn_->set_rpc_return_by_callback();
    }
  };
  context_base() = default;

  using return_type = return_msg_type;

  void response_error(coro_rpc::err_code error_code,
                      std::string_view error_msg) {
    if (!check_status())
      AS_UNLIKELY { return; };
    ELOGI << "rpc error in function:" << self_->get_rpc_function_name()
          << ". error code:" << error_code.ec << ". message : " << error_msg;
    self_->conn_->template response_error<rpc_protocol>(
        error_code, error_msg, self_->req_head_,
        std::move(self_->complete_handler_));
  }
  void response_error(coro_rpc::err_code error_code) {
    response_error(error_code, error_code.message());
  }
  /*!
   * Send response message
   *
   * The `return_msg_type` must be constructed by these `args`.
   *
   * If the connection has already sent response message,
   * an error log will be reported.
   *
   * @tparam Args
   * @param args
   */
  template <typename... Args>
  void response_msg(Args &&...args) {
    if (!check_status())
      AS_UNLIKELY { return; };
    if constexpr (std::is_same_v<return_msg_type, void>) {
      static_assert(sizeof...(args) == 0, "illegal args");
      std::visit(
          [&]<typename serialize_proto>(const serialize_proto &) {
            self_->conn_->template response_msg<rpc_protocol>(
                serialize_proto::serialize(),
                std::move(self_->resp_attachment_), self_->req_head_,
                std::move(self_->complete_handler_));
          },
          *rpc_protocol::get_serialize_protocol(self_->req_head_));
    }
    else {
      static_assert(
          requires { return_msg_type{std::forward<Args>(args)...}; },
          "constructed return_msg_type failed by illegal args");
      return_msg_type ret{std::forward<Args>(args)...};
      std::visit(
          [&]<typename serialize_proto>(const serialize_proto &) {
            self_->conn_->template response_msg<rpc_protocol>(
                serialize_proto::serialize(ret),
                std::move(self_->resp_attachment_), self_->req_head_,
                std::move(self_->complete_handler_));
          },
          *rpc_protocol::get_serialize_protocol(self_->req_head_));

      // complete_handler_(std::move(conn_), std::move(ret));
    }
    /*finish here*/
    self_->status_ = context_status::finish_response;
  }
  const context_info_t<rpc_protocol> *get_context_info() const noexcept {
    return self_.get();
  }
  context_info_t<rpc_protocol> *get_context_info() noexcept {
    return self_.get();
  }
};

template <typename T>
struct get_type_t {
  using type = T;
};

template <typename T>
struct get_type_t<async_simple::coro::Lazy<T>> {
  using type = T;
};

template <auto func>
inline auto get_return_type() {
  using T = decltype(func);
  using param_type = util::function_parameters_t<T>;
  using return_type =
      typename get_type_t<util::function_return_type_t<T>>::type;
  if constexpr (std::is_void_v<param_type>) {
    if constexpr (std::is_void_v<return_type>) {
      return;
    }
    else {
      return return_type{};
    }
  }
  else {
    using First = std::tuple_element_t<0, param_type>;
    constexpr bool is_conn =
        util::is_specialization<First, context_base>::value;

    if constexpr (is_conn) {
      using U = typename First::return_type;
      if constexpr (std::is_void_v<U>) {
        return;
      }
      else {
        return U{};
      }
    }
    else if constexpr (!std::is_same_v<void, return_type>) {
      return return_type{};
    }
    else {
      return;
    }
  }
}
}  // namespace coro_rpc