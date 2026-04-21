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
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <variant>

#include "context.hpp"
#include "coro_connection.hpp"
#include "ylt/coro_rpc/impl/errno.h"
#include "ylt/easylog.hpp"
#include "ylt/struct_pack/compatible.hpp"
#include "ylt/struct_pack/derived_helper.hpp"
#include "ylt/struct_pack/type_calculate.hpp"
#include "ylt/util/type_traits.h"

namespace coro_rpc::internal {
// TODO: remove this later
template <bool is_conn, typename First>
auto get_return_type() {
  if constexpr (is_conn) {
    using T = typename First::return_type;
    if constexpr (std::is_void_v<T>) {
      return;
    }
    else {
      return T{};
    }
  }
  else {
    return First{};
  }
}
template <typename rpc_protocol>
using rpc_context = std::shared_ptr<context_info_t<rpc_protocol>>;

using rpc_conn = std::shared_ptr<coro_connection>;
template <typename rpc_protocol, typename serialize_proto, auto func,
          typename Self = void>
inline std::pair<coro_rpc::err_code, std::string> execute(
    std::string_view data, rpc_context<rpc_protocol> &context_info,
    Self *self = nullptr) {
  using namespace std::string_literals;
  using T = decltype(func);
  using param_type = util::function_parameters_t<T>;
  using return_type = util::function_return_type_t<T>;

  if constexpr (!std::is_void_v<param_type>) {
    using First = std::tuple_element_t<0, param_type>;
    constexpr bool is_conn = requires { typename First::return_type; };
    if constexpr (is_conn) {
      static_assert(std::is_void_v<return_type>,
                    "The return_type must be void");
    }

    using conn_return_type = decltype(get_return_type<is_conn, First>());
    constexpr bool has_coro_conn_v =
        std::is_convertible_v<context_base<conn_return_type, rpc_protocol>,
                              First>;
    auto args = util::get_args<has_coro_conn_v, param_type>();

    bool is_ok = true;
    constexpr size_t size = std::tuple_size_v<decltype(args)>;
    if constexpr (size > 0) {
      is_ok = serialize_proto::deserialize_to(args, data);
    }

    if (!is_ok) {
      // fix for single argument function want add more compatible argument
      if constexpr (size == 1) {
        std::tuple<decltype(args) &> args_wrapper = args;
        is_ok = serialize_proto::deserialize_to(args_wrapper, data);
      }
      else if constexpr (size > 1) {
        if constexpr (struct_pack::get_type_code<std::tuple<std::remove_cvref_t<
                          std::tuple_element_t<0, decltype(args)>>>>() ==
                      struct_pack::get_type_code<decltype(args)>()) {
          std::tuple<decltype(std::get<0>(args))> args_wrapper =
              std::get<0>(args);
          is_ok = serialize_proto::deserialize_to(args_wrapper, data);
        }
      }
      if (!is_ok) {
        AS_UNLIKELY {
          return std::pair{err_code{errc::invalid_rpc_arguments},
                           "invalid rpc arg"s};
        }
      }
    }

    if constexpr (std::is_void_v<return_type>) {
      if constexpr (std::is_void_v<Self>) {
        if constexpr (has_coro_conn_v) {
          // call void func(coro_conn, args...)
          std::apply(func, std::tuple_cat(
                               std::forward_as_tuple(
                                   context_base<conn_return_type, rpc_protocol>(
                                       context_info)),
                               std::move(args)));
        }
        else {
          // call void func(args...)
          std::apply(func, std::move(args));
        }
      }
      else {
        if constexpr (has_coro_conn_v) {
          // call void self->func(coro_conn, args...)
          std::apply(
              func, std::tuple_cat(
                        std::forward_as_tuple(
                            *self, context_base<conn_return_type, rpc_protocol>(
                                       context_info)),
                        std::move(args)));
        }
        else {
          // call void self->func(args...)
          std::apply(func, std::tuple_cat(std::forward_as_tuple(*self),
                                          std::move(args)));
        }
      }
      return std::pair{err_code{}, serialize_proto::serialize()};
    }
    else {
      if constexpr (std::is_void_v<Self>) {
        // call return_type func(args...)

        return std::pair{err_code{}, serialize_proto::serialize(
                                         std::apply(func, std::move(args)))};
      }
      else {
        // call return_type self->func(args...)

        return std::pair{err_code{},
                         serialize_proto::serialize(std::apply(
                             func, std::tuple_cat(std::forward_as_tuple(*self),
                                                  std::move(args))))};
      }
    }
  }
  else {
    if constexpr (std::is_void_v<return_type>) {
      if constexpr (std::is_void_v<Self>) {
        func();
      }
      else {
        (self->*func)();
      }
      return std::pair{err_code{}, serialize_proto::serialize()};
    }
    else {
      if constexpr (std::is_void_v<Self>) {
        return std::pair{err_code{}, serialize_proto::serialize(func())};
      }
      else {
        return std::pair{err_code{},
                         serialize_proto::serialize((self->*func)())};
      }
    }
  }
}

template <typename rpc_protocol, typename serialize_proto, auto func,
          typename Self = void>
inline async_simple::coro::Lazy<std::pair<coro_rpc::err_code, std::string>>
execute_coro(std::string_view data, Self *self = nullptr) {
  using namespace std::string_literals;
  using T = decltype(func);
  using param_type = util::function_parameters_t<T>;
  using return_type = typename get_type_t<
      typename util::function_return_type_t<T>::ValueType>::type;

  if constexpr (!std::is_void_v<param_type>) {
    using First = std::tuple_element_t<0, param_type>;
    constexpr bool is_conn = requires { typename First::return_type; };
    static_assert(
        !is_conn,
        "context<T> is not allowed as parameter in coroutine function");

    bool is_ok = true;
    constexpr size_t size = std::tuple_size_v<param_type>;
    param_type args;
    if constexpr (size > 0) {
      is_ok = serialize_proto::deserialize_to(args, data);
    }
    if (!is_ok)
      AS_UNLIKELY {
        co_return std::make_pair(coro_rpc::errc::invalid_rpc_arguments,
                                 "invalid rpc function arguments"s);
      }
    if constexpr (std::is_void_v<return_type>) {
      if constexpr (std::is_void_v<Self>) {
        // call void func(args...)
        co_await std::apply(func, std::move(args));
      }
      else {
        // call void self->func(args...)
        co_await std::apply(func, std::tuple_cat(std::forward_as_tuple(*self),
                                                 std::move(args)));
      }
      co_return std::pair{err_code{}, serialize_proto::serialize()};
    }
    else {
      if constexpr (std::is_void_v<Self>) {
        // call return_type func(args...)
        co_return std::pair{err_code{},
                            serialize_proto::serialize(
                                co_await std::apply(func, std::move(args)))};
      }
      else {
        // call return_type self->func(args...)
        co_return std::pair{
            err_code{}, serialize_proto::serialize(co_await std::apply(
                            func, std::tuple_cat(std::forward_as_tuple(*self),
                                                 std::move(args))))};
      }
    }
  }
  else {
    if constexpr (std::is_void_v<return_type>) {
      if constexpr (std::is_void_v<Self>) {
        co_await func();
      }
      else {
        // clang-format off
        co_await (self->*func)();
        // clang-format on
      }
      co_return std::pair{err_code{}, serialize_proto::serialize()};
    }
    else {
      if constexpr (std::is_void_v<Self>) {
        co_return std::pair{err_code{},
                            serialize_proto::serialize(co_await func())};
      }
      else {
        // clang-format off
          co_return std::pair{err_code{},serialize_proto::serialize(co_await (self->*func)())};
        // clang-format on
      }
    }
  }
}
}  // namespace coro_rpc::internal