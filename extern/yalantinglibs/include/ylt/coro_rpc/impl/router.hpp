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
#include <ylt/coro_rpc/impl/errno.h>
#include <ylt/util/function_name.h>
#include <ylt/util/type_traits.h>

#include <functional>
#include <memory>
#include <sstream>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>
#include <ylt/easylog.hpp>
#include <ylt/struct_pack/md5_constexpr.hpp>

#include "rpc_execute.hpp"
#include "ylt/coro_rpc/impl/expected.hpp"
#include "ylt/coro_rpc/impl/protocol/coro_rpc_protocol.hpp"

namespace coro_rpc {

template <typename rpc_protocol>
using rpc_context = std::shared_ptr<context_info_t<rpc_protocol>>;

namespace protocol {
template <typename T, auto func>
concept has_gen_register_key = requires() {
  T::template gen_register_key<func>();
};

template <typename rpc_protocol,
          template <typename...> typename map_t = std::unordered_map>

class router {
 public:
  using router_handler_t =
      std::function<std::pair<coro_rpc::err_code, std::string>(
          std::string_view, rpc_context<rpc_protocol> &context_info,
          typename rpc_protocol::supported_serialize_protocols protocols)>;

  using coro_router_handler_t = std::function<
      async_simple::coro::Lazy<std::pair<coro_rpc::err_code, std::string>>(
          std::string_view,
          typename rpc_protocol::supported_serialize_protocols protocols)>;

  using route_key = typename rpc_protocol::route_key_t;

  const std::string &get_name(const route_key &key) {
    static std::string empty_string;
    if (auto it = id2name_.find(key); it != id2name_.end()) {
      return it->second;
    }
    else
      return empty_string;
  }

 private:
  std::unordered_map<route_key, router_handler_t> handlers_;
  std::unordered_map<route_key, coro_router_handler_t> coro_handlers_;
  std::unordered_map<route_key, std::string> id2name_;

  // See https://gcc.gnu.org/bugzilla/show_bug.cgi?id=100611
  // We use this struct instead of lambda for workaround
  template <auto Func, typename Self>
  struct execute_visitor {
    std::string_view data;
    Self *self;
    template <typename serialize_protocol>
    async_simple::coro::Lazy<std::pair<coro_rpc::err_code, std::string>>
    operator()(const serialize_protocol &) {
      return internal::execute_coro<rpc_protocol, serialize_protocol, Func>(
          data, self);
    }
  };

  template <auto Func>
  struct execute_visitor<Func, void> {
    std::string_view data;
    template <typename serialize_protocol>
    async_simple::coro::Lazy<std::pair<coro_rpc::err_code, std::string>>
    operator()(const serialize_protocol &) {
      return internal::execute_coro<rpc_protocol, serialize_protocol, Func>(
          data);
    }
  };

  template <auto func, typename Self>
  void regist_one_handler(Self *self) {
    if (self == nullptr)
      AS_UNLIKELY { ELOG_CRITICAL << "null connection!"; }

    route_key key{};

    if constexpr (has_gen_register_key<rpc_protocol, func>) {
      key = rpc_protocol::template gen_register_key<func>();
    }
    else {
      key = auto_gen_register_key<func>();
    }

    regist_one_handler_impl<func>(self, key);
  }

  template <auto func>
  static constexpr route_key auto_gen_register_key() {
    constexpr auto name = get_func_name<func>();
    constexpr auto id =
        struct_pack::MD5::MD5Hash32Constexpr(name.data(), name.length());
    return id;
  }

  template <auto func, typename Self>
  void regist_one_handler_impl(Self *self, const route_key &key) {
    if (self == nullptr)
      AS_UNLIKELY { ELOG_CRITICAL << "null connection!"; }

    constexpr auto name = get_func_name<func>();
    using return_type = util::function_return_type_t<decltype(func)>;
    if constexpr (util::is_specialization_v<return_type,
                                            async_simple::coro::Lazy>) {
      auto it = coro_handlers_.emplace(
          key,
          [self](
              std::string_view data,
              typename rpc_protocol::supported_serialize_protocols protocols) {
            execute_visitor<func, Self> visitor{data, self};
            return std::visit(visitor, protocols);
          });
      if (!it.second) {
        ELOG_CRITICAL << "duplication function " << name << " registered!";
      }
    }
    else {
      auto it = handlers_.emplace(
          key,
          [self](
              std::string_view data, rpc_context<rpc_protocol> &context_info,
              typename rpc_protocol::supported_serialize_protocols protocols) {
            return std::visit(
                [data, &context_info, self]<typename serialize_protocol>(
                    const serialize_protocol &obj) mutable {
                  return internal::execute<rpc_protocol, serialize_protocol,
                                           func>(data, context_info, self);
                },
                protocols);
          });
      if (!it.second) {
        ELOG_CRITICAL << "duplication function " << name << " registered!";
      }
    }

    id2name_.emplace(key, name);
  }

  template <auto func>
  void regist_one_handler() {
    route_key key{};
    if constexpr (has_gen_register_key<rpc_protocol, func>) {
      key = rpc_protocol::template gen_register_key<func>();
    }
    else {
      key = auto_gen_register_key<func>();
    }
    regist_one_handler_impl<func>(key);
  }

  template <auto func>
  void regist_one_handler_impl(const route_key &key) {
    static_assert(!std::is_member_function_pointer_v<decltype(func)>,
                  "register member function but lack of the parent object");
    using return_type = util::function_return_type_t<decltype(func)>;

    constexpr auto name = get_func_name<func>();
    if constexpr (util::is_specialization_v<return_type,
                                            async_simple::coro::Lazy>) {
      auto it = coro_handlers_.emplace(
          key,
          [](std::string_view data,
             typename rpc_protocol::supported_serialize_protocols protocols) {
            execute_visitor<func, void> visitor{data};
            return std::visit(visitor, protocols);
          });
      if (!it.second) {
        ELOG_CRITICAL << "duplication function " << name << " registered!";
      }
    }
    else {
      auto it = handlers_.emplace(
          key,
          [](std::string_view data, rpc_context<rpc_protocol> &context_info,
             typename rpc_protocol::supported_serialize_protocols protocols) {
            return std::visit(
                [data, &context_info]<typename serialize_protocol>(
                    const serialize_protocol &obj) mutable {
                  return internal::execute<rpc_protocol, serialize_protocol,
                                           func>(data, context_info);
                },
                protocols);
          });
      if (!it.second) {
        ELOG_CRITICAL << "duplication function " << name << " registered!";
      }
    }
    id2name_.emplace(key, name);
  }

 public:
  router_handler_t *get_handler(uint32_t id) {
    if (auto it = handlers_.find(id); it != handlers_.end()) {
      return &it->second;
    }
    return nullptr;
  }

  coro_router_handler_t *get_coro_handler(uint32_t id) {
    if (auto it = coro_handlers_.find(id); it != coro_handlers_.end()) {
      return &it->second;
    }
    return nullptr;
  }

  async_simple::coro::Lazy<std::pair<coro_rpc::err_code, std::string>>
  route_coro(auto handler, std::string_view data,
             typename rpc_protocol::supported_serialize_protocols protocols,
             const typename rpc_protocol::route_key_t &route_key) {
    using namespace std::string_literals;
    if (handler)
      AS_LIKELY {
        try {
#ifndef NDEBUG
          ELOG_INFO << "route function name: " << get_name(route_key);
#endif
          // clang-format off
          co_return co_await (*handler)(data, protocols);
        } catch (coro_rpc::rpc_error& err) {
          co_return std::make_pair(err.code, std::move(err.msg));
        } catch (const std::exception &e) {
          co_return std::make_pair(coro_rpc::errc::rpc_throw_exception, e.what());
        } catch (...) {
          co_return std::make_pair(coro_rpc::errc::rpc_throw_exception,
                                   "unknown rpc function exception"s);
        }
      }
    else {
      co_return std::make_pair(coro_rpc::errc::function_not_registered,
                               "the rpc function not registered"s);
    }
  }

  std::pair<coro_rpc::err_code, std::string> route(
      auto handler, std::string_view data,
      rpc_context<rpc_protocol> &context_info,
      typename rpc_protocol::supported_serialize_protocols protocols,
      const typename rpc_protocol::route_key_t &route_key) {
    using namespace std::string_literals;
    if (handler)
      AS_LIKELY {
        try {
#ifndef NDEBUG
          ELOG_INFO << "route function name: " << get_name(route_key);
#endif
          return (*handler)(data, context_info, protocols);
        } catch (coro_rpc::rpc_error& err) {
          return std::make_pair(err.code, std::move(err.msg));
        } catch (const std::exception &e) {
          return std::make_pair(err_code{coro_rpc::errc::rpc_throw_exception}, e.what());
        } catch (...) {
          return std::make_pair(err_code{errc::rpc_throw_exception},
                                "unknown rpc function exception"s);
        }
      }
    else {
      using namespace std;
      return std::make_pair(coro_rpc::errc::function_not_registered,
                            "the rpc function not registered");
    }
  }

  /*!
   * Register RPC service functions (member function)
   *
   * Before RPC server started, all RPC service functions must be registered.
   * All you need to do is fill in the template parameters with the address of
   * your own RPC functions. If RPC function is registered twice, the program
   * will be terminate with exit code `EXIT_FAILURE`.
   *
   * Note: All functions must be member functions of the same class.
   *
   * ```cpp
   * class test_class {
   *  public:
   *  void plus_one(int val) {}
   *  std::string get_str(std::string str) { return str; }
   * };
   * int main() {
   *   test_class obj{};
   *   // register member functions
   *   register_handler<&test_class::plus_one, &test_class::get_str>(&obj);
   *   return 0;
   * }
   * ```
   *
   * @tparam first the address of RPC function. e.g. `&foo::bar`
   * @tparam func the address of RPC function. e.g. `&foo::bar`
   * @param self the object pointer corresponding to these member functions
   */

  template <auto first, auto... func>
  void register_handler(util::class_type_t<decltype(first)> *self) {
    regist_one_handler<first>(self);
    (regist_one_handler<func>(self), ...);
  }

  template <auto func>
  void register_handler(util::class_type_t<decltype(func)> *self,
                        const route_key &key) {
    regist_one_handler_impl<func>(self, key);
  }

  /*!
   * Register RPC service functions (non-member function)
   *
   * Before RPC server started, all RPC service functions must be registered.
   * All you need to do is fill in the template parameters with the address of
   * your own RPC functions. If RPC function is registered twice, the program
   * will be terminate with exit code `EXIT_FAILURE`.
   *
   * ```cpp
   * // RPC functions (non-member function)
   * void hello() {}
   * std::string get_str() { return ""; }
   * int add(int a, int b) {return a + b; }
   * int main() {
   *   register_handler<hello>();         // register one RPC function at once
   *   register_handler<get_str, add>();  // register multiple RPC functions at
   * once return 0;
   * }
   * ```
   *
   * @tparam first the address of RPC function. e.g. `foo`, `bar`
   * @tparam func the address of RPC function. e.g. `foo`, `bar`
   */

  template <auto first, auto... func>
  void register_handler() {
    regist_one_handler<first>();
    (regist_one_handler<func>(), ...);
  }

  template <auto func>
  void register_handler(const route_key &key) {
    regist_one_handler_impl<func>(key);
  }
};

}  // namespace protocol
}  // namespace coro_rpc