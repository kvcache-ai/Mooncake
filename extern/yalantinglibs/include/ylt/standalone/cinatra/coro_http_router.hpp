#pragma once
#include <algorithm>
#include <functional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>

#include "cinatra/cinatra_log_wrapper.hpp"
#include "cinatra/coro_http_request.hpp"
#include "cinatra/coro_radix_tree.hpp"
#include "cinatra/response_cv.hpp"
#include "cinatra/utils.hpp"
#include "coro_http_response.hpp"
#include "ylt/util/type_traits.h"

namespace cinatra {
template <class, class = void>
struct has_before : std::false_type {};

template <class T>
struct has_before<T, std::void_t<decltype(std::declval<T>().before(
                         std::declval<coro_http_request&>(),
                         std::declval<coro_http_response&>()))>>
    : std::true_type {};

template <class, class = void>
struct has_after : std::false_type {};

template <class T>
struct has_after<T, std::void_t<decltype(std::declval<T>().after(
                        std::declval<coro_http_request&>(),
                        std::declval<coro_http_response&>()))>>
    : std::true_type {};

template <class T>
constexpr bool has_before_v = has_before<T>::value;

template <class T>
constexpr bool has_after_v = has_after<T>::value;

class coro_http_router {
 public:
  // eg: "GET hello/" as a key
  template <http_method method, typename Func, typename... Aspects>
  void set_http_handler(std::string key, Func handler, Aspects&&... asps) {
    constexpr auto method_name = cinatra::method_name(method);
    std::string whole_str;
    whole_str.append(method_name).append(" ").append(key);

    // hold keys to make sure map_handles_ key is
    // std::string_view, avoid memcpy when route
    using return_type = typename util::function_traits<Func>::return_type;
    if constexpr (coro_io::is_lazy_v<return_type>) {
      std::function<async_simple::coro::Lazy<void>(coro_http_request & req,
                                                   coro_http_response & resp)>
          http_handler;
      if constexpr (sizeof...(Aspects) > 0) {
        http_handler = [this, handler = std::move(handler),
                        ... asps = std::forward<Aspects>(asps)](
                           coro_http_request& req,
                           coro_http_response& resp) mutable
            -> async_simple::coro::Lazy<void> {
          bool ok = true;
          (do_before(asps, req, resp, ok), ...);
          if (ok) {
            co_await handler(req, resp);
          }
          ok = true;
          (do_after(asps, req, resp, ok), ...);
        };
      }
      else {
        http_handler = std::move(handler);
      }

      if (whole_str.find(":") != std::string::npos) {
        std::string method_str(method_name);
        coro_router_tree_->coro_insert(whole_str, std::move(http_handler),
                                       method_str);
      }
      else {
        if (whole_str.find("{") != std::string::npos ||
            whole_str.find(")") != std::string::npos) {
          std::string pattern = whole_str;

          if (pattern.find("{}") != std::string::npos) {
            replace_all(pattern, "{}", "([^/]+)");
          }

          coro_regex_handles_.emplace_back(std::regex(pattern),
                                           std::move(http_handler));
        }
        else {
          auto [it, ok] = coro_keys_.emplace(std::move(whole_str));
          if (!ok) {
            CINATRA_LOG_WARNING << key << " has already registered.";
            return;
          }
          coro_handles_.emplace(*it, std::move(http_handler));
        }
      }
    }
    else {
      std::function<void(coro_http_request & req, coro_http_response & resp)>
          http_handler;
      if constexpr (sizeof...(Aspects) > 0) {
        http_handler = [this, handler = std::move(handler),
                        ... asps = std::forward<Aspects>(asps)](
                           coro_http_request& req,
                           coro_http_response& resp) mutable {
          bool ok = true;
          (do_before(asps, req, resp, ok), ...);
          if (ok) {
            handler(req, resp);
          }
          ok = true;
          (do_after(asps, req, resp, ok), ...);
        };
      }
      else {
        http_handler = std::move(handler);
      }

      if (whole_str.find(':') != std::string::npos) {
        std::string method_str(method_name);
        router_tree_->insert(whole_str, std::move(http_handler), method_str);
      }
      else if (whole_str.find("{") != std::string::npos ||
               whole_str.find(")") != std::string::npos) {
        std::string pattern = whole_str;

        if (pattern.find("{}") != std::string::npos) {
          replace_all(pattern, "{}", "([^/]+)");
        }

        regex_handles_.emplace_back(std::regex(pattern),
                                    std::move(http_handler));
      }
      else {
        auto [it, ok] = keys_.emplace(std::move(whole_str));
        if (!ok) {
          CINATRA_LOG_WARNING << key << " has already registered.";
          return;
        }
        map_handles_.emplace(*it, std::move(http_handler));
      }
    }
  }

  template <typename T>
  void do_before(T& aspect, coro_http_request& req, coro_http_response& resp,
                 bool& ok) {
    if constexpr (has_before_v<T>) {
      if (!ok) {
        return;
      }
      ok = aspect.before(req, resp);
    }
  }

  template <typename T>
  void do_after(T& aspect, coro_http_request& req, coro_http_response& resp,
                bool& ok) {
    if constexpr (has_after_v<T>) {
      if (!ok) {
        return;
      }
      ok = aspect.after(req, resp);
    }
  }

  std::function<void(coro_http_request& req, coro_http_response& resp)>*
  get_handler(std::string_view key) {
    if (auto it = map_handles_.find(key); it != map_handles_.end()) {
      return &it->second;
    }
    return nullptr;
  }

  std::function<async_simple::coro::Lazy<void>(coro_http_request& req,
                                               coro_http_response& resp)>*
  get_coro_handler(std::string_view key) {
    if (auto it = coro_handles_.find(key); it != coro_handles_.end()) {
      return &it->second;
    }
    return nullptr;
  }

  void route(auto handler, auto& req, auto& resp, std::string_view key) {
    try {
      (*handler)(req, resp);
    } catch (const std::exception& e) {
      CINATRA_LOG_WARNING << "exception in business function, reason: "
                          << e.what();
      resp.set_status(status_type::service_unavailable);
    } catch (...) {
      CINATRA_LOG_WARNING << "unknown exception in business function";
      resp.set_status(status_type::service_unavailable);
    }
  }

  async_simple::coro::Lazy<void> route_coro(auto handler, auto& req, auto& resp,
                                            std::string_view key) {
    try {
      co_await (*handler)(req, resp);
    } catch (const std::exception& e) {
      CINATRA_LOG_WARNING << "exception in business function, reason: "
                          << e.what();
      resp.set_status(status_type::service_unavailable);
    } catch (...) {
      CINATRA_LOG_WARNING << "unknown exception in business function";
      resp.set_status(status_type::service_unavailable);
    }
  }

  const auto& get_handlers() const { return map_handles_; }

  const auto& get_coro_handlers() const { return coro_handles_; }

  std::shared_ptr<radix_tree> get_router_tree() { return router_tree_; }

  std::shared_ptr<radix_tree> get_coro_router_tree() {
    return coro_router_tree_;
  }

  const auto& get_coro_regex_handlers() { return coro_regex_handles_; }

  const auto& get_regex_handlers() { return regex_handles_; }

 private:
  std::set<std::string> keys_;
  std::unordered_map<
      std::string_view,
      std::function<void(coro_http_request& req, coro_http_response& resp)>>
      map_handles_;

  std::set<std::string> coro_keys_;
  std::unordered_map<std::string_view,
                     std::function<async_simple::coro::Lazy<void>(
                         coro_http_request& req, coro_http_response& resp)>>
      coro_handles_;

  std::shared_ptr<radix_tree> router_tree_ =
      std::make_shared<radix_tree>(radix_tree());

  std::shared_ptr<radix_tree> coro_router_tree_ =
      std::make_shared<radix_tree>(radix_tree());

  std::vector<std::tuple<
      std::regex,
      std::function<void(coro_http_request& req, coro_http_response& resp)>>>
      regex_handles_;

  std::vector<std::tuple<
      std::regex, std::function<async_simple::coro::Lazy<void>(
                      coro_http_request& req, coro_http_response& resp)>>>
      coro_regex_handles_;
};
}  // namespace cinatra