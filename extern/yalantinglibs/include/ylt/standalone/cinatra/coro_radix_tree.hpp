#pragma once

#include <async_simple/coro/Lazy.h>

#include <functional>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include "cinatra/coro_http_request.hpp"
#include "coro_http_response.hpp"
#include "ylt/util/type_traits.h"

namespace cinatra {
constexpr char type_asterisk = '*';
constexpr char type_colon = ':';
constexpr char type_slash = '/';

typedef std::tuple<
    bool, std::function<void(coro_http_request &req, coro_http_response &resp)>,
    std::unordered_map<std::string, std::string>>
    parse_result;

typedef std::tuple<bool,
                   std::function<async_simple::coro::Lazy<void>(
                       coro_http_request &req, coro_http_response &resp)>,
                   std::unordered_map<std::string, std::string>>
    coro_result;

struct handler_t {
  std::string method;
  std::function<void(coro_http_request &req, coro_http_response &resp)> handler;
};

struct coro_handler_t {
  std::string method;
  std::function<async_simple::coro::Lazy<void>(coro_http_request &req,
                                               coro_http_response &resp)>
      coro_handler;
};

struct radix_tree_node {
  std::string path;
  handler_t handler;
  coro_handler_t coro_handler;
  std::string indices;
  std::vector<std::shared_ptr<radix_tree_node>> children;
  int max_params;

  radix_tree_node() = default;
  radix_tree_node(const std::string &path) { this->path = path; }
  ~radix_tree_node() {}

  std::function<void(coro_http_request &req, coro_http_response &resp)>
  get_handler(const std::string &method) {
    if (handler.method == method) {
      return handler.handler;
    }

    return nullptr;
  }

  std::function<async_simple::coro::Lazy<void>(coro_http_request &req,
                                               coro_http_response &resp)>
  get_coro_handler(const std::string &method) {
    if (coro_handler.method == method) {
      return coro_handler.coro_handler;
    }
    return nullptr;
  }

  int add_handler(
      std::function<void(coro_http_request &req, coro_http_response &resp)>
          handler,
      const std::string &method) {
    this->handler = handler_t{method, handler};

    return 0;
  }

  int add_coro_handler(std::function<async_simple::coro::Lazy<void>(
                           coro_http_request &req, coro_http_response &resp)>
                           coro_handler,
                       const std::string &method) {
    this->coro_handler = coro_handler_t{method, coro_handler};
    return 0;
  }

  std::shared_ptr<radix_tree_node> insert_child(
      char index, std::shared_ptr<radix_tree_node> child) {
    auto i = this->get_index_position(index);
    this->indices.insert(this->indices.begin() + i, index);
    this->children.insert(this->children.begin() + i, child);
    return child;
  }

  std::shared_ptr<radix_tree_node> get_child(char index) {
    auto i = this->get_index_position(index);
    return this->indices[i] != index ? nullptr : this->children[i];
  }

  int get_index_position(char target) {
    int low = 0, high = this->indices.size(), mid;

    while (low < high) {
      mid = low + ((high - low) >> 1);
      if (this->indices[mid] < target)
        low = mid + 1;
      else
        high = mid;
    }
    return low;
  }
};

class radix_tree {
 public:
  radix_tree() {
    this->root = std::make_shared<radix_tree_node>(radix_tree_node());
  }

  ~radix_tree() {}

  int insert(
      const std::string &path,
      std::function<void(coro_http_request &req, coro_http_response &resp)>
          handler,
      const std::string &method) {
    auto root = this->root;
    int i = 0, n = path.size(), param_count = 0, code = 0;
    while (i < n) {
      if (!root->indices.empty() &&
          (root->indices[0] == type_asterisk || path[i] == type_asterisk ||
           (path[i] != type_colon && root->indices[0] == type_colon) ||
           (path[i] == type_colon && root->indices[0] != type_colon) ||
           (path[i] == type_colon && root->indices[0] == type_colon &&
            path.substr(i + 1, find_pos(path, type_slash, i) - i - 1) !=
                root->children[0]->path))) {
        code = -1;
        break;
      }

      auto child = root->get_child(path[i]);
      if (!child) {
        auto p = find_pos(path, type_colon, i);

        if (p == n) {
          p = find_pos(path, type_asterisk, i);

          root = root->insert_child(path[i], std::make_shared<radix_tree_node>(
                                                 path.substr(i, p - i)));

          if (p < n) {
            root = root->insert_child(
                type_asterisk,
                std::make_shared<radix_tree_node>(path.substr(p + 1)));
            ++param_count;
          }

          code = root->add_handler(handler, method);
          break;
        }

        root = root->insert_child(
            path[i], std::make_shared<radix_tree_node>(path.substr(i, p - i)));

        i = find_pos(path, type_slash, p);

        root = root->insert_child(
            type_colon,
            std::make_shared<radix_tree_node>(path.substr(p + 1, i - p - 1)));
        ++param_count;

        if (i == n) {
          code = root->add_handler(handler, method);
          break;
        }
      }
      else {
        root = child;

        if (path[i] == type_colon) {
          ++param_count;
          i += root->path.size() + 1;

          if (i == n) {
            code = root->add_handler(handler, method);
            break;
          }
        }
        else {
          auto j = 0UL;
          auto m = root->path.size();

          for (; i < n && j < m && path[i] == root->path[j]; ++i, ++j) {
          }

          if (j < m) {
            std::shared_ptr<radix_tree_node> child(
                std::make_shared<radix_tree_node>(root->path.substr(j)));
            child->handler = root->handler;
            child->indices = root->indices;
            child->children = root->children;

            root->path = root->path.substr(0, j);
            root->handler = {};
            root->indices = child->path[0];
            root->children = {child};
          }

          if (i == n) {
            code = root->add_handler(handler, method);
            break;
          }
        }
      }
    }

    if (param_count > this->root->max_params)
      this->root->max_params = param_count;

    return code;
  }

  int coro_insert(const std::string &path,
                  std::function<async_simple::coro::Lazy<void>(
                      coro_http_request &req, coro_http_response &resp)>
                      coro_handler,
                  std::string &method) {
    auto root = this->root;
    int i = 0, n = path.size(), param_count = 0, code = 0;
    while (i < n) {
      if (!root->indices.empty() &&
          (root->indices[0] == type_asterisk || path[i] == type_asterisk ||
           (path[i] != type_colon && root->indices[0] == type_colon) ||
           (path[i] == type_colon && root->indices[0] != type_colon) ||
           (path[i] == type_colon && root->indices[0] == type_colon &&
            path.substr(i + 1, find_pos(path, type_slash, i) - i - 1) !=
                root->children[0]->path))) {
        code = -1;
        break;
      }

      auto child = root->get_child(path[i]);
      if (!child) {
        auto p = find_pos(path, type_colon, i);

        if (p == n) {
          p = find_pos(path, type_asterisk, i);

          root = root->insert_child(path[i], std::make_shared<radix_tree_node>(
                                                 path.substr(i, p - i)));

          if (p < n) {
            root = root->insert_child(
                type_asterisk,
                std::make_shared<radix_tree_node>(path.substr(p + 1)));
            ++param_count;
          }

          code = root->add_coro_handler(coro_handler, method);
          break;
        }

        root = root->insert_child(
            path[i], std::make_shared<radix_tree_node>(path.substr(i, p - i)));

        i = find_pos(path, type_slash, p);

        root = root->insert_child(
            type_colon,
            std::make_shared<radix_tree_node>(path.substr(p + 1, i - p - 1)));
        ++param_count;

        if (i == n) {
          code = root->add_coro_handler(coro_handler, method);
          break;
        }
      }
      else {
        root = child;

        if (path[i] == type_colon) {
          ++param_count;
          i += root->path.size() + 1;

          if (i == n) {
            code = root->add_coro_handler(coro_handler, method);
            break;
          }
        }
        else {
          auto j = 0UL;
          auto m = root->path.size();

          for (; i < n && j < m && path[i] == root->path[j]; ++i, ++j) {
          }

          if (j < m) {
            std::shared_ptr<radix_tree_node> child(
                std::make_shared<radix_tree_node>(root->path.substr(j)));
            child->coro_handler = root->coro_handler;
            child->indices = root->indices;
            child->children = root->children;

            root->path = root->path.substr(0, j);
            root->coro_handler = {};
            root->indices = child->path[0];
            root->children = {child};
          }

          if (i == n) {
            code = root->add_coro_handler(coro_handler, method);
            break;
          }
        }
      }
    }

    if (param_count > this->root->max_params)
      this->root->max_params = param_count;

    return code;
  }

  parse_result get(const std::string &path, const std::string &method) {
    std::unordered_map<std::string, std::string> params;
    auto root = this->root;

    int i = 0, n = path.size(), p;

    while (i < n) {
      if (root->indices.empty())
        return parse_result();

      if (root->indices[0] == type_colon) {
        root = root->children[0];

        p = find_pos(path, type_slash, i);
        params[root->path] = path.substr(i, p - i);
        i = p;
      }
      else if (root->indices[0] == type_asterisk) {
        root = root->children[0];
        params[root->path] = path.substr(i);
        break;
      }
      else {
        root = root->get_child(path[i]);
        if (!root || path.substr(i, root->path.size()) != root->path)
          return parse_result();
        i += root->path.size();
      }
    }

    return parse_result{true, root->get_handler(method), params};
  }

  coro_result get_coro(const std::string &path, const std::string &method) {
    std::unordered_map<std::string, std::string> params;
    auto root = this->root;

    int i = 0, n = path.size(), p;

    while (i < n) {
      if (root->indices.empty())
        return coro_result();

      if (root->indices[0] == type_colon) {
        root = root->children[0];

        p = find_pos(path, type_slash, i);
        params[root->path] = path.substr(i, p - i);
        i = p;
      }
      else if (root->indices[0] == type_asterisk) {
        root = root->children[0];
        params[root->path] = path.substr(i);
        break;
      }
      else {
        root = root->get_child(path[i]);
        if (!root || path.substr(i, root->path.size()) != root->path)
          return coro_result();
        i += root->path.size();
      }
    }

    return coro_result{true, root->get_coro_handler(method), params};
  }

 private:
  int find_pos(const std::string &str, char target, int start) {
    auto i = str.find(target, start);
    return i == -1 ? str.size() : i;
  }

  std::shared_ptr<radix_tree_node> root;
};
}  // namespace cinatra