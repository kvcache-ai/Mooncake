#pragma once

#include <any>
#include <charconv>
#include <initializer_list>
#include <optional>
#include <regex>
#include <string>

#include "async_simple/coro/Lazy.h"
#include "define.h"
#include "http_parser.hpp"
#include "session.hpp"
#include "session_manager.hpp"
#include "utils.hpp"
#include "ws_define.h"

namespace cinatra {

inline std::vector<std::pair<int, int>> parse_ranges(std::string_view range_str,
                                                     size_t file_size,
                                                     bool &is_valid) {
  range_str = trim_sv(range_str);
  if (range_str.empty()) {
    return {{0, file_size - 1}};
  }

  if (range_str.find("--") != std::string_view::npos) {
    is_valid = false;
    return {};
  }

  if (range_str == "-") {
    return {{0, file_size - 1}};
  }

  std::vector<std::pair<int, int>> vec;
  auto ranges = split_sv(range_str, ",");
  for (auto range : ranges) {
    auto sub_range = split_sv(range, "-");
    auto fist_range = trim_sv(sub_range[0]);

    int start = 0;
    if (fist_range.empty()) {
      start = -1;
    }
    else {
      auto [ptr, ec] = std::from_chars(
          fist_range.data(), fist_range.data() + fist_range.size(), start);
      if (ec != std::errc{}) {
        is_valid = false;
        return {};
      }
    }

    int end = 0;
    if (sub_range.size() == 1) {
      end = file_size - 1;
    }
    else {
      auto second_range = trim_sv(sub_range[1]);
      if (second_range.empty()) {
        end = file_size - 1;
      }
      else {
        auto [ptr, ec] =
            std::from_chars(second_range.data(),
                            second_range.data() + second_range.size(), end);
        if (ec != std::errc{}) {
          is_valid = false;
          return {};
        }
      }
    }

    if (start > 0 && (start >= file_size || start == end)) {
      // out of range
      is_valid = false;
      return {};
    }

    if (end > 0 && end >= file_size) {
      end = file_size - 1;
    }

    if (start == -1) {
      start = file_size - end;
      end = file_size - 1;
    }

    vec.push_back({start, end});
  }
  return vec;
}

class coro_http_connection;
class coro_http_request {
 public:
  coro_http_request(http_parser &parser, coro_http_connection *conn)
      : parser_(parser), conn_(conn) {}

  std::string_view get_header_value(std::string_view key) {
    auto headers = parser_.get_headers();
    for (auto &header : headers) {
      if (iequal0(header.name, key)) {
        return header.value;
      }
    }

    return {};
  }

  std::string_view get_query_value(std::string_view key) {
    return parser_.get_query_value(key);
  }

  std::string get_decode_query_value(std::string_view key) {
    auto value = parser_.get_query_value(key);
    if (value.empty()) {
      return "";
    }

    return code_utils::get_string_by_urldecode(value);
  }

  std::span<http_header> get_headers() const { return parser_.get_headers(); }

  const auto &get_queries() const { return parser_.queries(); }

  std::string_view full_url() { return parser_.full_url(); }

  void set_body(std::string &body) {
    body_ = body;
    auto type = get_content_type();
    if (type == content_type::urlencoded) {
      parser_.parse_query(body_);
    }
  }

  std::string_view get_body() const { return body_; }

  bool is_chunked() { return parser_.is_chunked(); }

  std::string_view get_accept_encoding() {
    return get_header_value("Accept-Encoding");
  }

  content_encoding get_encoding_type() {
    auto encoding_type = get_header_value("Content-Encoding");
    if (!encoding_type.empty()) {
      if (encoding_type.find("gzip") != std::string_view::npos)
        return content_encoding::gzip;
      else if (encoding_type.find("deflate") != std::string_view::npos)
        return content_encoding::deflate;
      else if (encoding_type.find("br") != std::string_view::npos)
        return content_encoding::br;
      else
        return content_encoding::none;
    }
    else {
      return content_encoding::none;
    }
  }

  content_type get_content_type() {
    if (is_chunked())
      return content_type::chunked;

    auto content_type = get_header_value("content-type");
    if (!content_type.empty()) {
      if (content_type.find("application/x-www-form-urlencoded") !=
          std::string_view::npos) {
        return content_type::urlencoded;
      }
      else if (content_type.find("multipart/form-data") !=
               std::string_view::npos) {
        return content_type::multipart;
      }
      else if (content_type.find("application/octet-stream") !=
               std::string_view::npos) {
        return content_type::octet_stream;
      }
      else {
        return content_type::string;
      }
    }

    if (is_websocket_) {
      return content_type::websocket;
    }

    return content_type::unknown;
  }

  std::string_view get_url() { return parser_.url(); }

  std::string_view get_method() { return parser_.method(); }

  std::string_view get_boundary() {
    auto content_type = get_header_value("content-type");
    if (content_type.empty()) {
      return {};
    }

    auto pos = content_type.rfind("=");
    if (pos == std::string_view::npos) {
      return "";
    }

    return content_type.substr(pos + 1);
  }

  coro_http_connection *get_conn() { return conn_; }

  bool is_upgrade() {
    if (!parser_.has_upgrade())
      return false;

    auto u = get_header_value("Upgrade");
    if (u.empty())
      return false;

    if (u != WEBSOCKET)
      return false;

    auto sec_ws_key = get_header_value("sec-websocket-key");
    if (sec_ws_key.empty() || sec_ws_key.size() != 24)
      return false;

    is_websocket_ = true;
    return true;
  }

  bool is_support_compressed() {
    auto extension_str = get_header_value("Sec-WebSocket-Extensions");
    if (extension_str.find("permessage-deflate") != std::string::npos) {
      return true;
    }
    return false;
  }

  void set_aspect_data(std::vector<std::string> data) {
    aspect_data_ = std::move(data);
  }

  template <typename... Args>
  void set_aspect_data(Args... args) {
    (aspect_data_.push_back(std::move(args)), ...);
  }

  void set_user_data(std::any data) { user_data_ = std::move(data); }

  std::any get_user_data() { return user_data_; }

  std::vector<std::string> &get_aspect_data() { return aspect_data_; }

  std::unordered_map<std::string_view, std::string_view> get_cookies(
      std::string_view cookie_str) const {
    auto cookies = get_cookies_map(cookie_str);
    return cookies;
  }

  std::shared_ptr<session> get_session(bool create = true) {
    auto &session_manager = session_manager::get();

    auto cookies = get_cookies(get_header_value("Cookie"));
    std::string session_id;
    auto iter = cookies.find(CSESSIONID);
    if (iter == cookies.end() && !create) {
      return nullptr;
    }
    else if (iter == cookies.end()) {
      session_id = session_manager.generate_session_id();
    }
    else {
      session_id = iter->second;
    }

    cached_session_id_ = session_id;
    return session_manager.get_session(session_id);
  }

  std::string get_cached_session_id() {
    std::string temp_session_id = "";
    cached_session_id_.swap(temp_session_id);
    return temp_session_id;
  }

  bool has_session() { return !cached_session_id_.empty(); }
  void clear() {
    body_ = {};
    if (!aspect_data_.empty()) {
      aspect_data_.clear();
    }
    if (user_data_.has_value()) {
      user_data_.reset();
    }
  }

  std::unordered_map<std::string, std::string> params_;
  std::smatch matches_;

 private:
  http_parser &parser_;
  std::string_view body_;
  coro_http_connection *conn_;
  bool is_websocket_ = false;
  std::vector<std::string> aspect_data_;
  std::string cached_session_id_;
  std::any user_data_;
};
}  // namespace cinatra