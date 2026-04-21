#pragma once

#include <chrono>
#include <ctime>
#include <string>

#include "time_util.hpp"

namespace cinatra {

class cookie {
 public:
  cookie() = default;
  cookie(const std::string &name, const std::string &value)
      : name_(name), value_(value) {}

  void set_version(int version) { version_ = version; }

  void set_name(const std::string &name) { name_ = name; }

  std::string get_name() const { return name_; }

  void set_value(const std::string &value) { value_ = value; }

  std::string get_value() const { return value_; }

  void set_comment(const std::string &comment) { comment_ = comment; }

  void set_domain(const std::string &domain) { domain_ = domain; }

  void set_path(const std::string &path) { path_ = path; }

  void set_priority(const std::string &priority) { priority_ = priority; }

  void set_secure(bool secure) { secure_ = secure; }

  void set_max_age(std::time_t seconds) { max_age_ = seconds; }

  void set_http_only(bool http_only) { http_only_ = http_only; }

  std::string to_string() const {
    std::string result;
    result.reserve(256);
    result.append(name_);
    result.append("=");
    if (version_ == 0) {
      // Netscape cookie
      result.append(value_);
      if (!domain_.empty()) {
        result.append("; domain=");
        result.append(domain_);
      }
      if (!path_.empty()) {
        result.append("; path=");
        result.append(path_);
      }
      if (!priority_.empty()) {
        result.append("; Priority=");
        result.append(priority_);
      }
      if (max_age_ != -1) {
        result.append("; expires=");
        result.append(get_gmt_time_str(std::chrono::system_clock::from_time_t(
            max_age_ + std::time(nullptr))));
      }
      if (secure_) {
        result.append("; secure");
      }
      if (http_only_) {
        result.append("; HttpOnly");
      }
    }
    else {
      // RFC 2109 cookie
      result.append("\"");
      result.append(value_);
      result.append("\"");
      if (!comment_.empty()) {
        result.append("; Comment=\"");
        result.append(comment_);
        result.append("\"");
      }
      if (!domain_.empty()) {
        result.append("; Domain=\"");
        result.append(domain_);
        result.append("\"");
      }
      if (!path_.empty()) {
        result.append("; Path=\"");
        result.append(path_);
        result.append("\"");
      }
      if (!priority_.empty()) {
        result.append("; Priority=\"");
        result.append(priority_);
        result.append("\"");
      }

      if (max_age_ != -1) {
        result.append("; Max-Age=\"");
        result.append(std::to_string(max_age_));
        result.append("\"");
      }
      if (secure_) {
        result.append("; secure");
      }
      if (http_only_) {
        result.append("; HttpOnly");
      }
      result.append("; Version=\"1\"");
    }
    return result;
  }

 private:
  std::string name_ = "";
  std::string value_ = "";
  std::string comment_ = "";
  std::string domain_ = "";
  std::string priority_ = "";
  std::string path_ = "";
  std::time_t max_age_ = -1;
  int version_ = 0;
  bool secure_ = false;
  bool http_only_ = false;
};

}  // namespace cinatra