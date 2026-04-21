#pragma once

#include <ctime>
#include <mutex>
#include <string>
#include <unordered_map>

#include "cookie.hpp"

namespace cinatra {

class session {
 public:
  session(const std::string &session_id, std::size_t session_timeout,
          bool need_set_to_client)
      : session_id_(session_id),
        session_timeout_(session_timeout),
        cookie_(CSESSIONID, session_id_),
        need_set_to_client_(need_set_to_client) {
    time_stamp_ = session_timeout_ + std::time(nullptr);
    cookie_.set_max_age(session_timeout_);
  }

  void set_session_timeout(const std::size_t session_timeout = 86400) {
    std::unique_lock<std::mutex> lock(mtx_);

    session_timeout_ = session_timeout;
    time_stamp_ = session_timeout_ + std::time(nullptr);
    cookie_.set_max_age(session_timeout_);
    need_set_to_client_ = true;
  }

  void invalidate() { set_session_timeout(0); }

  void set_data(const std::string &name, std::any data) {
    std::unique_lock<std::mutex> lock(mtx_);

    data_[name] = std::move(data);
  }

  void remove_data(const std::string &name) {
    std::unique_lock<std::mutex> lock(mtx_);

    if (data_.find(name) != data_.end()) {
      data_.erase(name);
    }
  }

  template <typename T>
  std::optional<T> get_data(const std::string &name) {
    std::unique_lock<std::mutex> lock(mtx_);

    auto iter = data_.find(name);
    if (iter != data_.end()) {
      try {
        return std::any_cast<T>(iter->second);
      } catch (const std::exception &e) {
        return std::nullopt;
      }
    }

    return std::nullopt;
  }

  const auto &get_all_data() const { return data_; }

  const std::string &get_session_id() {
    std::unique_lock<std::mutex> lock(mtx_);
    return session_id_;
  }

  std::size_t get_time_stamp() {
    std::unique_lock<std::mutex> lock(mtx_);
    return time_stamp_;
  }

  cookie &get_session_cookie() {
    std::unique_lock<std::mutex> lock(mtx_);
    return cookie_;
  }

  bool get_need_set_to_client() {
    std::unique_lock<std::mutex> lock(mtx_);
    return need_set_to_client_;
  }

  void set_need_set_to_client(bool need_set_to_client) {
    std::unique_lock<std::mutex> lock(mtx_);
    need_set_to_client_ = need_set_to_client;
  }

 private:
  session() = delete;

  std::string session_id_;
  // after session_timeout_ seconds, the session expires
  std::size_t session_timeout_;
  // the session expires at time_stamp_
  std::time_t time_stamp_;
  std::mutex mtx_;
  std::unordered_map<std::string, std::any> data_;
  cookie cookie_;
  bool need_set_to_client_;
};

}  // namespace cinatra