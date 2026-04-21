#pragma once

#include <asio/steady_timer.hpp>
#include <atomic>
#include <chrono>
#include <string>

#include "session.hpp"
#include "ylt/coro_io/coro_io.hpp"

namespace cinatra {

class session_manager {
 public:
  static session_manager &get() {
    static session_manager instance;
    return instance;
  }

  std::string generate_session_id() {
    auto tp = std::chrono::high_resolution_clock::now();
    auto nano = tp.time_since_epoch().count();
    id_++;
    return std::to_string(nano).append(std::to_string(id_));
  }

  std::shared_ptr<session> get_session(const std::string &session_id) {
    std::unique_lock<std::mutex> lock(mtx_);

    std::shared_ptr<session> new_session = nullptr;
    auto iter = map_.find(session_id);
    if (iter != map_.end()) {
      return iter->second;
    }
    else {
      new_session =
          std::make_shared<session>(session_id, session_timeout_, true);
      map_.insert({session_id, new_session});
    }

    return new_session;
  }

  void remove_expire_session() {
    std::unique_lock<std::mutex> lock(mtx_);

    auto now = std::time(nullptr);
    for (auto it = map_.begin(); it != map_.end();) {
      if (it->second->get_time_stamp() <= now)
        it = map_.erase(it);
      else
        ++it;
    }
  }

  bool check_session_existence(const std::string &session_id) {
    std::unique_lock<std::mutex> lock(mtx_);

    return map_.find(session_id) != map_.end();
  }

  void start_check_session_timer() {
    std::lock_guard lock(timer_mtx_);
    std::weak_ptr<asio::steady_timer> timer = check_session_timer_;
    check_session_timer_->expires_after(check_session_duration_);
    check_session_timer_->async_wait([this, timer](auto ec) {
      auto ptr = timer.lock();
      if (ptr == nullptr) {
        return;
      }

      if (ec || stop_timer_) {
        return;
      }

      remove_expire_session();
      start_check_session_timer();
    });
  }

  void set_check_session_duration(auto duration) {
    check_session_duration_ = duration;
    {
      std::lock_guard lock(timer_mtx_);
      std::error_code ec;
      check_session_timer_->cancel(ec);
    }

    start_check_session_timer();
  }

  void stop_timer() { stop_timer_ = true; }

 private:
  session_manager()
      : check_session_timer_(std::make_shared<asio::steady_timer>(
            coro_io::get_global_executor()->get_asio_executor())) {
    start_check_session_timer();
  };
  session_manager(const session_manager &) = delete;
  session_manager(session_manager &&) = delete;

  std::atomic_int64_t id_ = 0;
  std::unordered_map<std::string, std::shared_ptr<session>> map_;
  std::mutex mtx_;

  // session_timeout_ should be no less than 0
  std::size_t session_timeout_ = 86400;
  std::atomic<bool> stop_timer_ = false;
  std::mutex timer_mtx_;
  std::shared_ptr<asio::steady_timer> check_session_timer_;
  std::atomic<std::chrono::steady_clock::duration> check_session_duration_ = {
      std::chrono::seconds(15)};
};

}  // namespace cinatra