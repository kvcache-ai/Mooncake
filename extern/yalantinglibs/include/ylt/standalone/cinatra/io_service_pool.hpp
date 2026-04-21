#pragma once
#include <memory>
#include <thread>
#include <vector>

#include "use_asio.hpp"
#include "utils.hpp"

namespace cinatra {
class io_service_pool {
 public:
  using executor_type = asio::io_context::executor_type;
  explicit io_service_pool(std::size_t pool_size) : next_io_context_(0) {
    if (pool_size == 0) {
      pool_size = 1;  // set default value as 1
    }

    for (std::size_t i = 0; i < pool_size; ++i) {
      io_context_ptr io_context(new asio::io_context);
      work_ptr work(new asio::io_context::work(*io_context));
      io_contexts_.push_back(io_context);
      work_.push_back(work);
    }
  }

  void run() {
    std::vector<std::shared_ptr<std::thread>> threads;
    for (std::size_t i = 0; i < io_contexts_.size(); ++i) {
      threads.emplace_back(std::make_shared<std::thread>(
          [](io_context_ptr svr) {
            svr->run();
          },
          io_contexts_[i]));
    }

    for (std::size_t i = 0; i < threads.size(); ++i) {
      threads[i]->join();
    }
    promise_.set_value();
  }

  void stop() {
    work_.clear();
    promise_.get_future().wait();
  }

  bool has_stop() const { return work_.empty(); }

  size_t current_io_context() { return next_io_context_ - 1; }

  asio::io_context::executor_type get_executor() {
    asio::io_context &io_context = *io_contexts_[next_io_context_];
    ++next_io_context_;
    if (next_io_context_ == io_contexts_.size()) {
      next_io_context_ = 0;
    }
    return io_context.get_executor();
  }

  asio::io_service &get_io_service() {
    asio::io_service &io_service = *io_contexts_[next_io_context_];
    ++next_io_context_;
    if (next_io_context_ == io_contexts_.size())
      next_io_context_ = 0;
    return io_service;
  }

 private:
  using io_context_ptr = std::shared_ptr<asio::io_context>;
  using work_ptr = std::shared_ptr<asio::io_context::work>;

  std::vector<io_context_ptr> io_contexts_;
  std::vector<work_ptr> work_;
  std::size_t next_io_context_;
  std::promise<void> promise_;
};

class io_service_inplace : private noncopyable {
 public:
  explicit io_service_inplace() {
    io_services_ = std::make_shared<asio::io_service>();
    work_ = std::make_shared<asio::io_service::work>(*io_services_);
  }

  void run() { io_services_->run(); }

  intptr_t run_one() { return io_services_->run_one(); }

  intptr_t poll() { return io_services_->poll(); }

  intptr_t poll_one() { return io_services_->poll_one(); }

  void stop() {
    work_ = nullptr;

    if (io_services_)
      io_services_->stop();
  }

  asio::io_service &get_io_service() { return *io_services_; }

 private:
  using io_service_ptr = std::shared_ptr<asio::io_service>;
  using work_ptr = std::shared_ptr<asio::io_service::work>;

  io_service_ptr io_services_;
  work_ptr work_;
};
}  // namespace cinatra
