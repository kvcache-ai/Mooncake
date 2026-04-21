#pragma once
#include <iostream>
#include <mutex>
namespace cinatra {
struct null_logger_t {
  template <typename T>
  const null_logger_t& operator<<(T&&) const {
    return *this;
  }
};
struct cout_logger_t {
  template <typename T>
  const cout_logger_t& operator<<(T&& t) const {
    std::cout << std::forward<T>(t);
    return *this;
  }
  ~cout_logger_t() { std::cout << std::endl; }
  std::unique_lock<std::mutex> lock_ = std::unique_lock{mtx_};
  inline static std::mutex mtx_;
};
struct cerr_logger_t {
  template <typename T>
  const cerr_logger_t& operator<<(T&& t) const {
    std::cerr << std::forward<T>(t);
    return *this;
  }
  ~cerr_logger_t() { std::cerr << std::endl; }
  std::unique_lock<std::mutex> lock_ = std::unique_lock{mtx_};
  inline static std::mutex mtx_;
};

constexpr inline cinatra::null_logger_t NULL_LOGGER;

}  // namespace cinatra

#ifdef CINATRA_LOG_ERROR
#else
#define CINATRA_LOG_ERROR \
  cinatra::cerr_logger_t {}
#endif

#ifdef CINATRA_LOG_WARNING
#else
#ifndef NDEBUG
#define CINATRA_LOG_WARNING \
  cinatra::cerr_logger_t {}
#else
#define CINATRA_LOG_WARNING cinatra::NULL_LOGGER
#endif
#endif

#ifdef CINATRA_LOG_INFO
#else
#ifndef NDEBUG
#define CINATRA_LOG_INFO \
  cinatra::cout_logger_t {}
#else
#define CINATRA_LOG_INFO cinatra::NULL_LOGGER
#endif
#endif

#ifdef CINATRA_LOG_DEBUG
#else
#ifndef NDEBUG
#define CINATRA_LOG_DEBUG \
  cinatra::cout_logger_t {}
#else
#define CINATRA_LOG_DEBUG cinatra::NULL_LOGGER
#endif
#endif

#ifdef CINATRA_LOG_TRACE
#else
#ifndef NDEBUG
#define CINATRA_LOG_TRACE \
  cinatra::cout_logger_t {}
#else
#define CINATRA_LOG_TRACE cinatra::NULL_LOGGER
#endif
#endif
