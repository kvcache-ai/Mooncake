#pragma once
#include <string>
#include <system_error>

namespace cinatra {
enum class http_errc { connect_timeout = 2025, request_timeout };

class http_error_category : public std::error_category {
 public:
  const char* name() const noexcept override { return "coro_http_error"; }

  std::string message(int ev) const override {
    switch (static_cast<http_errc>(ev)) {
      case http_errc::connect_timeout:
        return "Connect timeout";
      case http_errc::request_timeout:
        return "Request timeout";
      default:
        return "Unknown error";
    }
  }
};

inline cinatra::http_error_category& category() {
  static cinatra::http_error_category instance;
  return instance;
}

inline std::error_code make_error_code(http_errc e) {
  return {static_cast<int>(e), category()};
}

inline bool operator==(const std::error_code& code, http_errc ec) {
  return code.value() == (int)ec;
}

}  // namespace cinatra