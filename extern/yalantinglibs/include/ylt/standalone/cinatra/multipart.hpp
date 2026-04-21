#pragma once
#include "define.h"

namespace cinatra {

template <typename T>
class multipart_reader_t {
 public:
  multipart_reader_t(T *conn)
      : conn_(conn),
        head_buf_(conn_->head_buf_),
        chunked_buf_(conn_->chunked_buf_) {}

  async_simple::coro::Lazy<part_head_t> read_part_head(
      std::string_view boundary) {
    if (boundary.empty()) {
      co_return part_head_t{std::make_error_code(std::errc::protocol_error)};
    }

    if (head_buf_.size() > 0) {
      const char *data_ptr = asio::buffer_cast<const char *>(head_buf_.data());
      chunked_buf_.sputn(data_ptr, head_buf_.size());
      head_buf_.consume(head_buf_.size());
    }

    part_head_t result{};
    std::error_code ec{};
    size_t size;

    auto get_part_name = [](std::string_view data, std::string_view name,
                            size_t start) {
      start += name.length();
      size_t end = data.find("\"", start);
      return data.substr(start, end - start);
    };

    constexpr std::string_view name = "name=\"";
    constexpr std::string_view filename = "filename=\"";

    while (true) {
      if (std::tie(ec, size) =
              co_await conn_->async_read_until(chunked_buf_, CRCF);
          ec) {
        result.ec = ec;
        conn_->close();
        co_return result;
      }

      const char *data_ptr =
          asio::buffer_cast<const char *>(chunked_buf_.data());
      chunked_buf_.consume(size);
      if (*data_ptr == '-') {
        continue;
      }
      std::string_view data{data_ptr, size};
      if (size == 2) {  // got the head end: \r\n\r\n
        break;
      }

      if (size_t pos = data.find("name"); pos != std::string_view::npos) {
        result.name = get_part_name(data, name, pos);

        if (size_t pos = data.find("filename"); pos != std::string_view::npos) {
          result.filename = get_part_name(data, filename, pos);
        }
        continue;
      }
    }

    co_return result;
  }

  async_simple::coro::Lazy<chunked_result> read_part_body(
      std::string_view boundary) {
    chunked_result result{};
    std::error_code ec{};
    size_t size = 0;

    if (std::tie(ec, size) =
            co_await conn_->async_read_until(chunked_buf_, boundary);
        ec) {
      result.ec = ec;
      conn_->close();
      co_return result;
    }

    const char *data_ptr = asio::buffer_cast<const char *>(chunked_buf_.data());
    chunked_buf_.consume(size);
    result.data = std::string_view{
        data_ptr, size - boundary.size() - 4};  //-- boundary \r\n

    if (std::tie(ec, size) =
            co_await conn_->async_read_until(chunked_buf_, CRCF);
        ec) {
      result = {};
      result.ec = ec;
      conn_->close();
      co_return result;
    }

    data_ptr = asio::buffer_cast<const char *>(chunked_buf_.data());
    std::string data{data_ptr, size};
    if (size > 2) {
      constexpr std::string_view complete_flag = "--\r\n";
      if (data == complete_flag) {
        result.eof = true;
      }
    }

    chunked_buf_.consume(size);
    co_return result;
  }

 private:
  T *conn_;
  asio::streambuf &head_buf_;
  asio::streambuf &chunked_buf_;
};

template <typename T>
multipart_reader_t(T *con) -> multipart_reader_t<T>;
}  // namespace cinatra