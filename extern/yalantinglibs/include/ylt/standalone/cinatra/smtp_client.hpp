#pragma once
#include <asio.hpp>
#include <string>

#include "utils.hpp"

namespace cinatra::smtp {
struct email_server {
  std::string server;
  std::string port;
  std::string user;
  std::string password;
};
struct email_data {
  std::string from_email;
  std::vector<std::string> to_email;
  std::string subject;
  std::string text;
  std::string filepath;
};

template <typename T>
class client {
 public:
  static constexpr bool IS_SSL = std::is_same_v<T, cinatra::SSL>;
  client(asio::io_service &io_service)
      : io_context_(io_service), socket_(io_service), resolver_(io_service) {}

  ~client() { close(); }

  void set_email_server(const email_server &server) { server_ = server; }
  void set_email_data(const email_data &data) { data_ = data; }

  void start() {
    std::string host = server_.server;
    size_t pos = host.find("://");
    if (pos != std::string::npos) {
      host.erase(0, pos + 3);
    }

    asio::ip::tcp::resolver::query qry(
        host, server_.port, asio::ip::resolver_query_base::numeric_service);
    std::error_code ec;
    auto endpoint_iterator = resolver_.resolve(qry, ec);
    asio::connect(socket_, endpoint_iterator, ec);
    if (ec) {
      return;
    }

    if constexpr (IS_SSL) {
      upgrade_to_ssl();
    }

    build_request();

    asio::write(socket(), request_, ec);
    if (ec) {
      return;
    }

    while (true) {
      asio::read(socket(), response_, asio::transfer_at_least(1), ec);
      if (ec) {
        return;
      }

      std::stringstream stream;
      stream << &response_;
      std::string content = stream.str();

      if (content.find("250 Mail OK") != std::string::npos) {
        return;
      }
    }
  }

 private:
  auto &socket() {
#ifdef CINATRA_ENABLE_SSL
    if constexpr (IS_SSL) {
      assert(ssl_socket_);
      return *ssl_socket_;
    }
    else
#endif
    {
      return socket_;
    }
  }
  void upgrade_to_ssl() {
#ifdef CINATRA_ENABLE_SSL
    asio::ssl::context ctx(asio::ssl::context::sslv23);
    ctx.set_default_verify_paths();
    ctx.set_verify_mode(ctx.verify_fail_if_no_peer_cert);

    ssl_socket_ = std::make_unique<asio::ssl::stream<asio::ip::tcp::socket &>>(
        socket_, ctx);
    ssl_socket_->set_verify_mode(asio::ssl::verify_none);
    ssl_socket_->set_verify_callback([](auto preverified, auto &ctx) {
      char subject_name[256];
      X509 *cert = X509_STORE_CTX_get_current_cert(ctx.native_handle());
      X509_NAME_oneline(X509_get_subject_name(cert), subject_name, 256);

      return preverified;
    });

    std::error_code ec;
    ssl_socket_->handshake(asio::ssl::stream_base::client, ec);
#endif
  }

  std::string load_file_contents(const std::string &filepath) {
    std::ifstream fin(filepath.c_str(), std::ios::in | std::ios::binary);
    if (!fin) {
      throw std::invalid_argument("not exist");
    }

    std::ostringstream oss;
    oss << fin.rdbuf();
    return oss.str();
  }

  void build_smtp_content(std::ostream &out) {
    out << "Content-Type: multipart/mixed; boundary=\"cinatra\"\r\n\r\n";
    out << "--cinatra\r\nContent-Type: text/plain;\r\n\r\n";
    out << data_.text << "\r\n\r\n";
  }

  void build_smtp_file(std::ostream &out) {
    if (data_.filepath.empty()) {
      return;
    }

    std::string filename =
        std::filesystem::path(data_.filepath).filename().string();
    out << "--cinatra\r\nContent-Type: application/octet-stream; name=\""
        << filename << "\"\r\n";
    out << "Content-Transfer-Encoding: base64\r\n";
    out << "Content-Disposition: attachment; filename=\"" << filename
        << "\"\r\n";
    out << "\r\n";

    std::string file_content = load_file_contents(data_.filepath);
    size_t file_size = file_content.size();

    std::string encoded = base64_encode(file_content);

    int SEND_BUF_SIZE = 1024;

    int no_of_rows = (int)file_size / SEND_BUF_SIZE + 1;

    for (int i = 0; i != no_of_rows; ++i) {
      std::string sub_buf = encoded.substr(i * SEND_BUF_SIZE, SEND_BUF_SIZE);

      out << sub_buf << "\r\n";
    }
  }

  void build_request() {
    std::ostream out(&request_);

    out << "EHLO " << server_.server << "\r\n";
    out << "AUTH LOGIN\r\n";
    out << base64_encode(server_.user) << "\r\n";
    out << base64_encode(server_.password) << "\r\n";
    out << "MAIL FROM:<" << data_.from_email << ">\r\n";
    for (auto to : data_.to_email) out << "RCPT TO:<" << to << ">\r\n";
    out << "DATA\r\n";
    out << "FROM: " << data_.from_email << "\r\n";
    for (auto to : data_.to_email) out << "TO: " << to << "\r\n";
    out << "SUBJECT: " << data_.subject << "\r\n";

    build_smtp_content(out);
    build_smtp_file(out);

    out << "--cinatra--\r\n";
    out << ".\r\n";
  }

  void close() {
    std::error_code ignore_ec;
    if constexpr (IS_SSL) {
#ifdef CINATRA_ENABLE_SSL
      ssl_socket_->shutdown(ignore_ec);
#endif
    }

    socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
    socket_.close(ignore_ec);
  }

 private:
  asio::io_context &io_context_;
  asio::ip::tcp::socket socket_;
#ifdef CINATRA_ENABLE_SSL
  std::unique_ptr<asio::ssl::stream<asio::ip::tcp::socket &>> ssl_socket_;
#endif
  asio::ip::tcp::resolver resolver_;

  email_server server_;
  email_data data_;

  asio::streambuf request_;
  asio::streambuf response_;
};

template <typename T>
static inline auto get_smtp_client(asio::io_service &io_service) {
  return smtp::client<T>(io_service);
}

}  // namespace cinatra::smtp
