#include <string>

#include "doctest.h"
#include "ylt/coro_http/coro_http_client.hpp"
#include "ylt/coro_http/coro_http_server.hpp"

using namespace coro_http;

std::string_view REQ =
    "R(GET /wp-content/uploads/2010/03/hello-kitty-darth-vader-pink.jpg "
    "HTTP/1.1\r\n"
    "Host: www.kittyhell.com\r\n"
    "User-Agent: Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.6; ja-JP-mac; "
    "rv:1.9.2.3) Gecko/20100401 Firefox/3.6.3 "
    "Pathtraq/0.9\r\n"
    "Accept: "
    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8\r\n"
    "Accept-Language: ja,en-us;q=0.7,en;q=0.3\r\n"
    "Accept-Encoding: gzip,deflate\r\n"
    "Accept-Charset: Shift_JIS,utf-8;q=0.7,*;q=0.7\r\n"
    "Keep-Alive: 115\r\n"
    "Connection: keep-alive\r\n"
    "Cookie: wp_ozh_wsa_visits=2; wp_ozh_wsa_visit_lasttime=xxxxxxxxxx; "
    "__utma=xxxxxxxxx.xxxxxxxxxx.xxxxxxxxxx.xxxxxxxxxx.xxxxxxxxxx.x; "
    "__utmz=xxxxxxxxx.xxxxxxxxxx.x.x.utmccn=(referral)|utmcsr=reader.livedoor."
    "com|utmcct=/reader/|utmcmd=referral\r\n"
    "\r\n)";

std::string_view multipart_str =
    "R(POST / HTTP/1.1\r\n"
    "User-Agent: PostmanRuntime/7.39.0\r\n"
    "Accept: */*\r\n"
    "Cache-Control: no-cache\r\n"
    "Postman-Token: 33c25732-1648-42ed-a467-cc9f1eb1e961\r\n"
    "Host: purecpp.cn\r\n"
    "Accept-Encoding: gzip, deflate, br\r\n"
    "Connection: keep-alive\r\n"
    "Content-Type: multipart/form-data; "
    "boundary=--------------------------559980232503017651158362\r\n"
    "Cookie: CSESSIONID=87343c8a24f34e28be05efea55315aab\r\n"
    "\r\n"
    "----------------------------559980232503017651158362\r\n"
    "Content-Disposition: form-data; name=\"test\"\r\n"
    "tom\r\n"
    "----------------------------559980232503017651158362--\r\n";

std::string_view bad_multipart_str =
    "R(POST / HTTP/1.1\r\n"
    "User-Agent: PostmanRuntime/7.39.0\r\n"
    "Accept: */*\r\n"
    "Cache-Control: no-cache\r\n"
    "Postman-Token: 33c25732-1648-42ed-a467-cc9f1eb1e961\r\n"
    "Host: purecpp.cn\r\n"
    "Accept-Encoding: gzip, deflate, br\r\n"
    "Connection: keep-alive\r\n"
    "Content-Type: multipart/form-data; boundary=559980232503017651158362\r\n"
    "Cookie: CSESSIONID=87343c8a24f34e28be05efea55315aab\r\n"
    "\r\n"
    "559980232503017651158362\r\n"
    "Content-Disposition: form-data; name=\"test\"\r\n"
    "tom\r\n"
    "559980232503017651158362--\r\n";

std::string_view resp_str =
    "R(HTTP/1.1 400 Bad Request\r\n"
    "Connection: keep-alive\r\n"
    "Content-Length: 20\r\n"
    "Host: cinatra\r\n"
    "\r\n\r\n"
    "the url is not right)";

TEST_CASE("http_parser test") {
  http_parser parser{};
  parser.parse_request(REQ.data(), REQ.size(), 0);
  CHECK(parser.body_len() == 0);
  CHECK(parser.body_len() + parser.header_len() == parser.total_len());
  CHECK(parser.has_connection());

  parser = {};
  std::string_view str(REQ.data(), 20);
  int ret = parser.parse_request(str.data(), str.size(), 0);
  CHECK(ret < 0);

  parser = {};
  ret = parser.parse_request(multipart_str.data(), multipart_str.size(), 0);
  CHECK(ret > 0);
  auto boundary = parser.get_boundary();
  CHECK(boundary == "--------------------------559980232503017651158362");

  parser = {};
  ret = parser.parse_request(bad_multipart_str.data(), bad_multipart_str.size(),
                             0);
  CHECK(ret > 0);
  auto bad_boundary = parser.get_boundary();
  CHECK(bad_boundary.empty());

  parser = {};
  std::string_view part_resp(resp_str.data(), 20);
  ret = parser.parse_response(part_resp.data(), part_resp.size(), 0);
  CHECK(ret < 0);
}

std::string_view req_str =
    "R(GET /wp-content/uploads/2010/03/hello-kitty-darth-vader-pink.jpg "
    "HTTP/1.1\r\n"
    "Content-Type: application/octet-stream"
    "Host: cinatra\r\n"
    "\r\n)";

std::string_view req_str1 =
    "R(GET /ws "
    "HTTP/1.1\r\n"
    "Connection: upgrade\r\n"
    "Upgrade: cinatra\r\n"
    "\r\n)";

std::string_view req_str2 =
    "R(GET /ws "
    "HTTP/1.1\r\n"
    "Connection: upgrade\r\n"
    "Upgrade: websocket\r\n"
    "\r\n)";

std::string_view req_str3 =
    "R(GET /ws "
    "HTTP/1.1\r\n"
    "Connection: upgrade\r\n"
    "Upgrade: websocket\r\n"
    "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
    "Sec-WebSocket-Extensions: permessage-deflate\r\n"
    "\r\n)";

std::string_view req_str4 =
    "R(GET /ws "
    "HTTP/1.1\r\n"
    "Connection: upgrade\r\n"
    "Upgrade: websocket\r\n"
    "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
    "Content-Encoding: gzip\r\n"
    "\r\n)";

std::string_view req_str5 =
    "R(GET /ws "
    "HTTP/1.1\r\n"
    "Connection: upgrade\r\n"
    "Upgrade: websocket\r\n"
    "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
    "Content-Encoding: deflate\r\n"
    "\r\n)";

std::string_view req_str6 =
    "R(GET /ws "
    "HTTP/1.1\r\n"
    "Connection: upgrade\r\n"
    "Upgrade: websocket\r\n"
    "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
    "Content-Encoding: br\r\n"
    "\r\n)";

std::string_view req_str7 =
    "R(GET /ws "
    "HTTP/1.1\r\n"
    "Connection: upgrade\r\n"
    "Upgrade: websocket\r\n"
    "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
    "Content-Encoding: cinatra\r\n"
    "\r\n)";

TEST_CASE("http_request test") {
  http_parser parser{};
  int ret = parser.parse_request(req_str.data(), req_str.size(), 0);
  CHECK(ret);
  coro_http_request req(parser, nullptr);
  CHECK(parser.msg().empty());

  CHECK(req.get_accept_encoding().empty());
  CHECK(req.get_content_type() == content_type::octet_stream);
  CHECK(req.get_boundary().empty());

  req.set_aspect_data(std::string("test"));
  CHECK(req.get_aspect_data().size() == 1);
  req.set_aspect_data(std::vector<std::string>{"test", "aspect"});
  CHECK(req.get_aspect_data().size() == 2);
  CHECK(!req.is_support_compressed());
  CHECK(!req.is_upgrade());

  parser = {};
  parser.parse_request(req_str2.data(), req_str2.size(), 0);
  CHECK(!req.is_upgrade());

  parser = {};
  parser.parse_request(req_str3.data(), req_str3.size(), 0);
  CHECK(req.is_upgrade());
  CHECK(req.is_support_compressed());
  CHECK(req.get_encoding_type() == content_encoding::none);

  parser = {};
  parser.parse_request(req_str4.data(), req_str4.size(), 0);
  CHECK(req.is_upgrade());
  CHECK(req.get_encoding_type() == content_encoding::gzip);

  parser = {};
  parser.parse_request(req_str5.data(), req_str5.size(), 0);
  CHECK(req.is_upgrade());
  CHECK(req.get_encoding_type() == content_encoding::deflate);

  parser = {};
  parser.parse_request(req_str6.data(), req_str6.size(), 0);
  CHECK(req.is_upgrade());
  CHECK(req.get_encoding_type() == content_encoding::br);

  parser = {};
  parser.parse_request(req_str7.data(), req_str7.size(), 0);
  CHECK(req.is_upgrade());
  CHECK(req.get_encoding_type() == content_encoding::none);
}

TEST_CASE("uri test") {
  std::string uri = "https://example.com?name=tom";
  uri_t u;
  bool r = u.parse_from(uri.data());
  CHECK(r);
  CHECK(u.get_port() == "443");
  context c{u, http_method::GET};
  context c1{u, http_method::GET, "test"};
  CHECK(u.get_query() == "name=tom");

  uri = "https://example.com:521?name=tom";
  r = u.parse_from(uri.data());
  CHECK(r);
  CHECK(u.get_port() == "521");

  uri = "#https://example.com?name=tom";
  r = u.parse_from(uri.data());
  CHECK(!r);

  uri = "https##://example.com?name=tom";
  r = u.parse_from(uri.data());
  CHECK(!r);

  uri = "https://^example.com?name=tom";
  r = u.parse_from(uri.data());
  CHECK(!r);

  uri = "https://example.com?^name=tom";
  r = u.parse_from(uri.data());
  CHECK(!r);

  uri = "http://username:password@example.com";
  r = u.parse_from(uri.data());
  CHECK(r);
  CHECK(u.uinfo == "username:password");

  uri = "http://example.com/data.csv#row=4";
  r = u.parse_from(uri.data());
  CHECK(r);
  CHECK(u.fragment == "row=4");

  uri = "https://example.com?name=tom$";
  r = u.parse_from(uri.data());
  CHECK(r);

  uri = "https://example.com?name=tom!";
  r = u.parse_from(uri.data());
  CHECK(r);
}
