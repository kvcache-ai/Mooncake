#pragma once
#include <string_view>

#include "define.h"

namespace cinatra {

enum class status_type {
  init,
  http_continue = 100,
  switching_protocols = 101,
  processing = 102,
  ok = 200,
  created = 201,
  accepted = 202,
  nonauthoritative = 203,
  no_content = 204,
  reset_content = 205,
  partial_content = 206,
  multi_status = 207,
  already_reported = 208,
  im_used = 226,
  multiple_choices = 300,
  moved_permanently = 301,
  moved_temporarily = 302,
  not_modified = 304,
  use_proxy = 305,
  temporary_redirect = 307,
  permanent_redirect = 308,
  bad_request = 400,
  unauthorized = 401,
  payment_required = 402,
  forbidden = 403,
  not_found = 404,
  method_not_allowed = 405,
  not_acceptable = 406,
  proxy_authentication_required = 407,
  request_timeout = 408,
  conflict = 409,
  gone = 410,
  length_required = 411,
  precondition_failed = 412,
  request_entity_too_large = 413,
  request_uri_too_long = 414,
  unsupported_media_type = 415,
  range_not_satisfiable = 416,
  expectation_failed = 417,
  im_a_teapot = 418,
  enchance_your_calm = 420,
  misdirected_request = 421,
  unprocessable_entity = 422,
  locked = 423,
  failed_dependency = 424,
  too_early = 425,
  upgrade_required = 426,
  precondition_required = 428,
  too_many_requests = 429,
  request_header_fields_too_large = 431,
  unavailable_for_legal_reasons = 451,
  internal_server_error = 500,
  not_implemented = 501,
  bad_gateway = 502,
  service_unavailable = 503,
  gateway_timeout = 504,
  version_not_supported = 505,
  variant_also_negotiates = 506,
  insufficient_storage = 507,
  loop_detected = 508,
  not_extended = 510,
  network_authentication_required = 511
};

// http response status string
namespace http_status_string {
inline constexpr std::string_view http_continue = "HTTP/1.1 100 Continue\r\n";
inline constexpr std::string_view switching_protocols =
    "HTTP/1.1 101 Switching Protocals\r\n";
inline constexpr std::string_view processing = "HTTP/1.1 102 Processing\r\n";
inline constexpr std::string_view rep_ok = "HTTP/1.1 200 OK\r\n";
inline constexpr std::string_view rep_created = "HTTP/1.1 201 Created\r\n";
inline constexpr std::string_view rep_accepted = "HTTP/1.1 202 Accepted\r\n";
inline constexpr std::string_view rep_nonauthoritative =
    "HTTP/1.1 203 Nonauthoritative\r\n";
inline constexpr std::string_view rep_no_content =
    "HTTP/1.1 204 No Content\r\n";
inline constexpr std::string_view rep_reset_content =
    "HTTP/1.1 205 Reset Content\r\n";
inline constexpr std::string_view rep_partial_content =
    "HTTP/1.1 206 Partial Content\r\n";
inline constexpr std::string_view rep_multi_status =
    "HTTP/1.1 207 Multi Status\r\n";
inline constexpr std::string_view rep_already_reported =
    "HTTP/1.1 208 Already Reported\r\n";
inline constexpr std::string_view rep_im_used = "HTTP/1.1 226 Im Used\r\n";
inline constexpr std::string_view rep_multiple_choices =
    "HTTP/1.1 300 Multiple Choices\r\n";
inline constexpr std::string_view rep_moved_permanently =
    "HTTP/1.1 301 Moved Permanently\r\n";
inline constexpr std::string_view rep_moved_temporarily =
    "HTTP/1.1 302 Moved Temporarily\r\n";
inline constexpr std::string_view rep_not_modified =
    "HTTP/1.1 304 Not Modified\r\n";
inline constexpr std::string_view rep_use_proxy = "HTTP/1.1 305 Use Proxy\r\n";
inline constexpr std::string_view rep_temporary_redirect =
    "HTTP/1.1 307 Temporary Redirect\r\n";
inline constexpr std::string_view rep_permanent_redirect =
    "HTTP/1.1 308 Permanent Redirect\r\n";
inline constexpr std::string_view rep_bad_request =
    "HTTP/1.1 400 Bad Request\r\n";
inline constexpr std::string_view rep_unauthorized =
    "HTTP/1.1 401 Unauthorized\r\n";
inline constexpr std::string_view rep_payment_required =
    "HTTP/1.1 402 Payment Required\r\n";
inline constexpr std::string_view rep_forbidden = "HTTP/1.1 403 Forbidden\r\n";
inline constexpr std::string_view rep_not_found = "HTTP/1.1 404 Not Found\r\n";
inline constexpr std::string_view rep_method_not_allowed =
    "HTTP/1.1 405 Method Not Allowed\r\n";
inline constexpr std::string_view rep_not_acceptable =
    "HTTP/1.1 406 Not Acceptable\r\n";
inline constexpr std::string_view rep_proxy_authentication_required =
    "HTTP/1.1 407 Proxy Authentication Required\r\n";
inline constexpr std::string_view rep_request_timeout =
    "HTTP/1.1 408 Request Timeout\r\n";
inline constexpr std::string_view rep_conflict = "HTTP/1.1 409 Conflict\r\n";
inline constexpr std::string_view rep_gone = "HTTP/1.1 410 Gone\r\n";
inline constexpr std::string_view rep_length_required =
    "HTTP/1.1 411 Length Required\r\n";
inline constexpr std::string_view rep_precondition_failed =
    "HTTP/1.1 412 Precondition Failed\r\n";
inline constexpr std::string_view rep_request_entity_too_large =
    "HTTP/1.1 413 Request Entity Too Large\r\n";
inline constexpr std::string_view rep_request_uri_too_long =
    "HTTP/1.1 414 Request Uri Too Long\r\n";
inline constexpr std::string_view rep_unsupported_media_type =
    "HTTP/1.1 415 Unsupported Media Type\r\n";
inline constexpr std::string_view rep_range_not_satisfiable =
    "HTTP/1.1 416 Requested Range Not Satisfiable\r\n";
inline constexpr std::string_view rep_expectation_failed =
    "HTTP/1.1 417 Expectation Failed\r\n";
inline constexpr std::string_view rep_im_a_teapot =
    "HTTP/1.1 418 Im a Teapot\r\n";
inline constexpr std::string_view rep_enchance_your_calm =
    "HTTP/1.1 420 Enchance Your Calm\r\n";
inline constexpr std::string_view rep_misdirected_request =
    "HTTP/1.1 421 Misdirected Request\r\n";
inline constexpr std::string_view rep_unprocessable_entity =
    "HTTP/1.1 422 Unprocessable Entity\r\n";
inline constexpr std::string_view rep_locked = "HTTP/1.1 423 Locked\r\n";
inline constexpr std::string_view rep_failed_dependency =
    "HTTP/1.1 424 Failed_Dependency\r\n";
inline constexpr std::string_view rep_too_early = "HTTP/1.1 425 Too Early\r\n";
inline constexpr std::string_view rep_upgrade_required =
    "HTTP/1.1 426 Upgrade Required\r\n";
inline constexpr std::string_view rep_precondition_required =
    "HTTP/1.1 428 Precondition Required\r\n";
inline constexpr std::string_view rep_too_many_requests =
    "HTTP/1.1 429 Too Many Requests\r\n";
inline constexpr std::string_view rep_request_header_fields_too_large =
    "HTTP/1.1 431 Request Header Fields Too Large\r\n";
inline constexpr std::string_view rep_unavailable_for_legal_reasons =
    "HTTP/1.1 451 Unavailabl For Legal Reasons\r\n";
inline constexpr std::string_view rep_internal_server_error =
    "HTTP/1.1 500 Internal Server Error\r\n";
inline constexpr std::string_view rep_not_implemented =
    "HTTP/1.1 501 Not Implemented\r\n";
inline constexpr std::string_view rep_bad_gateway =
    "HTTP/1.1 502 Bad Gateway\r\n";
inline constexpr std::string_view rep_service_unavailable =
    "HTTP/1.1 503 Service Unavailable\r\n";
inline constexpr std::string_view rep_gateway_timeout =
    "HTTP/1.1 504 Gateway Timeout\r\n";
inline constexpr std::string_view rep_version_not_supported =
    "HTTP/1.1 505 Version Not Supported\r\n";
inline constexpr std::string_view rep_variant_also_negotiates =
    "HTTP/1.1 506 Variant Also Negotiates\r\n";
inline constexpr std::string_view rep_insufficient_storage =
    "HTTP/1.1 507 Insufficient Storage\r\n";
inline constexpr std::string_view rep_loop_detected =
    "HTTP/1.1 508 Loop Detected\r\n";
inline constexpr std::string_view rep_not_extended =
    "HTTP/1.1 510 Not Extended\r\n";
inline constexpr std::string_view rep_network_authentication_required =
    "HTTP/1.1 511 Network Authentication Required\r\n";
}  // namespace http_status_string

inline constexpr std::string_view to_http_status_string(status_type status) {
  using namespace http_status_string;
  switch (status) {
    case cinatra::status_type::switching_protocols:
      return switching_protocols;
    case cinatra::status_type::ok:
      return rep_ok;
    case cinatra::status_type::created:
      return rep_created;
    case cinatra::status_type::accepted:
      return rep_accepted;
    case cinatra::status_type::no_content:
      return rep_no_content;
    case cinatra::status_type::partial_content:
      return rep_partial_content;
    case cinatra::status_type::multiple_choices:
      return rep_multiple_choices;
    case cinatra::status_type::moved_permanently:
      return rep_moved_permanently;
    case cinatra::status_type::moved_temporarily:
      return rep_moved_temporarily;
    case cinatra::status_type::not_modified:
      return rep_not_modified;
    case cinatra::status_type::temporary_redirect:
      return rep_temporary_redirect;
    case cinatra::status_type::bad_request:
      return rep_bad_request;
    case cinatra::status_type::unauthorized:
      return rep_unauthorized;
    case cinatra::status_type::forbidden:
      return rep_forbidden;
    case cinatra::status_type::not_found:
      return rep_not_found;
    case cinatra::status_type::method_not_allowed:
      return rep_method_not_allowed;
    case cinatra::status_type::conflict:
      return rep_conflict;
    case cinatra::status_type::range_not_satisfiable:
      return rep_range_not_satisfiable;
    case cinatra::status_type::internal_server_error:
      return rep_internal_server_error;
    case cinatra::status_type::not_implemented:
      return rep_not_implemented;
    case cinatra::status_type::bad_gateway:
      return rep_bad_gateway;
    case cinatra::status_type::service_unavailable:
      return rep_service_unavailable;
    default:
      return rep_not_implemented;
  }
}

inline constexpr std::string_view default_status_content(status_type status) {
  std::string_view str = to_http_status_string(status);
  return str.substr(9, str.size() - 11);
}

}  // namespace cinatra
