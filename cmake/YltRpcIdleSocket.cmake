# The pinned YLT client has no public idle-socket accessor. Add that accessor
# in a generated include overlay, leaving FetchContent sources (including
# user-provided source overrides) immutable. No framing or I/O code changes.
set(_rpc_header
    "${yalantinglibs_SOURCE_DIR}/include/ylt/coro_rpc/impl/coro_rpc_client.hpp")
file(SHA256 "${_rpc_header}" _rpc_hash)
if(NOT _rpc_hash STREQUAL
   "b9df4e598376f63ab54ef714de8c53cf0be536b2edab411834716ca6caf0fcbf")
  message(
    FATAL_ERROR "Re-audit the TENT idle-socket accessor for this YLT revision")
endif()
file(READ "${_rpc_header}" _rpc_source)
string(
  REPLACE
    "  void close() { close_socket_async(control_); }"
    "  // Only for the exclusive idle owner, running on get_executor().
  asio::ip::tcp::socket* tcp_socket_for_idle_check() noexcept {
    return control_->socket_wrapper_.socket().get();
  }

  void close() { close_socket_async(control_); }"
    _rpc_source
    "${_rpc_source}")
# Resolve sibling includes to their original files, avoiding duplicate types
# from copied headers with distinct pragma-once identities.
foreach(_relative common_service.hpp context.hpp expected.hpp
                  protocol/coro_rpc_protocol.hpp inject_action.hpp)
  string(REPLACE "\"${_relative}\"" "\"ylt/coro_rpc/impl/${_relative}\""
                 _rpc_source "${_rpc_source}")
endforeach()
set(_rpc_overlay "${yalantinglibs_BINARY_DIR}/tent-rpc-include")
file(
  GENERATE
  OUTPUT "${_rpc_overlay}/ylt/coro_rpc/impl/coro_rpc_client.hpp"
  CONTENT "${_rpc_source}")
file(
  GENERATE
  OUTPUT "${_rpc_overlay}/ylt/coro_rpc/coro_rpc_client.hpp"
  CONTENT "#pragma once\n#include <ylt/coro_rpc/impl/coro_rpc_client.hpp>\n")
target_include_directories(yalantinglibs BEFORE
                           INTERFACE "$<BUILD_INTERFACE:${_rpc_overlay}>")
