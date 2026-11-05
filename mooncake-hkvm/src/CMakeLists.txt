# mooncake-hkvm / src
#
# The DiffServer consumes Mooncake KV events over ZMQ (libzmq) and decodes the
# msgpack payload produced by KvEventPublisher, so both libraries are required.
# libzmq is linked; msgpack-c is header-only (include path only).

find_path(ZMQ_INCLUDE_DIR
  NAMES zmq.h
  PATHS /usr/include /usr/local/include /opt/homebrew/include)
find_library(ZMQ_LIBRARY
  NAMES zmq libzmq
  PATHS /usr/lib /usr/local/lib /usr/lib64 /opt/homebrew/lib)
find_path(MSGPACK_INCLUDE_DIR
  NAMES msgpack.hpp
  PATHS /usr/include /usr/local/include /opt/homebrew/include)

if(NOT ZMQ_INCLUDE_DIR OR NOT ZMQ_LIBRARY)
  message(WARNING
    "libzmq not found; skipping mooncake_hkvm_diff. "
    "Install it (e.g. `brew install zeromq` / `apt install libzmq3-dev`).")
  return()
endif()
if(NOT MSGPACK_INCLUDE_DIR)
  message(WARNING
    "msgpack-c not found; skipping mooncake_hkvm_diff. "
    "Install it (e.g. `brew install msgpack-cxx` / `apt install libmsgpack-dev`).")
  return()
endif()

add_library(mooncake_hkvm_diff STATIC diff_server.cpp)
target_include_directories(mooncake_hkvm_diff PUBLIC
  ${CMAKE_SOURCE_DIR}/include
  ${ZMQ_INCLUDE_DIR}
  ${MSGPACK_INCLUDE_DIR})
target_compile_definitions(mooncake_hkvm_diff PRIVATE MOONCAKE_ENABLE_KV_EVENTS=1)
target_link_libraries(mooncake_hkvm_diff PUBLIC ${ZMQ_LIBRARY})

# Example CLI driver: subscribes to two masters and prints the KV diff.
add_executable(mooncake_hkvm_diff_example
  ${CMAKE_SOURCE_DIR}/examples/diff_server_main.cpp)
target_link_libraries(mooncake_hkvm_diff_example PRIVATE mooncake_hkvm_diff)

# --- CopyEngine: reconciles two masters' KV stores toward identity. ---
# Requires the full mooncake build (mooncake_store + transfer_engine targets).
# This block is skipped in the standalone diff-server build.
if(TARGET mooncake_store AND TARGET transfer_engine)
  add_library(mooncake_hkvm_copy STATIC copy_engine.cpp)
  target_link_libraries(mooncake_hkvm_copy
    PUBLIC  mooncake_hkvm_diff
    PRIVATE mooncake_store transfer_engine)

  add_executable(mooncake_hkvm_copy_example
    ${CMAKE_SOURCE_DIR}/examples/copy_engine_main.cpp)
  target_link_libraries(mooncake_hkvm_copy_example PRIVATE mooncake_hkvm_copy)
else()
  message(STATUS "mooncake_store/transfer_engine targets not present; "
                 "skipping mooncake_hkvm_copy (build under top-level mooncake "
                 "with WITH_HKVM to enable).")
endif()
