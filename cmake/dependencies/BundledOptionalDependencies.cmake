# Copyright 2026 KVCache.AI

include_guard(GLOBAL)

set(MOONCAKE_HIREDIS_VERSION "1.4.0")
set(MOONCAKE_LIBURING_VERSION "2.14")
set(MOONCAKE_LIBZMQ_VERSION "4.3.5")

set(_hiredis_dir "${_MOONCAKE_DEPS_ROOT}/extern/hiredis")
set(_liburing_dir "${_MOONCAKE_DEPS_ROOT}/extern/liburing")
set(_libzmq_dir "${_MOONCAKE_DEPS_ROOT}/extern/libzmq")

function(mooncake_enable_bundled_hiredis)
  if(TARGET Mooncake::hiredis)
    return()
  endif()
  _mooncake_require_vendored("${_hiredis_dir}/hiredis.c" hiredis
                             "${MOONCAKE_HIREDIS_VERSION}")
  add_library(
    mooncake_hiredis STATIC EXCLUDE_FROM_ALL
    "${_hiredis_dir}/alloc.c"
    "${_hiredis_dir}/async.c"
    "${_hiredis_dir}/hiredis.c"
    "${_hiredis_dir}/net.c"
    "${_hiredis_dir}/read.c"
    "${_hiredis_dir}/sds.c"
    "${_hiredis_dir}/sockcompat.c")
  target_include_directories(
    mooncake_hiredis SYSTEM PUBLIC "$<BUILD_INTERFACE:${_hiredis_dir}/..>"
                                   "$<BUILD_INTERFACE:${_hiredis_dir}>")
  if(WIN32)
    target_compile_definitions(mooncake_hiredis PRIVATE _CRT_SECURE_NO_WARNINGS
                                                        WIN32_LEAN_AND_MEAN)
    target_link_libraries(mooncake_hiredis PUBLIC ws2_32 crypt32)
  elseif(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
    target_link_libraries(mooncake_hiredis PUBLIC m)
  elseif(CMAKE_SYSTEM_NAME MATCHES "SunOS")
    target_link_libraries(mooncake_hiredis PUBLIC socket)
  endif()
  _mooncake_set_bundled_properties(mooncake_hiredis)
  set_target_properties(mooncake_hiredis PROPERTIES OUTPUT_NAME
                                                    mooncake_hiredis)
  add_library(hiredis ALIAS mooncake_hiredis)
  add_library(hiredis::hiredis ALIAS mooncake_hiredis)
  add_library(Mooncake::hiredis ALIAS mooncake_hiredis)
  message(
    STATUS
      "Mooncake bundled dependency enabled: hiredis ${MOONCAKE_HIREDIS_VERSION}"
  )
endfunction()

function(mooncake_enable_bundled_liburing)
  if(TARGET Mooncake::uring)
    return()
  endif()
  if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(
      FATAL_ERROR "The bundled liburing provider is supported only on Linux")
  endif()
  _mooncake_require_vendored("${_liburing_dir}/src/queue.c" liburing
                             "${MOONCAKE_LIBURING_VERSION}")
  add_library(
    mooncake_uring STATIC EXCLUDE_FROM_ALL
    "${_liburing_dir}/src/setup.c" "${_liburing_dir}/src/queue.c"
    "${_liburing_dir}/src/register.c" "${_liburing_dir}/src/syscall.c"
    "${_liburing_dir}/src/version.c")
  target_compile_definitions(
    mooncake_uring PRIVATE _GNU_SOURCE _LARGEFILE_SOURCE _FILE_OFFSET_BITS=64
                           LIBURING_INTERNAL)
  target_compile_options(mooncake_uring
                         PRIVATE "-include${_liburing_dir}/config-host.h")
  target_include_directories(
    mooncake_uring SYSTEM
    PUBLIC "${_liburing_dir}/src/include"
    PRIVATE "${_liburing_dir}/src")
  _mooncake_set_bundled_properties(mooncake_uring)
  set_target_properties(mooncake_uring PROPERTIES OUTPUT_NAME mooncake_uring)
  add_library(uring ALIAS mooncake_uring)
  add_library(Mooncake::uring ALIAS mooncake_uring)
  message(
    STATUS
      "Mooncake bundled dependency enabled: liburing ${MOONCAKE_LIBURING_VERSION}"
  )
endfunction()

function(mooncake_enable_bundled_libzmq)
  if(TARGET Mooncake::zmq)
    return()
  endif()
  _mooncake_require_vendored("${_libzmq_dir}/src/zmq.cpp" libzmq
                             "${MOONCAKE_LIBZMQ_VERSION}")

  # libzmq's upstream build is feature rich; pin a portable, static core and do
  # not let its optional crypto/docs/tests/install machinery leak into Mooncake.
  set(BUILD_SHARED OFF)
  set(BUILD_STATIC ON)
  set(ENABLE_ASAN OFF)
  set(ENABLE_TSAN OFF)
  set(ENABLE_UBSAN OFF)
  set(ENABLE_DRAFTS OFF)
  set(ENABLE_WS OFF)
  set(ENABLE_RADIX_TREE OFF)
  set(ENABLE_CURVE OFF)
  set(WITH_LIBSODIUM OFF)
  set(WITH_LIBBSD OFF)
  set(WITH_OPENPGM OFF)
  set(WITH_NORM OFF)
  set(WITH_VMCI OFF)
  set(WITH_DOCS OFF)
  set(ENABLE_PRECOMPILED OFF)
  set(ENABLE_CPACK OFF)
  set(ENABLE_CLANG OFF)
  set(LIBZMQ_PEDANTIC OFF)
  set(LIBZMQ_WERROR OFF)
  set(ZMQ_BUILD_TESTS
      OFF
      CACHE BOOL "Build bundled libzmq tests" FORCE)
  # libzmq declares an old standalone CMake minimum, so explicitly select the
  # modern parent-project behavior for normal-variable options and visibility.
  set(CMAKE_POLICY_DEFAULT_CMP0063 NEW)
  set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)
  add_subdirectory("${_libzmq_dir}" "${CMAKE_BINARY_DIR}/_deps/mooncake_libzmq"
                   EXCLUDE_FROM_ALL)
  _mooncake_set_bundled_properties(objects)
  _mooncake_set_bundled_properties(libzmq-static)
  set_target_properties(libzmq-static PROPERTIES OUTPUT_NAME mooncake_zmq)
  add_library(zmq ALIAS libzmq-static)
  add_library(Mooncake::zmq ALIAS libzmq-static)
  message(
    STATUS
      "Mooncake bundled dependency enabled: libzmq ${MOONCAKE_LIBZMQ_VERSION}")
endfunction()
