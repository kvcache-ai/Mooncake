# Build entry points own these targets and call each provider at most once.
# Consumer directories only link the resulting Mooncake::* targets.

function(mooncake_provide_zstd)
  find_path(MOONCAKE_ZSTD_INCLUDE_DIR zstd.h)
  find_library(MOONCAKE_ZSTD_LIBRARY zstd)
  if(NOT MOONCAKE_ZSTD_INCLUDE_DIR OR NOT MOONCAKE_ZSTD_LIBRARY)
    message(FATAL_ERROR "zstd development files not found")
  endif()
  add_library(Mooncake::zstd UNKNOWN IMPORTED)
  set_target_properties(
    Mooncake::zstd
    PROPERTIES IMPORTED_LOCATION "${MOONCAKE_ZSTD_LIBRARY}"
               INTERFACE_INCLUDE_DIRECTORIES "${MOONCAKE_ZSTD_INCLUDE_DIR}")
endfunction()

function(mooncake_provide_cli11)
  find_path(MOONCAKE_CLI11_INCLUDE_DIR CLI/CLI.hpp)
  if(NOT MOONCAKE_CLI11_INCLUDE_DIR)
    message(FATAL_ERROR "CLI11 development files not found (libcli11-dev)")
  endif()
  add_library(Mooncake::cli11 INTERFACE IMPORTED)
  set_target_properties(
    Mooncake::cli11 PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                               "${MOONCAKE_CLI11_INCLUDE_DIR}")
endfunction()

function(mooncake_provide_xxhash)
  find_path(MOONCAKE_XXHASH_INCLUDE_DIR xxhash.h)
  find_library(MOONCAKE_XXHASH_LIBRARY NAMES xxhash libxxhash)
  if(NOT MOONCAKE_XXHASH_INCLUDE_DIR OR NOT MOONCAKE_XXHASH_LIBRARY)
    message(FATAL_ERROR "xxHash development files not found")
  endif()
  add_library(Mooncake::xxhash UNKNOWN IMPORTED)
  set_target_properties(
    Mooncake::xxhash
    PROPERTIES IMPORTED_LOCATION "${MOONCAKE_XXHASH_LIBRARY}"
               INTERFACE_INCLUDE_DIRECTORIES "${MOONCAKE_XXHASH_INCLUDE_DIR}")
endfunction()

function(mooncake_provide_liburing)
  find_path(MOONCAKE_LIBURING_INCLUDE_DIR liburing.h)
  find_library(MOONCAKE_LIBURING_LIBRARY uring)
  if(MOONCAKE_LIBURING_INCLUDE_DIR AND MOONCAKE_LIBURING_LIBRARY)
    add_library(Mooncake::liburing UNKNOWN IMPORTED)
    set_target_properties(
      Mooncake::liburing
      PROPERTIES IMPORTED_LOCATION "${MOONCAKE_LIBURING_LIBRARY}"
                 INTERFACE_INCLUDE_DIRECTORIES
                 "${MOONCAKE_LIBURING_INCLUDE_DIR}")
  endif()
endfunction()

function(mooncake_provide_libzmq)
  find_path(MOONCAKE_LIBZMQ_INCLUDE_DIR zmq.h)
  find_library(MOONCAKE_LIBZMQ_LIBRARY NAMES zmq libzmq)
  if(NOT MOONCAKE_LIBZMQ_INCLUDE_DIR OR NOT MOONCAKE_LIBZMQ_LIBRARY)
    message(FATAL_ERROR "libzmq development files not found")
  endif()
  add_library(Mooncake::libzmq UNKNOWN IMPORTED)
  set_target_properties(
    Mooncake::libzmq
    PROPERTIES IMPORTED_LOCATION "${MOONCAKE_LIBZMQ_LIBRARY}"
               INTERFACE_INCLUDE_DIRECTORIES "${MOONCAKE_LIBZMQ_INCLUDE_DIR}")
endfunction()

function(mooncake_provide_hiredis)
  cmake_parse_arguments(ARG "REQUIRED" "" "" ${ARGN})
  find_path(MOONCAKE_HIREDIS_INCLUDE_DIR hiredis/hiredis.h)
  find_library(MOONCAKE_HIREDIS_LIBRARY hiredis)
  if(ARG_REQUIRED)
    if(NOT MOONCAKE_HIREDIS_INCLUDE_DIR OR NOT MOONCAKE_HIREDIS_LIBRARY)
      message(FATAL_ERROR "hiredis development files not found")
    endif()
  endif()

  if(MOONCAKE_HIREDIS_INCLUDE_DIR AND MOONCAKE_HIREDIS_LIBRARY)
    add_library(Mooncake::hiredis UNKNOWN IMPORTED)
    set_target_properties(
      Mooncake::hiredis
      PROPERTIES IMPORTED_LOCATION "${MOONCAKE_HIREDIS_LIBRARY}"
                 INTERFACE_INCLUDE_DIRECTORIES
                 "${MOONCAKE_HIREDIS_INCLUDE_DIR}")
  endif()
endfunction()

function(mooncake_provide_boost)
  if(TARGET Mooncake::boost)
    return()
  endif()

  include(FetchContent)
  cmake_policy(PUSH)
  if(POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
  endif()

  set(_boost_version "1.92.0")
  set(_boost_sha256
      "ea7b982002cc9dfbe59b0b217b206f470dc75f3de0bb2973d844118934d82411")
  set(_boost_url
      "https://github.com/boostorg/boost/releases/download/boost-${_boost_version}/boost-${_boost_version}-b2-nodocs.tar.xz"
  )
  set(_boost_urls "${_boost_url}")
  if(GH_MIRROR)
    string(REPLACE "," ";" _gh_mirrors "${GH_MIRROR}")
    list(GET _gh_mirrors 0 _first_mirror)
    string(STRIP "${_first_mirror}" _first_mirror)
    if(_first_mirror)
      if(NOT _first_mirror MATCHES "/$")
        set(_first_mirror "${_first_mirror}/")
      endif()
      set(_boost_urls "${_first_mirror}${_boost_url}" "${_boost_url}")
    endif()
  endif()

  if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.18")
    FetchContent_Declare(
      boost_headers
      URL ${_boost_urls}
      URL_HASH SHA256=${_boost_sha256}
      SOURCE_SUBDIR boost)
    FetchContent_MakeAvailable(boost_headers)
  else()
    FetchContent_Declare(
      boost_headers
      URL ${_boost_urls}
      URL_HASH SHA256=${_boost_sha256})
    FetchContent_GetProperties(boost_headers)
    if(NOT boost_headers_POPULATED)
      FetchContent_Populate(boost_headers)
    endif()
  endif()
  cmake_policy(POP)

  FetchContent_GetProperties(boost_headers)
  message(
    STATUS
      "Boost headers: using pinned Boost ${_boost_version} (${boost_headers_SOURCE_DIR})"
  )

  add_library(Mooncake::boost INTERFACE IMPORTED)
  set_target_properties(
    Mooncake::boost
    PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${boost_headers_SOURCE_DIR}"
               INTERFACE_SYSTEM_INCLUDE_DIRECTORIES
               "${boost_headers_SOURCE_DIR}")
endfunction()
