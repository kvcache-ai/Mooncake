# CMake integration for the transfer_engine_rust package.
#
# Added when the parent CMakeLists.txt sets WITH_RUST_EXAMPLE=ON. Depends on the
# transfer_engine C++ target so libtransfer_engine is available before cargo
# links against it.

set(_te_rust_env
    CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}
    MOONCAKE_BUILD_DIR=${CMAKE_BINARY_DIR}
    MOONCAKE_TE_LIB_DIR=${CMAKE_CURRENT_BINARY_DIR}/../src
    MOONCAKE_TE_INCLUDE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/../include)
if(USE_ETCD)
  list(APPEND _te_rust_env MOONCAKE_WITH_ETCD=1)
endif()
if(USE_CUDA)
  list(APPEND _te_rust_env MOONCAKE_WITH_CUDA=1)
endif()
if(NOT USE_EFA)
  list(APPEND _te_rust_env MOONCAKE_WITHOUT_LIBFABRIC=1)
endif()
if(ENABLE_ASAN)
  list(APPEND _te_rust_env MOONCAKE_LINK_ASAN=1)
endif()

add_custom_target(build_transfer_engine_rust DEPENDS transfer_engine)
add_custom_command(
  TARGET build_transfer_engine_rust
  COMMAND ${CMAKE_COMMAND} -E env ${_te_rust_env} cargo build --release
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Building transfer_engine_rust library"
  VERBATIM)
set_property(TARGET build_transfer_engine_rust PROPERTY EXCLUDE_FROM_ALL FALSE)

add_custom_target(build_transfer_engine_rust_example DEPENDS transfer_engine)
add_custom_command(
  TARGET build_transfer_engine_rust_example
  COMMAND ${CMAKE_COMMAND} -E env ${_te_rust_env} cargo build --examples
          --release
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Building transfer_engine_rust examples"
  VERBATIM)

add_custom_target(build_transfer_engine_rust_tests DEPENDS transfer_engine)
add_custom_command(
  TARGET build_transfer_engine_rust_tests
  COMMAND ${CMAKE_COMMAND} -E env ${_te_rust_env} cargo test --tests --no-run
          --release
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Building transfer_engine_rust tests"
  VERBATIM)
