# CMake integration for the Rust-native Mooncake Store (store-rs).
#
# Only added when the top-level CMakeLists.txt sets WITH_STORE_RS=ON, which is
# off by default: this is a large Rust workspace and the C++ build must not
# require a Rust toolchain.
#
# store-rs is its own cargo workspace, matching the existing convention for
# mooncake-store/rust and mooncake-transfer-engine/rust.

if(NOT TARGET transfer_engine)
  message(FATAL_ERROR "WITH_STORE_RS=ON requires WITH_TE=ON")
endif()
if(NOT TARGET tent_shared)
  message(FATAL_ERROR "WITH_STORE_RS=ON requires USE_TENT=ON")
endif()

find_program(CARGO_EXECUTABLE cargo)
if(NOT CARGO_EXECUTABLE)
  message(FATAL_ERROR "WITH_STORE_RS=ON requires cargo on PATH")
endif()

# store-rs compiles its native shims against the TE and TENT artifacts from
# this CMake build tree.
add_custom_target(build_store_rs ALL
  COMMAND
    ${CMAKE_COMMAND} -E env
    CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}
    MOONCAKE_ROOT_DIR=${PROJECT_SOURCE_DIR}
    MOONCAKE_BUILD_DIR=${PROJECT_BINARY_DIR}
    MOONCAKE_STORE_RS_DIR=${CMAKE_CURRENT_SOURCE_DIR}
    ${CARGO_EXECUTABLE} build --release
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Building mooncake-store-rs (Rust workspace)"
  VERBATIM)
add_dependencies(build_store_rs transfer_engine tent_shared)

# Tests are compiled but not run here; running them needs Redis or etcd.
add_custom_target(build_store_rs_tests
  COMMAND
    ${CMAKE_COMMAND} -E env
    CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}
    MOONCAKE_ROOT_DIR=${PROJECT_SOURCE_DIR}
    MOONCAKE_BUILD_DIR=${PROJECT_BINARY_DIR}
    MOONCAKE_STORE_RS_DIR=${CMAKE_CURRENT_SOURCE_DIR}
    ${CARGO_EXECUTABLE} test --lib --no-run --release
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Building mooncake-store-rs tests"
  VERBATIM)
add_dependencies(build_store_rs_tests transfer_engine tent_shared)
