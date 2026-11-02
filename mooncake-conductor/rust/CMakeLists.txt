# CMake integration for the mooncake_conductor Rust package.
#
# This target is only added when the parent CMakeLists.txt sets
# WITH_CONDUCTOR_RUST=ON.  It depends on the conductor_cpp_core C++ target so
# that the static library is available before cargo tries to link against it.

add_custom_target(build_mooncake_conductor_rust DEPENDS conductor_cpp_core)

add_custom_command(
  TARGET build_mooncake_conductor_rust
  POST_BUILD
  COMMAND
    ${CMAKE_COMMAND} -E env
    # Where cargo should place its build artefacts.
    CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}
    MOONCAKE_BUILD_DIR=${PROJECT_BINARY_DIR}
    # Directory containing libconductor_cpp_core.a.
    MOONCAKE_CONDUCTOR_LIB_DIR=${CMAKE_CURRENT_BINARY_DIR}/..
    # Directory containing conductor/client/conductor_c.h.
    MOONCAKE_CONDUCTOR_INCLUDE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/../include cargo
    build --release
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Building mooncake_conductor Rust package"
  VERBATIM)

add_custom_target(build_mooncake_conductor_rust_tests
                  DEPENDS conductor_cpp_core)

add_custom_command(
  TARGET build_mooncake_conductor_rust_tests
  POST_BUILD
  COMMAND
    ${CMAKE_COMMAND} -E env CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}
    MOONCAKE_BUILD_DIR=${PROJECT_BINARY_DIR}
    MOONCAKE_CONDUCTOR_LIB_DIR=${CMAKE_CURRENT_BINARY_DIR}/..
    MOONCAKE_CONDUCTOR_INCLUDE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/../include cargo
    test --tests --no-run --release
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Building mooncake_conductor Rust tests"
  VERBATIM)
