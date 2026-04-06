# CMake integration for the mooncake_store Rust crate.
#
# This target is only added when the parent CMakeLists.txt sets
# WITH_STORE_RUST=ON.  It depends on the mooncake_store C++ target so that
# the static library is available before cargo tries to link against it.

add_custom_target(build_mooncake_store_rust DEPENDS mooncake_store)

add_custom_command(
    TARGET build_mooncake_store_rust
    COMMAND
        ${CMAKE_COMMAND} -E env
            # Where cargo should place its build artefacts.
            CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}
            # Directory containing libmooncake_store.a (or .so).
            MOONCAKE_STORE_LIB_DIR=${CMAKE_CURRENT_BINARY_DIR}/../src
            # Directory containing store_c.h.
            MOONCAKE_STORE_INCLUDE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/../include
        cargo build --release
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Building mooncake_store Rust crate"
    VERBATIM
)

# Build the basic_usage example so that CI can verify it compiles.
add_custom_target(build_mooncake_store_rust_example DEPENDS mooncake_store)

add_custom_command(
    TARGET build_mooncake_store_rust_example
    COMMAND
        ${CMAKE_COMMAND} -E env
            CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}
            MOONCAKE_STORE_LIB_DIR=${CMAKE_CURRENT_BINARY_DIR}/../src
            MOONCAKE_STORE_INCLUDE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/../include
        cargo build --example basic_usage --release
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Building mooncake_store Rust basic_usage example"
    VERBATIM
)
