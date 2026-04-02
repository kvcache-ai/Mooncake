if (GLOBAL_CONFIG)
    set(MOONCAKE_RUST_TE_LIB_DIR "${CMAKE_BINARY_DIR}/mooncake-transfer-engine/src")
else()
    set(MOONCAKE_RUST_TE_LIB_DIR "${CMAKE_BINARY_DIR}/src")
endif()

set(MOONCAKE_RUST_TE_HEADER "${CMAKE_CURRENT_SOURCE_DIR}/../include/transfer_engine_c.h")

add_custom_target(build_transfer_engine_rust DEPENDS transfer_engine)
add_custom_command(
    TARGET build_transfer_engine_rust
        COMMAND ${CMAKE_COMMAND} -E env
                        CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}
                        MOONCAKE_TE_LIB_DIR=${MOONCAKE_RUST_TE_LIB_DIR}
                        MOONCAKE_TE_HEADER=${MOONCAKE_RUST_TE_HEADER}
                        cargo build --release
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
set_property(TARGET build_transfer_engine_rust PROPERTY EXCLUDE_FROM_ALL FALSE)
