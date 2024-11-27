add_custom_target(build_transfer_engine_rust DEPENDS transfer_engine)
add_custom_command(
    TARGET build_transfer_engine_rust
    COMMAND ${CMAKE_COMMAND} -E env CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR} cargo build --release
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
set_property(TARGET build_transfer_engine_rust PROPERTY EXCLUDE_FROM_ALL FALSE)
