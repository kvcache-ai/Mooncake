# Build nvlink allocator and output to build directory
add_custom_target(build_nvlink_allocator DEPENDS transfer_engine)
add_custom_command(
    TARGET build_nvlink_allocator
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND bash build.sh ${CMAKE_CURRENT_BINARY_DIR}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Building nvlink allocator to ${CMAKE_CURRENT_BINARY_DIR}"
)
set_property(TARGET build_nvlink_allocator PROPERTY EXCLUDE_FROM_ALL FALSE)
