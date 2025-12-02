# Build nvlink allocator and output to build directory

add_custom_target(build_nvlink_allocator DEPENDS transfer_engine)

get_target_property(INCLUDE_DIRS build_nvlink_allocator INCLUDE_DIRECTORIES)
string(REPLACE ";" " " INCLUDE_DIRS_STR "${INCLUDE_DIRS}")

if (USE_CUDA)
add_custom_command(
    TARGET build_nvlink_allocator
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND bash build.sh ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND bash build.sh --use-nvcc ${CMAKE_CURRENT_BINARY_DIR} "${INCLUDE_DIRS_STR}"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Building nvlink allocator to ${CMAKE_CURRENT_BINARY_DIR}"
)
endif()

if (USE_HIP)
add_custom_command(
    TARGET build_nvlink_allocator
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND bash build.sh --use-hipcc ${CMAKE_CURRENT_BINARY_DIR} "${INCLUDE_DIRS_STR}"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Building nvlink allocator to ${CMAKE_CURRENT_BINARY_DIR}"
)
endif()

set_property(TARGET build_nvlink_allocator PROPERTY EXCLUDE_FROM_ALL FALSE)
