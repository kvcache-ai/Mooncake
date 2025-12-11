# Build nvlink allocator and output to build directory

add_custom_target(build_nvlink_allocator DEPENDS transfer_engine)

get_target_property(INCLUDE_DIRS build_nvlink_allocator INCLUDE_DIRECTORIES)
string(REPLACE ";" " " INCLUDE_DIRS_STR "${INCLUDE_DIRS}")

set(_extra_build_opts "")
if (USE_HIP)
    set(_extra_build_opts "--use-hipcc")
elseif (USE_MUSA)
    set(_extra_build_opts "--use-mcc")
endif()

if (USE_CUDA OR USE_HIP OR USE_MUSA)
    add_custom_command(
        TARGET build_nvlink_allocator
        COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}
        COMMAND bash build.sh ${_extra_build_opts} ${CMAKE_CURRENT_BINARY_DIR} "${INCLUDE_DIRS_STR}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Building nvlink allocator to ${CMAKE_CURRENT_BINARY_DIR}"
    )
endif()

set_property(TARGET build_nvlink_allocator PROPERTY EXCLUDE_FROM_ALL FALSE)
