# Build ubshmem allocator and output to build directory

add_custom_target(build_ubshmem_allocator DEPENDS transfer_engine)

get_target_property(INCLUDE_DIRS build_ubshmem_allocator INCLUDE_DIRECTORIES)
string(REPLACE ";" " " INCLUDE_DIRS_STR "${INCLUDE_DIRS}")

if (USE_UBSHMEM)
    add_custom_command(
        TARGET build_ubshmem_allocator
        COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}
        COMMAND bash build.sh ${CMAKE_CURRENT_BINARY_DIR} "${INCLUDE_DIRS_STR}"
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Building ubshmem allocator to ${CMAKE_CURRENT_BINARY_DIR}"
    )
endif()

set_property(TARGET build_ubshmem_allocator PROPERTY EXCLUDE_FROM_ALL FALSE)