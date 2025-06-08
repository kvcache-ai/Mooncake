# Currently you have to manually execute makefile in the src subdirectory.
add_custom_target(build_nvlink_hook DEPENDS transfer_engine)
add_custom_command(
    TARGET build_nvlink_hook
    COMMAND bash build.sh
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
set_property(TARGET build_nvlink_hook PROPERTY EXCLUDE_FROM_ALL FALSE)
