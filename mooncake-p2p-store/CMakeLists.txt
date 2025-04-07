# Currently you have to manually execute makefile in the src subdirectory.
add_custom_target(build_p2p_store DEPENDS transfer_engine)
add_custom_command(
    TARGET build_p2p_store
    COMMAND bash build.sh ${CMAKE_CURRENT_BINARY_DIR} ${USE_ETCD} ${USE_REDIS} ${USE_HTTP} ${USE_ETCD_LEGACY} ${CMAKE_BINARY_DIR}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
set_property(TARGET build_p2p_store PROPERTY EXCLUDE_FROM_ALL FALSE)
