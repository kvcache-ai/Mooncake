# The core's RDMA transport sources (the same glob as the static
# tent_xport_rdma) compiled as a plugin that claims the built-in "rdma" slot.
file(GLOB RDMA_PLUGIN_SOURCES
     ${CMAKE_CURRENT_SOURCE_DIR}/../../../src/transport/rdma/*.cpp)
tent_add_transport_plugin(tent_xport_rdma_plugin rdma ${RDMA_PLUGIN_SOURCES}
                          plugin_entry.cpp)
target_include_directories(tent_xport_rdma_plugin PRIVATE ${IBVERBS_INCLUDE})
# Link the system verbs and NUMA libraries (never TENT) so that the plugin's
# references to them carry symbol versions. An unversioned reference binds to
# the oldest version, and both libraries keep an older ABI under the same name
# for functions this code calls directly (ibv_modify_qp@IBVERBS_1.0,
# numa_node_to_cpus@libnuma_1.1). The host has already loaded both.
target_link_libraries(tent_xport_rdma_plugin PRIVATE ${IBVERBS_LIB} numa)
add_custom_command(
  TARGET tent_xport_rdma_plugin
  POST_BUILD
  COMMAND
    ${CMAKE_COMMAND} -DOBJDUMP=${CMAKE_OBJDUMP}
    -DPLUGIN=$<TARGET_FILE:tent_xport_rdma_plugin> -P
    ${CMAKE_CURRENT_SOURCE_DIR}/check_symbol_versions.cmake
  VERBATIM)
# A changed check relinks the plugin, which runs the check again.
set_property(
  TARGET tent_xport_rdma_plugin
  APPEND
  PROPERTY LINK_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/check_symbol_versions.cmake)
