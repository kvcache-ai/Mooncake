# Transport plugins are MODULE libraries named libtent_xport_<name>.so. They are
# collected in one directory that can be passed to TENT as plugins/dir (or
# MC_TENT_PLUGIN_DIR) and are installed to lib/tent_plugins.
set(TENT_TRANSPORT_PLUGIN_DIR
    ${CMAKE_BINARY_DIR}/tent_plugins
    CACHE INTERNAL "Build-tree directory of the TENT transport plugins")

# tent_add_transport_plugin(<target> <name> <sources>...)
function(tent_add_transport_plugin target name)
  add_library(${target} MODULE ${ARGN})
  target_link_libraries(${target} PRIVATE tent_plugin)
  set_target_properties(
    ${target}
    PROPERTIES PREFIX "lib"
               OUTPUT_NAME "tent_xport_${name}"
               LIBRARY_OUTPUT_DIRECTORY ${TENT_TRANSPORT_PLUGIN_DIR})
  install(TARGETS ${target} LIBRARY DESTINATION lib/tent_plugins)
endfunction()

add_subdirectory(loopback)
