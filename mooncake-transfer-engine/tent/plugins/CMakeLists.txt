set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_library(tent_plugin INTERFACE)
target_include_directories(tent_plugin
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
        $<INSTALL_INTERFACE:include>
)
target_compile_features(tent_plugin INTERFACE cxx_std_20)
# Transport plugins include TENT runtime headers (transport.h pulls in the RPC
# and yalantinglibs headers), so they must compile exactly like a TENT consumer.
# They link nothing from TENT: the process that loads them provides those
# symbols (see include/tent/transport_plugin.h).
target_include_directories(
  tent_plugin
  INTERFACE $<TARGET_PROPERTY:tent_runtime,INTERFACE_INCLUDE_DIRECTORIES>)
target_compile_definitions(
  tent_plugin
  INTERFACE $<TARGET_PROPERTY:tent_runtime,INTERFACE_COMPILE_DEFINITIONS>)
target_compile_options(
  tent_plugin
  INTERFACE $<TARGET_PROPERTY:tent_runtime,INTERFACE_COMPILE_OPTIONS>)

if (USE_CUDA)
    add_subdirectory(cuda)
endif()

if (USE_HIP)
    add_subdirectory(rocm)
endif()

option(TENT_BUILD_TRANSPORT_PLUGINS "Build the in-tree TENT transport plugins"
       ON)
if(NOT TENT_BUILD_TRANSPORT_PLUGINS
   AND (NOT TENT_XPORT_TCP STREQUAL "static" OR NOT TENT_XPORT_RDMA STREQUAL
                                                "static"))
  message(
    FATAL_ERROR
      "TENT_XPORT_TCP=${TENT_XPORT_TCP} and TENT_XPORT_RDMA=${TENT_XPORT_RDMA} "
      "build transport plugins, which needs TENT_BUILD_TRANSPORT_PLUGINS=ON")
endif()
# A build directory reconfigured back to static drops the TCP/RDMA plugin it
# built before, so its plugin directory does not offer a stale copy.
foreach(name tcp rdma)
  string(TOUPPER ${name} upper)
  if(TENT_XPORT_${upper} STREQUAL "static")
    file(REMOVE ${CMAKE_BINARY_DIR}/tent_plugins/libtent_xport_${name}.so)
  endif()
endforeach()
if(TENT_BUILD_TRANSPORT_PLUGINS)
  add_subdirectory(transport)
endif()
# find_package(tentPlugin): tent_plugin for plugins built out of tree.
add_subdirectory(sdk)
