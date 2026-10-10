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
if(TENT_BUILD_TRANSPORT_PLUGINS)
  add_subdirectory(transport)
endif()
