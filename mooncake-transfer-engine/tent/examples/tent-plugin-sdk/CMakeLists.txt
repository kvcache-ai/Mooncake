# The loopback sample plugin built against an installed TENT (see README.md).
cmake_minimum_required(VERSION 3.16)
project(tent_plugin_sdk_example LANGUAGES CXX)

find_package(tentPlugin 1 REQUIRED)

# The sample's sources: the TENT source tree's while this directory is in it,
# otherwise the SDK's.
set(src ${CMAKE_CURRENT_SOURCE_DIR}/../../plugins/transport/loopback)
if(NOT EXISTS ${src}/plugin_entry.cpp)
  set(src ${tentPlugin_EXAMPLE_DIR})
endif()
add_library(tent_xport_loopback MODULE ${src}/loopback_transport.cpp
                                       ${src}/plugin_entry.cpp)
target_link_libraries(tent_xport_loopback PRIVATE tent::plugin)
set_target_properties(tent_xport_loopback PROPERTIES PREFIX "lib")
