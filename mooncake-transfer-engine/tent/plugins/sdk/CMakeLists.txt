# The tentPlugin CMake package, for transport plugins built out of tree:
#
# ~~~
#   find_package(tentPlugin REQUIRED)
#   target_link_libraries(<plugin> PRIVATE tent::plugin)
# ~~~
#
# tent::plugin is what tent_plugin is in this tree. It carries the installed
# headers and the compile definitions of this build, which a plugin needs to see
# the same types as the core that loads it, and links nothing. The package
# version is the plugin ABI version. It is installed with the headers and the
# loopback sample's sources as the component tent_plugin_sdk.
include(CMakePackageConfigHelpers)

set(tent_plugin_abi_header
    ${CMAKE_CURRENT_SOURCE_DIR}/../../include/tent/transport_plugin.h)
set_property(
  DIRECTORY
  APPEND
  PROPERTY CMAKE_CONFIGURE_DEPENDS ${tent_plugin_abi_header})
file(STRINGS ${tent_plugin_abi_header} tent_plugin_abi
     REGEX "^#define TENT_TRANSPORT_PLUGIN_ABI_VERSION ")
string(REGEX MATCH "[0-9]+" tent_plugin_abi "${tent_plugin_abi}")

# The definitions an in-tree plugin compiles with: those of this directory (set
# directory-wide above it), the -D flags in CMAKE_CXX_FLAGS and the runtime's
# usage requirements.
get_directory_property(tent_plugin_definitions COMPILE_DEFINITIONS)
set(TENT_PLUGIN_OPTIONS)
separate_arguments(tent_plugin_flags UNIX_COMMAND "${CMAKE_CXX_FLAGS}")
foreach(flag ${tent_plugin_flags})
  if(flag MATCHES "^-D(.+)$")
    list(APPEND tent_plugin_definitions "${CMAKE_MATCH_1}")
  elseif(flag STREQUAL "-fcoroutines")
    set(TENT_PLUGIN_OPTIONS -fcoroutines)
  endif()
endforeach()
set(TENT_PLUGIN_DEFINITIONS)
foreach(definition ${tent_plugin_definitions})
  if(NOT definition MATCHES "\\$<")
    # Literal commas and '>' would end the generator expressions below.
    string(REPLACE "," "$<COMMA>" definition "${definition}")
    string(REPLACE ">" "$<ANGLE-R>" definition "${definition}")
  endif()
  list(APPEND TENT_PLUGIN_DEFINITIONS "${definition}")
endforeach()
# The runtime's include directories, without those in this source tree, this
# build tree and the fetched yalantinglibs: their headers are installed.
set(tent_plugin_trees)
foreach(tree ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR}
             ${yalantinglibs_SOURCE_DIR})
  string(REGEX REPLACE "([][+.*()^$?|\\\\])" "\\\\\\1" tree "${tree}")
  string(REPLACE "," "$<COMMA>" tree "${tree}")
  string(REPLACE ">" "$<ANGLE-R>" tree "${tree}")
  list(APPEND tent_plugin_trees "^${tree}(/|$)")
endforeach()
string(JOIN "|" TENT_PLUGIN_TREES ${tent_plugin_trees})

configure_package_config_file(
  tentPluginConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/tentPluginConfig.cmake.in
  INSTALL_DESTINATION lib/cmake/tentPlugin)
file(
  GENERATE
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/tentPluginConfig.cmake
  INPUT ${CMAKE_CURRENT_BINARY_DIR}/tentPluginConfig.cmake.in)
write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/tentPluginConfigVersion.cmake
  VERSION ${tent_plugin_abi}
  COMPATIBILITY ExactVersion)
install(
  FILES ${CMAKE_CURRENT_BINARY_DIR}/tentPluginConfig.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/tentPluginConfigVersion.cmake
  DESTINATION lib/cmake/tentPlugin
  COMPONENT tent_plugin_sdk)
# The loopback sample's sources, which the SDK example builds
# (tentPlugin_EXAMPLE_DIR).
set(tent_plugin_loopback ${CMAKE_CURRENT_SOURCE_DIR}/../transport/loopback)
install(
  FILES ${tent_plugin_loopback}/loopback_transport.h
        ${tent_plugin_loopback}/loopback_transport.cpp
        ${tent_plugin_loopback}/plugin_entry.cpp
  DESTINATION share/tent/examples/loopback
  COMPONENT tent_plugin_sdk)
