set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_library(tent_plugin INTERFACE)
target_include_directories(tent_plugin
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
        $<INSTALL_INTERFACE:include>
)
target_compile_features(tent_plugin INTERFACE cxx_std_20)

if (USE_CUDA)
    add_subdirectory(cuda)
endif()

if (USE_HIP)
    add_subdirectory(rocm)
endif()

# libtent_xport_<name>.so for a transport built as a plugin (TENT_XPORT_<NAME>,
# src/CMakeLists.txt): plugin_entry.cpp and the sources of the transport's
# static library, compiled like that library (include directories as SYSTEM: the
# third-party ones among them are system headers for TENT). It links no TENT
# library, not even that static one, which also holds TENT objects; the TENT of
# the process that loads it provides them.
function(tent_xport_plugin name library)
  set(target tent_xport_${name}_plugin)
  get_target_property(sources ${library} SOURCES)
  get_target_property(source_dir ${library} SOURCE_DIR)
  list(
    TRANSFORM sources
    PREPEND ${source_dir}/
    REGEX "^[^/]")
  add_library(${target} MODULE transport/${name}/plugin_entry.cpp ${sources})
  target_link_libraries(${target} PRIVATE tent_plugin)
  target_include_directories(
    ${target} SYSTEM PRIVATE $<TARGET_PROPERTY:${library},INCLUDE_DIRECTORIES>)
  target_compile_definitions(
    ${target} PRIVATE $<TARGET_PROPERTY:${library},COMPILE_DEFINITIONS>)
  set_target_properties(
    ${target}
    PROPERTIES OUTPUT_NAME tent_xport_${name} LIBRARY_OUTPUT_DIRECTORY
                                              ${CMAKE_BINARY_DIR}/tent_plugins)
  install(TARGETS ${target} LIBRARY DESTINATION lib/tent_plugins)
endfunction()

if(TENT_XPORT_TCP STREQUAL "plugin")
  tent_xport_plugin(tcp tent_xport_tcp)
endif()
if(TENT_XPORT_RDMA STREQUAL "plugin")
  tent_xport_plugin(rdma tent_xport_rdma)
  # Linked so that the plugin's references to them carry symbol versions.
  target_link_libraries(tent_xport_rdma_plugin PRIVATE ${IBVERBS_LIB} numa)
endif()
