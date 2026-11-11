find_package(yaml-cpp REQUIRED)

set(MOONCAKE_COMMON_SOURCES
    default_config.cpp
    environ.cpp
)
add_library(mooncake_common
    ${MOONCAKE_COMMON_SOURCES}
)

# Internal headers under src/ (e.g. environ.h) are only visible to in-tree
# targets and are not part of the installed interface.
target_include_directories(
  mooncake_common
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
         $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
         $<INSTALL_INTERFACE:include>)

target_link_libraries(mooncake_common PUBLIC
    yaml-cpp
    jsoncpp
)

if (BUILD_SHARED_LIBS)
  set_target_properties(mooncake_common PROPERTIES INSTALL_RPATH "$ORIGIN")
  install(TARGETS mooncake_common DESTINATION lib)
endif()
