find_package(yaml-cpp REQUIRED)

set(MOONCAKE_COMMON_SOURCES
    default_config.cpp
    environ.cpp
)
add_library(mooncake_common
    ${MOONCAKE_COMMON_SOURCES}
)

target_link_libraries(mooncake_common PUBLIC
    yaml-cpp
    jsoncpp
)

if (BUILD_SHARED_LIBS)
  install(TARGETS mooncake_common DESTINATION lib)
endif()
