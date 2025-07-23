find_package(yaml-cpp REQUIRED)

set(MOONCAKE_COMMON_SOURCES
    default_config.cpp
)
add_library(mooncake_common
    ${MOONCAKE_COMMON_SOURCES}
)

target_link_libraries(mooncake_common PUBLIC
    yaml-cpp
    jsoncpp
)