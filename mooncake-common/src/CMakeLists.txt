find_package(yaml-cpp REQUIRED)

set(MOONCAKE_COMMON_SOURCES
    default_config.cpp
    environ.cpp
)

add_library(asio_shared SHARED asio_impl.cpp)

target_compile_definitions(asio_shared
    PUBLIC
        ASIO_SEPARATE_COMPILATION
        ASIO_DYN_LINK
)

set_target_properties(asio_shared PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    INSTALL_RPATH "$ORIGIN"
    BUILD_WITH_INSTALL_RPATH TRUE
    OUTPUT_NAME "asio"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/mooncake-common"
)

target_link_libraries(asio_shared PUBLIC
    pthread
    yalantinglibs::yalantinglibs
)

add_library(mooncake_common
    ${MOONCAKE_COMMON_SOURCES}
)

target_include_directories(mooncake_common PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(mooncake_common PUBLIC
    yaml-cpp
    jsoncpp
    yalantinglibs::yalantinglibs
)

if (BUILD_SHARED_LIBS)
  install(TARGETS mooncake_common DESTINATION lib)
endif()

install(TARGETS asio_shared DESTINATION lib)
