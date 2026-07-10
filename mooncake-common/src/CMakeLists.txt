find_package(yaml-cpp REQUIRED)

find_package(asio QUIET)

if(asio_FOUND)
    message(STATUS "Found ASIO via find_package")
    set(ASIO_INCLUDE_DIR ${asio_INCLUDE_DIR})
else()
    find_path(ASIO_INCLUDE_DIR
        NAMES asio.hpp
        PATHS
            /usr/local/include
            /usr/include
            ${CMAKE_INSTALL_PREFIX}/include
        DOC "Path to ASIO headers"
    )

    if(NOT ASIO_INCLUDE_DIR)
        message(FATAL_ERROR "ASIO not found. Please install ASIO or set ASIO_INCLUDE_DIR manually.")
    endif()

    message(STATUS "Found ASIO at: ${ASIO_INCLUDE_DIR}")
endif()

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

target_include_directories(asio_shared
    PUBLIC
        ${ASIO_INCLUDE_DIR}
)

set_target_properties(asio_shared PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    INSTALL_RPATH "$ORIGIN"
    BUILD_WITH_INSTALL_RPATH TRUE
    OUTPUT_NAME "asio"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/mooncake-common"
)

target_link_libraries(asio_shared PUBLIC pthread)

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
)

if (BUILD_SHARED_LIBS)
  install(TARGETS mooncake_common DESTINATION lib)
endif()

install(TARGETS asio_shared DESTINATION lib)
