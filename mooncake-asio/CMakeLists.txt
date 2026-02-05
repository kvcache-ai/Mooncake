# Build asio as a shared library to avoid ODR violations
# when multiple shared libraries use asio

# Try to find ASIO using find_package first
find_package(asio QUIET)

if(asio_FOUND)
    message(STATUS "Found ASIO via find_package")
    set(ASIO_INCLUDE_DIR ${asio_INCLUDE_DIR})
else()
    # Fallback to find_path if find_package fails
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
)

target_link_libraries(asio_shared PUBLIC pthread)

install(TARGETS asio_shared DESTINATION lib)
