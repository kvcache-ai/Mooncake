set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/tests)
if(NOT CMAKE_BUILD_TYPE)
        set(CMAKE_BUILD_TYPE "Release")
endif()
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_INCLUDE_CURRENT_DIR ON)
find_package(Threads REQUIRED)
find_package(ZLIB)
if (ZLIB_FOUND)
    add_definitions(-DCINATRA_ENABLE_GZIP)
endif ()
link_libraries(Threads::Threads)

include_directories(include)
include_directories(include/ylt/thirdparty)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcoroutines")
        #-ftree-slp-vectorize with coroutine cause link error. disable it util gcc fix.
        set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fno-tree-slp-vectorize")
endif()

add_executable(coro_http_test
        test_mixed.cpp
        test_coro_http_server.cpp
        test_cinatra.cpp
        test_cinatra_websocket.cpp
        test_http_parse.cpp
        test_http_client_filter.cpp
        main.cpp
        )

add_custom_command(
        TARGET coro_http_test POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_CURRENT_SOURCE_DIR}/openssl_files
        ${CMAKE_BINARY_DIR}/src/coro_http/openssl_files)
add_custom_command(
        TARGET coro_http_test POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_CURRENT_SOURCE_DIR}/openssl_files
        ${CMAKE_BINARY_DIR}/output/openssl_files)

add_definitions(-DINJECT_FOR_HTTP_CLIENT_TEST)
add_definitions(-DINJECT_FOR_HTTP_SEVER_TEST)

add_test(NAME coro_http_test COMMAND coro_http_test)
# target_compile_definitions(easylog_test PRIVATE STRUCT_PACK_ENABLE_UNPORTABLE_TYPE)

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_SYSTEM_NAME MATCHES "Windows") # mingw-w64
    if (YLT_ENABLE_SSL)
        message(STATUS "Use SSL")
        find_package(OpenSSL REQUIRED)
        add_definitions(-DCINATRA_ENABLE_SSL)
        target_link_libraries(coro_http_test PRIVATE ws2_32 mswsock OpenSSL::SSL OpenSSL::Crypto)
    endif ()
    
    if (ZLIB_FOUND)
        target_link_libraries(coro_http_test PRIVATE ws2_32 mswsock ZLIB::ZLIB)
    endif ()
else()
    if (YLT_ENABLE_SSL)
        message(STATUS "Use SSL")
        find_package(OpenSSL REQUIRED)
        add_definitions(-DCINATRA_ENABLE_SSL)
        target_link_libraries(coro_http_test  OpenSSL::SSL OpenSSL::Crypto)
    endif ()
    
    if (ZLIB_FOUND)
        target_link_libraries(coro_http_test ZLIB::ZLIB)
    endif ()
endif()
