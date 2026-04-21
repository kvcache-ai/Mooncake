
if("${yaLanTingLibs_SOURCE_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/examples/coro_http)
else()
  # else find installed yalantinglibs
    cmake_minimum_required(VERSION 3.15)
    project(file_transfer)
    if(NOT CMAKE_BUILD_TYPE)
        set(CMAKE_BUILD_TYPE "Release")
    endif()
    set(CMAKE_CXX_STANDARD 20)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_INCLUDE_CURRENT_DIR ON)
    find_package(Threads REQUIRED)
    link_libraries(Threads::Threads)
  # if you have install ylt  
    find_package(yalantinglibs REQUIRED)
    link_libraries(yalantinglibs::yalantinglibs)
  # else 
  # include_directories(include)
  # include_directories(include/ylt/thirdparty)

  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcoroutines")
    #-ftree-slp-vectorize with coroutine cause link error. disable it util gcc fix.
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fno-tree-slp-vectorize")
  endif()
endif()

add_executable(coro_http_example example.cpp)
add_executable(coro_http_load_balancer load_balancer.cpp)
add_executable(coro_chat_room chat_room.cpp)

if(YLT_ENABLE_NTLS)
    message(STATUS "Building NTLS examples with Tongsuo support")
    add_executable(ntls_http_server ntls_http_server.cpp)
    add_executable(ntls_http_client ntls_http_client.cpp)
    target_compile_definitions(ntls_http_server PRIVATE YLT_ENABLE_NTLS)
    target_compile_definitions(ntls_http_client PRIVATE YLT_ENABLE_NTLS)
endif()
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_SYSTEM_NAME MATCHES "Windows") # mingw-w64
    target_link_libraries(coro_http_example PRIVATE ws2_32 mswsock)
    target_link_libraries(coro_http_load_balancer PRIVATE ws2_32 mswsock)
    target_link_libraries(coro_chat_room PRIVATE ws2_32 mswsock)
    if(YLT_ENABLE_NTLS)
        target_link_libraries(ntls_http_server PRIVATE ws2_32 mswsock)
        target_link_libraries(ntls_http_client PRIVATE ws2_32 mswsock)
    endif()
endif()

option(ENABLE_pybind11 "Enable pybind11 " OFF)
if(ENABLE_pybind11)
  find_package(pybind11 REQUIRED)

  pybind11_add_module(py_example py_example/py_example.cpp)
  set_target_properties(py_example PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/py_example
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/py_example
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/py_example
  )
endif()
