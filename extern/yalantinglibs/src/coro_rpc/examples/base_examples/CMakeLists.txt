
if("${yaLanTingLibs_SOURCE_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}")
  # if is the subproject of yalantinglibs
  # do nothing
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
  
  # When using coro_rpc_client to send request, only remote function declarations are required.
  # In the examples, RPC function declaration and definition are divided.
  # However, clang + ld(gold linker) + '-g' will report 'undefined reference to xxx'.
  # So you can use lld when compiler is clang by this code:
  # if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  # add_link_options(-fuse-ld=lld)    
  # endif()

  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcoroutines")
    #-ftree-slp-vectorize with coroutine cause link error. disable it util gcc fix.
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fno-tree-slp-vectorize")
  endif()
endif()

option(ENABLE_go "Enable go " OFF)
if(ENABLE_go)
  add_library(coro_rpc coro_rpc_lib/coro_rpc.cpp)
  set_target_properties(coro_rpc PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/coro_rpc_lib"
  )
endif()

option(ENABLE_rpc_pybind11 "Enable pybind11 " OFF)
if(ENABLE_rpc_pybind11)
  message(STATUS "enable rpc pybind")
  find_package(pybind11 REQUIRED)

  pybind11_add_module(py_coro_rpc py_example/py_rpc_example.cpp)
  set_target_properties(py_coro_rpc PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/py_example
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/py_example
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/py_example
  )

  pybind11_add_module(zero_copy py_example/zero_copy.cpp)
  set_target_properties(zero_copy PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/py_example
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/py_example
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/py_example
  )
endif()

add_executable(coro_rpc_example_load_balancer load_balancer.cpp)
add_executable(coro_rpc_example_client_pool client_pool.cpp)
add_executable(coro_rpc_example_client_pools client_pools.cpp)
add_executable(coro_rpc_example_client client.cpp)
add_executable(coro_rpc_example_concurrent_clients concurrent_clients.cpp)
add_executable(coro_rpc_example_server server.cpp rpc_service.cpp)

if(YLT_ENABLE_NTLS)
    message(STATUS "Building NTLS examples with Tongsuo support")
    add_executable(ntls_server ntls_server.cpp)
    add_executable(ntls_client ntls_client.cpp)
    target_compile_definitions(ntls_server PRIVATE YLT_ENABLE_NTLS)
    target_compile_definitions(ntls_client PRIVATE YLT_ENABLE_NTLS)
endif()
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_SYSTEM_NAME MATCHES "Windows") # mingw-w64
  target_link_libraries(coro_rpc_example_load_balancer PRIVATE ws2_32 mswsock)
  target_link_libraries(coro_rpc_example_client_pool PRIVATE ws2_32 mswsock)
  target_link_libraries(coro_rpc_example_client_pools PRIVATE ws2_32 mswsock)
  target_link_libraries(coro_rpc_example_client PRIVATE ws2_32 mswsock)
  target_link_libraries(coro_rpc_example_concurrent_clients PRIVATE ws2_32 mswsock)
  target_link_libraries(coro_rpc_example_server PRIVATE ws2_32 mswsock)
  if(YLT_ENABLE_NTLS)
        target_link_libraries(ntls_server PRIVATE ws2_32 mswsock)
        target_link_libraries(ntls_client PRIVATE ws2_32 mswsock)
  endif()
endif()

