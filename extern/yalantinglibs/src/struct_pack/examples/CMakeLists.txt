if("${yaLanTingLibs_SOURCE_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}")
  # If this is a subproject in ylt
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/examples)
else()
  # else find installed yalantinglibs
    cmake_minimum_required(VERSION 3.15)
    project(coro_rpc_examples)
    if(NOT CMAKE_BUILD_TYPE)
        set(CMAKE_BUILD_TYPE "Release")
    endif()
    set(CMAKE_CXX_STANDARD 20)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_INCLUDE_CURRENT_DIR ON)
  # if you have install ylt  
    find_package(yalantinglibs REQUIRED)
    link_libraries(yalantinglibs::yalantinglibs)
  # else 
  # include_directories(include)
  # include_directories(include/ylt/thirdparty)
endif()
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")
add_executable(struct_pack_example basic_usage.cpp non_aggregated_type.cpp serialize_config.cpp user_defined_serialization.cpp derived_class.cpp main.cpp)