project(proto_to_struct)

cmake_minimum_required(VERSION 3.10)

set(CMAKE_CXX_STANDARD 20)

find_package(Protobuf REQUIRED)
include_directories(${PROTOBUF_INCLUDE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

set(SOURCE_FILE proto_to_struct.cpp)

add_executable(proto_to_struct ${SOURCE_FILE})
target_link_libraries(proto_to_struct protobuf::libprotobuf protobuf::libprotoc pthread)
