cmake_minimum_required(VERSION 3.20)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

file(GLOB METADATA_SOURCES "*.cpp")
add_library(metadata STATIC ${METADATA_SOURCES})