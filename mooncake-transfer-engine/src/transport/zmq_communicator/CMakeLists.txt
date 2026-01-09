cmake_minimum_required(VERSION 3.14)

project(zmq_communicator)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find dependencies
find_package(glog REQUIRED)
find_package(Python3 COMPONENTS Interpreter Development REQUIRED)

# Set up paths for external libraries (relative to zmq_communicator directory)
set(MOONCAKE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../../../")
set(PYBIND11_INCLUDE "${MOONCAKE_ROOT}/extern/pybind11/include")
set(YALANTINGLIBS_INCLUDE "${MOONCAKE_ROOT}/extern/yalantinglibs/include")
set(ASYNC_SIMPLE_INCLUDE "${MOONCAKE_ROOT}/extern/async_simple")
set(MOONCAKE_COMMON_INCLUDE "${MOONCAKE_ROOT}/mooncake-common/include")
set(TRANSFER_ENGINE_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/../../..")

include_directories(
    ${PYBIND11_INCLUDE}
    ${YALANTINGLIBS_INCLUDE}
    ${ASYNC_SIMPLE_INCLUDE}
    ${MOONCAKE_COMMON_INCLUDE}
    ${TRANSFER_ENGINE_INCLUDE}
    ${Python3_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# ZMQ Communicator library
add_library(zmq_communicator
    message_codec.cpp
    req_rep_pattern.cpp
    pub_sub_pattern.cpp
    push_pull_pattern.cpp
    pair_pattern.cpp
    zmq_communicator.cpp
    zmq_interface.cpp
)

target_include_directories(zmq_communicator
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/../../..  # For includes like "transport/..."
)

target_link_libraries(zmq_communicator
    PUBLIC
        glog::glog
        ${Python3_LIBRARIES}
)

# Test executable
add_executable(zmq_test
    test_zmq.cpp
)

target_link_libraries(zmq_test
    PRIVATE
        zmq_communicator
        glog::glog
)

# Install targets
install(TARGETS zmq_communicator
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)

install(FILES
    zmq_types.h
    message_codec.h
    base_pattern.h
    req_rep_pattern.h
    pub_sub_pattern.h
    push_pull_pattern.h
    pair_pattern.h
    zmq_communicator.h
    zmq_interface.h
    DESTINATION include/mooncake/zmq_communicator
)

