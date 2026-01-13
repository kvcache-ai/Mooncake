# ZMQ Communicator library as static library
set(ZMQ_COMM_SOURCES
    message_codec.cpp
    patterns.cpp
    zmq_communicator.cpp
    zmq_interface.cpp
)

add_library(zmq_communicator_lib STATIC ${ZMQ_COMM_SOURCES})

# Find Python for zmq_interface.cpp
find_package(Python3 COMPONENTS Interpreter Development REQUIRED)

# Include pybind11 for zmq_interface.cpp
target_include_directories(zmq_communicator_lib
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/../../..
        ${CMAKE_SOURCE_DIR}/extern/pybind11/include
        ${Python3_INCLUDE_DIRS}
)

target_link_libraries(zmq_communicator_lib
    PUBLIC
        glog::glog
        yalantinglibs::yalantinglibs
)

# Tests are located in mooncake-transfer-engine/tests/
# See tests/zmq_communicator_test.cpp

