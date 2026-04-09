# ZMQ Communicator library as object library
# Note: zmq_interface.cpp is excluded from this library as it's only used in Python bindings
set(ZMQ_COMM_SOURCES message_codec.cpp patterns.cpp zmq_communicator.cpp)

add_library(zmq_communicator_lib OBJECT ${ZMQ_COMM_SOURCES})

target_include_directories(zmq_communicator_lib PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../include/zmq_communicator
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
)

# Tests are located in mooncake-transfer-engine/tests/
# See tests/zmq_communicator_test.cpp
