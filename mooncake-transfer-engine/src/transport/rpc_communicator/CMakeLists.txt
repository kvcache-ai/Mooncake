file(GLOB CORO_RPC_SOURCES "*.cpp")

find_package(Python3 COMPONENTS Interpreter Development.Module REQUIRED)

add_library(rpc_communicator OBJECT ${CORO_RPC_SOURCES})

target_link_libraries(rpc_communicator
    PRIVATE
    Mooncake::JsonCpp
    Mooncake::Yalantinglibs
    Mooncake::Glog
    Mooncake::Pybind11
    pthread
)

target_include_directories(rpc_communicator
    PRIVATE
    ${Python3_INCLUDE_DIRS}
)
