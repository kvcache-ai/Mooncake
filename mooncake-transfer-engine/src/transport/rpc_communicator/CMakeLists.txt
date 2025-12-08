file(GLOB CORO_RPC_SOURCES "*.cpp")

find_package(Python3 COMPONENTS Interpreter Development REQUIRED)

add_library(rpc_communicator OBJECT ${CORO_RPC_SOURCES})

target_link_libraries(rpc_communicator 
    PRIVATE 
    JsonCpp::JsonCpp 
    yalantinglibs::yalantinglibs 
    glog::glog 
    pthread 
    ${Python3_LIBRARIES}
)

target_include_directories(rpc_communicator 
    PRIVATE 
    ${Python3_INCLUDE_DIRS}
)

if(TARGET pybind11::headers)
    target_link_libraries(rpc_communicator PRIVATE pybind11::headers)
elseif(DEFINED pybind11_INCLUDE_DIR)
    target_include_directories(rpc_communicator PRIVATE ${pybind11_INCLUDE_DIR})
else()
    get_filename_component(_PYBIND11_DIR "${CMAKE_SOURCE_DIR}/extern/pybind11/include" ABSOLUTE)
    if(EXISTS "${_PYBIND11_DIR}/pybind11/pybind11.h")
        target_include_directories(rpc_communicator PRIVATE "${_PYBIND11_DIR}")
    else()
        message(FATAL_ERROR "Could not find pybind11 at ${_PYBIND11_DIR}")
    endif()
endif()