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
    set(_PYBIND11_DIR "")
    if(EXISTS "${CMAKE_SOURCE_DIR}/extern/pybind11/include/pybind11/pybind11.h")
        set(_PYBIND11_DIR "${CMAKE_SOURCE_DIR}/extern/pybind11/include")
    else()
        get_filename_component(_PROJECT_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../../../" ABSOLUTE)
        set(_PYBIND11_DIR "${_PROJECT_ROOT}/extern/pybind11/include")
        if(NOT EXISTS "${_PYBIND11_DIR}/pybind11/pybind11.h")
            set(_PYBIND11_DIR "")
        endif()
    endif()
    if(_PYBIND11_DIR)
        target_include_directories(rpc_communicator PRIVATE "${_PYBIND11_DIR}")
    else()
        message(FATAL_ERROR "Could not find pybind11")
    endif()
endif()