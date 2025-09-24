file(GLOB CORO_RPC_SOURCES "*.cpp")

find_package(Python3 COMPONENTS Interpreter Development REQUIRED)

if (NOT TARGET pybind11::headers)
    find_package(pybind11 CONFIG QUIET)
    if (NOT pybind11_FOUND)
        include(FetchContent)
        message(STATUS "Fetching pybind11 (v2.13.6) for coro_rpc_connector")
        FetchContent_Declare(pybind11
            GIT_REPOSITORY https://github.com/pybind/pybind11.git
            GIT_TAG v2.13.6
            GIT_SHALLOW TRUE
        )
        FetchContent_MakeAvailable(pybind11)
    endif()
endif()

add_library(coro_rpc_connector OBJECT ${CORO_RPC_SOURCES})

set(_PYBIND11_INC_DIRS "")
if(TARGET pybind11::headers)
    get_target_property(_PYBIND11_INC_DIRS pybind11::headers INTERFACE_INCLUDE_DIRECTORIES)
elseif(TARGET pybind11::pybind11)
    get_target_property(_PYBIND11_INC_DIRS pybind11::pybind11 INTERFACE_INCLUDE_DIRECTORIES)
elseif(TARGET pybind11::pybind11_headers)
    get_target_property(_PYBIND11_INC_DIRS pybind11::pybind11_headers INTERFACE_INCLUDE_DIRECTORIES)
endif()
if(_PYBIND11_INC_DIRS)
    target_include_directories(coro_rpc_connector PRIVATE ${_PYBIND11_INC_DIRS})
endif()

target_link_libraries(coro_rpc_connector 
    PRIVATE 
    JsonCpp::JsonCpp 
    yalantinglibs::yalantinglibs 
    glog::glog 
    pthread 
    ${Python3_LIBRARIES}
)

target_include_directories(coro_rpc_connector 
    PRIVATE 
    ${Python3_INCLUDE_DIRS}
)

# Add pybind11 headers/include paths if available
if(TARGET pybind11::headers)
    target_link_libraries(coro_rpc_connector PRIVATE pybind11::headers)
elseif(TARGET pybind11::pybind11)
    target_link_libraries(coro_rpc_connector PRIVATE pybind11::pybind11)
else()
    # Fallback: try to read include dirs from known pybind11 targets if present
    if(TARGET pybind11::pybind11_headers)
        get_target_property(_PYBIND_INCLUDES pybind11::pybind11_headers INTERFACE_INCLUDE_DIRECTORIES)
        if(_PYBIND_INCLUDES)
            target_include_directories(coro_rpc_connector PRIVATE ${_PYBIND_INCLUDES})
        endif()
    endif()
endif()