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

# Add pybind11 headers/include paths
# Try to link to pybind11 targets first (this automatically adds include directories)
# Note: pybind11 should be added via add_subdirectory in the root CMakeLists.txt
if(TARGET pybind11::headers)
    target_link_libraries(rpc_communicator PRIVATE pybind11::headers)
elseif(TARGET pybind11::pybind11)
    target_link_libraries(rpc_communicator PRIVATE pybind11::pybind11)
elseif(TARGET pybind11::pybind11_headers)
    target_link_libraries(rpc_communicator PRIVATE pybind11::pybind11_headers)
else()
    # Fallback: try to get include dirs from pybind11_INCLUDE_DIR variable
    # This is set by pybind11's CMakeLists.txt when using add_subdirectory
    if(DEFINED pybind11_INCLUDE_DIR)
        target_include_directories(rpc_communicator PRIVATE ${pybind11_INCLUDE_DIR})
    elseif(DEFINED PYBIND11_INCLUDE_DIR)
        target_include_directories(rpc_communicator PRIVATE ${PYBIND11_INCLUDE_DIR})
    else()
        # Last resort: use relative path from project root
        # Try multiple possible locations
        get_filename_component(_PROJECT_ROOT "${CMAKE_SOURCE_DIR}" ABSOLUTE)
        set(_PYBIND11_PATHS
            "${_PROJECT_ROOT}/extern/pybind11/include"
            "${CMAKE_CURRENT_SOURCE_DIR}/../../../../extern/pybind11/include"
            "${CMAKE_CURRENT_SOURCE_DIR}/../../../extern/pybind11/include"
        )
        
        set(_PYBIND11_FOUND FALSE)
        foreach(_PATH ${_PYBIND11_PATHS})
            if(EXISTS "${_PATH}/pybind11/pybind11.h")
                target_include_directories(rpc_communicator PRIVATE "${_PATH}")
                set(_PYBIND11_FOUND TRUE)
                break()
            endif()
        endforeach()
        
        if(NOT _PYBIND11_FOUND)
            message(FATAL_ERROR 
                "Could not find pybind11 headers. Please ensure pybind11 is configured.\n"
                "Searched in:\n"
                "  ${_PYBIND11_PATHS}\n"
                "Make sure extern/pybind11 exists and contains the pybind11 headers.")
        endif()
    endif()
endif()