# Find Python and pybind11 for the binding code
find_package(Python3 COMPONENTS Interpreter Development REQUIRED)
find_package(pybind11 QUIET)
if(NOT pybind11_FOUND)
    execute_process(
        COMMAND ${Python3_EXECUTABLE} -m pybind11 --cmakedir
        OUTPUT_VARIABLE pybind11_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE pybind11_RESULT
    )
    if(pybind11_RESULT EQUAL 0)
        find_package(pybind11 REQUIRED PATHS ${pybind11_DIR})
    else()
        message(FATAL_ERROR "pybind11 not found. Please install with: pip install pybind11")
    endif()
endif()

# Create object library for coro_rpc_connector
set(CORO_RPC_SOURCES
    cororpc_interface.cpp
    cororpc_communicator.cpp
)

add_library(coro_rpc_connector OBJECT ${CORO_RPC_SOURCES})

target_compile_features(coro_rpc_connector PRIVATE cxx_std_20)
target_compile_options(coro_rpc_connector PRIVATE -O3 -Wall)

target_include_directories(coro_rpc_connector PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../include
    ${Python3_INCLUDE_DIRS}
)

target_link_libraries(coro_rpc_connector PRIVATE 
    yalantinglibs::yalantinglibs
    pthread
)