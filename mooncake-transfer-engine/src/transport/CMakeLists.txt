file(GLOB XPORT_SOURCES "*.cpp" "coro_rpc_connector/*.cpp")

# Find Python and pybind11 for coro_rpc_connector
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

add_subdirectory(rdma_transport)

add_library(transport OBJECT ${XPORT_SOURCES} $<TARGET_OBJECTS:rdma_transport>)
target_link_libraries(transport PRIVATE JsonCpp::JsonCpp yalantinglibs::yalantinglibs glog::glog pthread pybind11::module ${Python3_LIBRARIES})

if (USE_TCP)
  add_subdirectory(tcp_transport)
  target_sources(transport PUBLIC $<TARGET_OBJECTS:tcp_transport>)
endif()

if (USE_NVMEOF)
  add_subdirectory(nvmeof_transport)
  target_sources(transport PUBLIC $<TARGET_OBJECTS:nvmeof_transport>)
endif()

if (USE_CXL)
  add_subdirectory(cxl_transport)
  target_sources(transport PUBLIC $<TARGET_OBJECTS:cxl_transport>)
endif()

if (USE_ASCEND OR USE_ASCEND_DIRECT)
  add_subdirectory(ascend_transport)
  target_sources(transport PUBLIC $<TARGET_OBJECTS:ascend_transport>)
endif()

if (USE_MNNVL)
  add_subdirectory(nvlink_transport)
  target_sources(transport PUBLIC $<TARGET_OBJECTS:nvlink_transport>)
endif()

target_include_directories(transport PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../../include)