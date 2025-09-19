file(GLOB CORO_RPC_SOURCES "*.cpp")

# Find Python for pybind11 support
find_package(Python3 COMPONENTS Interpreter Development REQUIRED)
find_package(pybind11 REQUIRED)

add_library(coro_rpc_connector OBJECT ${CORO_RPC_SOURCES})

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

# Add pybind11 headers if available
if(TARGET pybind11::headers)
    target_link_libraries(coro_rpc_connector PRIVATE pybind11::headers)
endif()