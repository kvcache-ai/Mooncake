file(GLOB SOURCES "*.cpp")
set(PYTHON_EXECUTABLE "python3")
execute_process(
    COMMAND ${PYTHON_EXECUTABLE} -c "import sys; print(sys.path[-1])"
    OUTPUT_VARIABLE PYTHON_SYS_PATH
)
string(STRIP ${PYTHON_SYS_PATH} PYTHON_SYS_PATH)

include_directories("../mooncake-store/include")
include_directories("../mooncake-store/include/cachelib_memory_allocator")

include_directories("../mooncake-store/include/cachelib_memory_allocator/include")
include_directories("../mooncake-store/include/cachelib_memory_allocator/fake_include")


set(PYBIND11_FINDPYTHON ON)
find_package(pybind11 CONFIG REQUIRED) 
pybind11_add_module(mooncake_vllm_adaptor ${SOURCES} ${CACHE_ALLOCATOR_SOURCES}
    vllm/vllm_adaptor.cpp 
    vllm/distributed_object_store.cpp
)

set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
 set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
 
 set_target_properties(mooncake_vllm_adaptor PROPERTIES
     INSTALL_RPATH "$ORIGIN/lib_so"
     BUILD_RPATH "$ORIGIN/lib_so"
     SKIP_BUILD_RPATH FALSE
 )
 

target_link_libraries(mooncake_vllm_adaptor PUBLIC 
    transfer_engine 
    glog 
    gflags 
    mooncake_store 
    cachelib_memory_allocator
)
message("${PYTHON_SYS_PATH}")
install(TARGETS mooncake_vllm_adaptor DESTINATION ${PYTHON_SYS_PATH}/)

pybind11_add_module(mooncake_sglang_adaptor ${SOURCES} ${CACHE_ALLOCATOR_SOURCES}
    sglang/sglang_adaptor.cpp 
)
target_link_libraries(mooncake_sglang_adaptor PUBLIC 
    transfer_engine 
    glog 
    gflags 
)
message("${PYTHON_SYS_PATH}")
install(TARGETS mooncake_sglang_adaptor DESTINATION ${PYTHON_SYS_PATH}/)
