file(GLOB SOURCES "*.cpp")
set(PYTHON_EXECUTABLE "python3")
execute_process(
    COMMAND ${PYTHON_EXECUTABLE} -c "import sys; print(sys.path[-1])"
    OUTPUT_VARIABLE PYTHON_SYS_PATH
)
string(STRIP ${PYTHON_SYS_PATH} PYTHON_SYS_PATH)

set(PYBIND11_FINDPYTHON ON)
find_package(pybind11 CONFIG REQUIRED) 
pybind11_add_module(mooncake_vllm_adaptor ${SOURCES} vllm/vllm_adaptor.cpp)
target_link_libraries(mooncake_vllm_adaptor PUBLIC transfer_engine glog gflags)
message("${PYTHON_SYS_PATH}")
install(TARGETS mooncake_vllm_adaptor DESTINATION ${PYTHON_SYS_PATH}/)
