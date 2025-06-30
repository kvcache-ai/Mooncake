file(GLOB ASCEND_TRANSPORT_MEM_SOURCES "*.cpp")

set(CMAKE_POSITION_INDEPENDENT_CODE OFF)

string(REPLACE "-O2" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
string(REPLACE "-O2" "" CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")

file(GLOB ASCEND_TOOLKIT_ROOT "/usr/local/Ascend/ascend-toolkit/latest/*-linux")
set(ASCEND_LIB_DIR "${ASCEND_TOOLKIT_ROOT}/lib64")
set(ASCEND_INCLUDE_DIR "${ASCEND_TOOLKIT_ROOT}/include")

file(GLOB MPI_INCLUDE_DIR "/usr/include/mpich-*")
file(GLOB MPI_LOCAL_INCLUDE_DIR "/usr/local/mpich-*/include")

link_directories(${ASCEND_LIB_DIR})
include_directories(/usr/local/include
                    /usr/include
                    ${ASCEND_INCLUDE_DIR}
                    ${ASCEND_INCLUDE_DIR}/hccl
                    ${ASCEND_INCLUDE_DIR}/experiment
                    ${ASCEND_INCLUDE_DIR}/experiment/hccl
                    ${ASCEND_INCLUDE_DIR}/experiment/slog/toolchain
                    ${ASCEND_INCLUDE_DIR}/experiment/metadef/common/util/error_manager
                    ${ASCEND_INCLUDE_DIR}/experiment/runtime
                    ${ASCEND_INCLUDE_DIR}/experiment/msprof
                    ${MPI_INCLUDE_DIR}
                    ${MPI_LOCAL_INCLUDE_DIR}
                    /usr/local/Ascend/ascend-toolkit/latest/tools/hccl_test/common/src/
                    )
add_library(ascend_transport_mem SHARED  ${ASCEND_TRANSPORT_MEM_SOURCES})
set_target_properties(ascend_transport_mem PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(ascend_transport_mem PRIVATE hccl ascendcl glog gflags)

target_compile_options(ascend_transport_mem BEFORE PRIVATE "-std=c++11" "-fstack-protector-strong" "-O2" "-s" "-Wl,-z,relro" "-Wl,-z,now" "-Wl,-z,noexecstack" "-Wl,--copy-dt-needed-entries" "-DOPEN_BUILD_PROJECT" "-DLOG_CPP" "-D_GLIBCXX_USE_CXX11_ABI=0")
target_link_options(ascend_transport_mem BEFORE PRIVATE "-std=c++11" "-fstack-protector-strong" "-O2" "-s" "-Wl,-z,relro" "-Wl,-z,now" "-Wl,-z,noexecstack" "-Wl,--copy-dt-needed-entries" "-DOPEN_BUILD_PROJECT" "-DLOG_CPP" "-D_GLIBCXX_USE_CXX11_ABI=0")




