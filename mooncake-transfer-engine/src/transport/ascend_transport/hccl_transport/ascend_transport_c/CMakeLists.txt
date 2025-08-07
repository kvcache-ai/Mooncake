file(GLOB ASCEND_TRANSPORT_MEM_SOURCES "*.cpp")

set(CMAKE_POSITION_INDEPENDENT_CODE OFF)

string(REPLACE "-O3" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
string(REPLACE "-O3" "" CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")

add_library(ascend_transport_mem SHARED  ${ASCEND_TRANSPORT_MEM_SOURCES})
set_target_properties(ascend_transport_mem PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(ascend_transport_mem PRIVATE hccl ascendcl glog gflags)

target_compile_options(ascend_transport_mem BEFORE PRIVATE "-std=c++11" "-fstack-protector-strong" "-s" "-Wl,-z,relro" "-Wl,-z,now" "-Wl,-z,noexecstack" "-Wl,--copy-dt-needed-entries" "-DOPEN_BUILD_PROJECT" "-DLOG_CPP" "-D_GLIBCXX_USE_CXX11_ABI=0")
target_link_options(ascend_transport_mem BEFORE PRIVATE "-std=c++11" "-fstack-protector-strong" "-s" "-Wl,-z,relro" "-Wl,-z,now" "-Wl,-z,noexecstack" "-Wl,--copy-dt-needed-entries" "-DOPEN_BUILD_PROJECT" "-DLOG_CPP" "-D_GLIBCXX_USE_CXX11_ABI=0")
