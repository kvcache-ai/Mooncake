set(CMAKE_C_STANDARD 99)
set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -Wall -Wextra -Wno-unused-parameter -fPIC")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -Wall -Wextra -Wno-unused-parameter -fPIC")

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcoroutines")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fno-tree-slp-vectorize")
endif()

set(CMAKE_C_FLAGS_RELEASE "-O3")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

set(CMAKE_C_FLAGS_DEBUG "-O0")
set(CMAKE_CXX_FLAGS_DEBUG "-O0")

if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror=thread-safety")
endif()

option(ENABLE_ASAN "enable address sanitizer" OFF)

if (ENABLE_ASAN)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=leak")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=leak")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
endif()

# keep debuginfo by default
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "RelWithDebInfo")
endif()

message(CMAKE_BUILD_TYPE ": ${CMAKE_BUILD_TYPE}")

# Necessary if you are using Alibaba Cloud eRDMA
add_definitions(-DCONFIG_ERDMA)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(ENABLE_SCCACHE "Whether to open sccache" OFF)
if (ENABLE_SCCACHE)
  find_program(SCCACHE sccache REQUIRED)
endif()
if(SCCACHE AND ENABLE_SCCACHE)
  message(STATUS "Building with SCCACHE enabled")
  set(CMAKE_C_COMPILER_LAUNCHER ${SCCACHE})
  set(CMAKE_CXX_COMPILER_LAUNCHER ${SCCACHE})
endif()

add_compile_definitions(GLOG_USE_GLOG_EXPORT)
add_compile_options(-fno-tree-slp-vectorize)

option(BUILD_EXAMPLES "Build examples" ON)

option(BUILD_UNIT_TESTS "Build unit tests" ON)
option(USE_CUDA "option for enabling gpu features for NVIDIA GPU" OFF)
option(USE_MUSA "option for enabling gpu features for MTHREADS GPU" OFF)
option(USE_HIP "option for enabling gpu features for AMD GPU" OFF)
option(USE_NVMEOF "option for using NVMe over Fabric" OFF)
option(USE_TCP "option for using TCP transport" ON)
option(USE_BAREX "option for using accl-barex transport" OFF)
option(USE_ASCEND "option for using npu with HCCL" OFF)
option(USE_ASCEND_DIRECT "option for using ascend npu with adxl engine" OFF)
option(USE_UBSHMEM "option for using ascend npu with shmem" OFF)
option(USE_ASCEND_HETEROGENEOUS "option for transferring between ascend npu and gpu" OFF)
option(USE_MNNVL "option for using Multi-Node NVLink transport" OFF)
option(USE_CXL "option for using CXL protocol" OFF)
option(USE_EFA "option for using AWS EFA transport" OFF)

if (USE_EFA)
  # Add EFA/libfabric library path globally
  link_directories(/opt/amazon/efa/lib)
  include_directories(/opt/amazon/efa/include)
  add_compile_definitions(USE_EFA)
  message(STATUS "AWS EFA (libfabric) transport is enabled")
endif()
option(USE_ETCD "option for enable etcd as metadata server" OFF)
option(USE_ETCD_LEGACY "option for enable etcd based on etcd-cpp-api-v3" OFF)
option(USE_REDIS "option for enable redis as metadata server" OFF)
option(USE_HTTP "option for enable http as metadata server" ON)
option(WITH_RUST_EXAMPLE "build the Rust interface and sample code for the transfer engine" OFF)
option(WITH_METRICS "enable metrics and metrics reporting thread" ON)
option(USE_3FS "option for using 3FS storage backend" OFF)
option(WITH_NVIDIA_PEERMEM "disable to support RDMA without nvidia-peermem. If WITH_NVIDIA_PEERMEM=OFF then USE_CUDA=ON is required." ON)
option(USE_EVENT_DRIVEN_COMPLETION "option for using event-driven completion (store & transfer engine)" OFF)

option(USE_TENT "option for building Mooncake TENT" OFF)

option(USE_LRU_MASTER "option for using LRU in master service" OFF)
option(USE_INTRA_NVLINK "option for using IntraNode nvlink transport" OFF)
set(LRU_MAX_CAPACITY 1000)

if (USE_LRU_MASTER)
  add_compile_definitions(USE_LRU_MASTER)
  add_compile_definitions(LRU_MAX_CAPACITY)
endif()

if (USE_EVENT_DRIVEN_COMPLETION)
  add_compile_definitions(USE_EVENT_DRIVEN_COMPLETION)
  message(STATUS "Event-driven completion is enabled")
else()
  message(STATUS "Event-driven completion is disabled")
endif()

if (USE_NVMEOF)
  set(USE_CUDA ON)
  add_compile_definitions(USE_NVMEOF)
  message(STATUS "NVMe-oF support is enabled")
endif()

if (USE_MNNVL)
  if (NOT USE_HIP AND NOT USE_MUSA)
    set(USE_CUDA ON)
  endif()
  add_compile_definitions(USE_MNNVL)
  message(STATUS "Multi-Node NVLink support is enabled")
endif()

if (USE_CUDA)
  add_compile_definitions(USE_CUDA)
  message(STATUS "CUDA support is enabled")
  include_directories(/usr/local/cuda/include)
  link_directories(
    /usr/local/cuda/lib
    /usr/local/cuda/lib64
  )
endif()

if (USE_MUSA)
  add_compile_definitions(USE_MUSA)
  message(STATUS "MUSA support is enabled")
  include_directories(/usr/local/musa/include)
  link_directories(
    /usr/local/musa/lib
  )
endif()

if (USE_HIP)
  list(APPEND CMAKE_PREFIX_PATH "/opt/rocm/lib/cmake")
  find_package(HIP REQUIRED)
  include_directories(${HIP_INCLUDE_DIRS})
  add_compile_definitions(USE_HIP __HIP_PLATFORM_AMD__)
  message(STATUS "HIP support is enabled")

  find_program(HIPIFY_PERL_EXECUTABLE hipify-perl)
  if(NOT HIPIFY_PERL_EXECUTABLE)
    message(FATAL_ERROR
            "hipify-perl not found.\n"
            "Please ensure the ROCm or HIP SDK is installed and in your PATH.")
  endif()
endif()

# This function converts given CUDA source files into HIP-compatible
# files using hipify-perl, placing the outputs in the build directory for use in
# project compilation. The file path changes to a new location after hipify.
function(hipify_files input_var_name)
    set(result_files)

    foreach(input_file IN LISTS ${input_var_name})
        file(RELATIVE_PATH rel_path ${CMAKE_SOURCE_DIR} ${input_file})
        set(output_file "${CMAKE_BINARY_DIR}/${rel_path}")

        get_filename_component(output_dir ${output_file} DIRECTORY)
        file(MAKE_DIRECTORY ${output_dir})

        add_custom_command(
            OUTPUT ${output_file}
            COMMAND ${HIPIFY_PERL_EXECUTABLE} ${input_file} > ${output_file}
            DEPENDS ${input_file}
            COMMENT "HIPifying ${input_file} → ${output_file}"
        )

        list(APPEND result_files ${output_file})
    endforeach()

    set(${input_var_name} ${result_files} PARENT_SCOPE)
endfunction()

if (USE_CXL)
  add_compile_definitions(USE_CXL)
  message(STATUS "CXL support is enabled")
endif()

if (USE_TCP)
  add_compile_definitions(USE_TCP)
endif()

if (USE_BAREX)
  add_compile_definitions(USE_BAREX)
endif()

if (USE_ASCEND OR USE_ASCEND_DIRECT OR USE_UBSHMEM)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DOPEN_BUILD_PROJECT ")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DOPEN_BUILD_PROJECT ")
  string(TOLOWER "${CMAKE_SYSTEM_PROCESSOR}" CURRENT_CPU)
  set(CPU_ARCH "unknown")
  if(CURRENT_CPU MATCHES "^(aarch64|arm64)$")
    set(CPU_ARCH "aarch64")
  elseif(CURRENT_CPU MATCHES "^(x86_64|amd64)$")
    set(CPU_ARCH "x86_64")
  else()
    message(WARNING "Unsupported cpu arch.")
  endif()
  if(DEFINED ENV{ASCEND_HOME_PATH})
    message(STATUS "Use env ASCEND_HOME_PATH")
    file(GLOB ASCEND_TOOLKIT_ROOT "$ENV{ASCEND_HOME_PATH}/${CPU_ARCH}-linux")
  else()
    file(GLOB ASCEND_TOOLKIT_ROOT "/usr/local/Ascend/ascend-toolkit/latest/${CPU_ARCH}-linux")
  endif()
  set(ASCEND_LIB_DIR "${ASCEND_TOOLKIT_ROOT}/lib64")
  set(ASCEND_INCLUDE_DIR "${ASCEND_TOOLKIT_ROOT}/include")
  add_compile_options(-Wno-ignored-qualifiers)
  include_directories(/usr/local/include /usr/include ${ASCEND_INCLUDE_DIR})
  link_directories(${ASCEND_LIB_DIR})
endif()

if (USE_ASCEND)
  set(ASCEND_DEVLIB_DIR "${ASCEND_TOOLKIT_ROOT}/devlib")
  link_directories(${ASCEND_DEVLIB_DIR})
  add_compile_definitions(USE_ASCEND)
endif()

if (USE_ASCEND_DIRECT)
  set(BUILD_SHARED_LIBS ON)
  add_compile_definitions(USE_ASCEND_DIRECT)
endif()

if (USE_UBSHMEM)
  set(BUILD_SHARED_LIBS ON)
  add_compile_definitions(USE_UBSHMEM)
endif()

if (USE_ASCEND_HETEROGENEOUS)
  file(GLOB ASCEND_TOOLKIT_ROOT "/usr/local/Ascend/ascend-toolkit/latest/*-linux")
  set(ASCEND_LIB_DIR "${ASCEND_TOOLKIT_ROOT}/lib64")
  set(ASCEND_INCLUDE_DIR "${ASCEND_TOOLKIT_ROOT}/include")
  add_compile_definitions(USE_ASCEND_HETEROGENEOUS)
  include_directories(/usr/local/include /usr/include ${ASCEND_INCLUDE_DIR})
  link_directories(${ASCEND_LIB_DIR})
endif()

if (USE_REDIS)
  add_compile_definitions(USE_REDIS)
  message(STATUS "Redis as metadata server support is enabled")
endif()

if (USE_HTTP)
  add_compile_definitions(USE_HTTP)
  message(STATUS "Http as metadata server support is enabled")
endif()

if (NOT USE_ETCD AND NOT USE_REDIS AND NOT USE_HTTP)
  message(STATUS "None of USE_ETCD, USE_REDIS, USE_HTTP is selected, only \"P2PHANDSHAKE\" is supported as metadata server")
endif()

if (WITH_METRICS)
  add_compile_definitions(WITH_METRICS)
  message(STATUS "metrics is enabled")
endif()

if(USE_3FS)
  add_compile_definitions(USE_3FS)
  message(STATUS "3FS storage backend is enabled")
endif()

if(WITH_NVIDIA_PEERMEM)
  add_compile_definitions(WITH_NVIDIA_PEERMEM)
endif()

set(GFLAGS_USE_TARGET_NAMESPACE "true")
find_package(yaml-cpp REQUIRED)
find_package(gflags REQUIRED)
find_package(yalantinglibs CONFIG REQUIRED)
add_compile_definitions(YLT_ENABLE_IBV)
