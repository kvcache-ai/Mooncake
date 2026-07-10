# limit_jobs.cmake — Memory-aware build parallelism
#
# Auto-detects available memory and CPU count, calculates safe parallel job
# limits for compilation and linking separately. With Ninja, creates job pools
# so compilation uses many cores while memory-heavy linking is restricted.
#
# User overrides (cmake -D...):
#   PARALLEL_COMPILE_JOBS  — override compile parallelism
#   PARALLEL_LINK_JOBS     — override link parallelism
#   MAX_COMPILER_MEMORY_MB — per-compile-job memory estimate (default: 1500)
#   MAX_LINKER_MEMORY_MB   — per-link-job memory estimate (default: 4000)

set(MAX_COMPILER_MEMORY_MB "1500" CACHE STRING
    "Estimated peak memory per compile job in MB")
set(MAX_LINKER_MEMORY_MB "4000" CACHE STRING
    "Estimated peak memory per link job in MB")

# Guard against invalid user input (division by zero)
if(MAX_COMPILER_MEMORY_MB LESS_EQUAL 0)
    message(WARNING "[limit_jobs] MAX_COMPILER_MEMORY_MB=${MAX_COMPILER_MEMORY_MB} "
        "invalid, falling back to 1500")
    set(MAX_COMPILER_MEMORY_MB 1500 CACHE STRING
        "Estimated peak memory per compile job in MB" FORCE)
endif()
if(MAX_LINKER_MEMORY_MB LESS_EQUAL 0)
    message(WARNING "[limit_jobs] MAX_LINKER_MEMORY_MB=${MAX_LINKER_MEMORY_MB} "
        "invalid, falling back to 4000")
    set(MAX_LINKER_MEMORY_MB 4000 CACHE STRING
        "Estimated peak memory per link job in MB" FORCE)
endif()

# Detect system resources
cmake_host_system_information(RESULT _available_mem_mb
    QUERY AVAILABLE_PHYSICAL_MEMORY)
cmake_host_system_information(RESULT _nproc
    QUERY NUMBER_OF_LOGICAL_CORES)

message(STATUS "[limit_jobs] Available memory: ${_available_mem_mb} MB, "
    "CPU cores: ${_nproc}")

# Calculate safe parallel jobs from memory
math(EXPR _compile_jobs "${_available_mem_mb} / ${MAX_COMPILER_MEMORY_MB}")
math(EXPR _link_jobs "${_available_mem_mb} / ${MAX_LINKER_MEMORY_MB}")

# Clamp: [1, nproc]
if(_compile_jobs LESS 1)
    set(_compile_jobs 1)
endif()
if(_compile_jobs GREATER _nproc)
    set(_compile_jobs ${_nproc})
endif()
if(_link_jobs LESS 1)
    set(_link_jobs 1)
endif()
if(_link_jobs GREATER _nproc)
    set(_link_jobs ${_nproc})
endif()

# Use auto-detected values unless user explicitly overrides with -D
if(NOT DEFINED PARALLEL_COMPILE_JOBS)
    set(PARALLEL_COMPILE_JOBS "${_compile_jobs}")
endif()
if(NOT DEFINED PARALLEL_LINK_JOBS)
    set(PARALLEL_LINK_JOBS "${_link_jobs}")
endif()

message(STATUS "[limit_jobs] Compile jobs: ${PARALLEL_COMPILE_JOBS} "
    "(${MAX_COMPILER_MEMORY_MB} MB/job), "
    "Link jobs: ${PARALLEL_LINK_JOBS} (${MAX_LINKER_MEMORY_MB} MB/job)")

# Apply to build system
if(CMAKE_GENERATOR MATCHES "Ninja")
    set_property(GLOBAL APPEND PROPERTY JOB_POOLS
        compile_pool=${PARALLEL_COMPILE_JOBS}
        link_pool=${PARALLEL_LINK_JOBS}
    )
    set(CMAKE_JOB_POOL_COMPILE "compile_pool" CACHE STRING "" FORCE)
    set(CMAKE_JOB_POOL_LINK "link_pool" CACHE STRING "" FORCE)
    message(STATUS "[limit_jobs] Ninja job pools: "
        "compile=${PARALLEL_COMPILE_JOBS}, link=${PARALLEL_LINK_JOBS}")
else()
    message(STATUS "[limit_jobs] Hint: use -G Ninja for automatic "
        "compile/link parallelism separation")
    message(STATUS "[limit_jobs] With Make, recommend: "
        "cmake --build . -j${PARALLEL_LINK_JOBS}")
endif()
