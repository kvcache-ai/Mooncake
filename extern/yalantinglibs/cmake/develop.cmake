message(STATUS "-------------YLT DEVELOP SETTING------------")
# extra
option(BUILD_EXAMPLES "Build examples" ON)
message(STATUS "BUILD_EXAMPLES: ${BUILD_EXAMPLES}")

# bench test
option(BUILD_BENCHMARK "Build benchmark" ON)
message(STATUS "BUILD_BENCHMARK: ${BUILD_BENCHMARK}")

# unit test
option(BUILD_UNIT_TESTS "Build unit tests" ON)
message(STATUS "BUILD_UNIT_TESTS: ${BUILD_UNIT_TESTS}")
if(BUILD_UNIT_TESTS)
    enable_testing()
endif()

# coverage test
option(COVERAGE_TEST "Build with unit test coverage" OFF)
message(STATUS "COVERAGE_TEST: ${COVERAGE_TEST}")
if(COVERAGE_TEST)
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage --coverage")
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
    endif()
endif()

# generator benchmark test data
option(GENERATE_BENCHMARK_DATA "Generate benchmark data" ON)
message(STATUS "GENERATE_BENCHMARK_DATA: ${GENERATE_BENCHMARK_DATA}")


# Enable coro_rpc user define protocol example
option(CORO_RPC_USE_OTHER_RPC "coro_rpc extend to support other rpc" OFF)
message(STATUS "CORO_RPC_USE_OTHER_RPC: ${CORO_RPC_USE_OTHER_RPC}")

# Enable address sanitizer
option(ENABLE_SANITIZER "Enable sanitizer(Debug+Gcc/Clang/AppleClang)" ON)

option(ENABLE_TSAN "Enable thread sanitizer" OFF)

if(ENABLE_SANITIZER AND NOT MSVC)
    if (ENABLE_TSAN) 
        ## thread sanitizer
        check_tsan(HAS_TSAN)
        if(HAS_TSAN)
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
        else()
            message(WARNING "thread sanitizer is no supported with current tool-chains")
        endif()
    else()
        ## address santizer
        if(CMAKE_BUILD_TYPE STREQUAL "Debug" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
            check_asan(HAS_ASAN)
            if(HAS_ASAN)
                set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
            else()
                message(WARNING "address sanitizer is no supported with current tool-chains")
            endif()
        endif()
    endif()
endif()

# warning
option(ENABLE_WARNING "Enable warning for all project " OFF)
if(ENABLE_WARNING)
    if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
        list(APPEND MSVC_OPTIONS "/W3")
        if(MSVC_VERSION GREATER 1900) # Allow non fatal security warnings for msvc 2015
            list(APPEND MSVC_OPTIONS "/WX")
        endif()
        add_compile_options(MSVC_OPTIONS)
    else()
        add_compile_options(-Wall
                            -Wextra
                            -Wconversion
                            -pedantic
                            -Werror
                            -Wfatal-errors)
    endif()
endif()
message(STATUS "--------------------------------------------")