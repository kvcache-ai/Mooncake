message(STATUS "-------------YLT COMPILE SETTING------------")

# CPP Standard
foreach(i ${CMAKE_CXX_COMPILE_FEATURES})
    if (i STREQUAL cxx_std_20)
        set(has_cxx_std_20 TRUE)
    endif()
    if (i STREQUAL cxx_std_17)
        set(has_cxx_std_17 TRUE)
    endif()
endforeach()
if (has_cxx_std_20)
    if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 13)
            set(has_cxx_std_20 FALSE)
        endif()
    elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 10)
            set(has_cxx_std_20 FALSE)
        endif()
    elseif(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
        if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 19.30)
            set(has_cxx_std_20 FALSE)
        endif()
    endif()
endif()

if (has_cxx_std_20)
    option(ENABLE_CPP_20 "Enable CPP 20" ON)
elseif (has_cxx_std_17)
    option(ENABLE_CPP_20 "Enable CPP 20" OFF)
else()
    message(FATAL_ERROR "Compiler ${CMAKE_CXX_COMPILER} has no C++17 support.")
endif()
if (ENABLE_CPP_20)  
    set(CMAKE_CXX_STANDARD 20)
else()
    set(CMAKE_CXX_STANDARD 17)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)
message(STATUS "CXX Standard: ${CMAKE_CXX_STANDARD}")
# Build Type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release")
endif()
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
# libc++ or libstdc++&clang
option(BUILD_WITH_LIBCXX "Build with libc++" OFF)
message(STATUS "BUILD_WITH_LIBCXX: ${BUILD_WITH_LIBCXX}")
if(BUILD_WITH_LIBCXX AND CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
else()
endif()

include (TestBigEndian)
TEST_BIG_ENDIAN(IS_BIG_ENDIAN)
if(IS_BIG_ENDIAN)
 message(STATUS "ENDIAN: BIG")
else()
 message(STATUS "ENDIAN: LITTLE")
endif()

# When using coro_rpc_client to send request, only remote function declarations are required.
# In the examples, RPC function declaration and definition are divided.
# However, clang + ld(gold linker) + '-g' will report 'undefined reference to xxx'.
# So you can use lld when compiler is clang by this code:

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    check_lld(has_lld)
    if (has_lld)
        add_link_options(-fuse-ld=lld)    
    endif()
endif()

# ccache
option(USE_CCACHE "use ccache to faster compile when develop" OFF)
message(STATUS "USE_CCACHE: ${USE_CCACHE}")
if (USE_CCACHE)
    find_program(CCACHE_FOUND ccache)
    if (CCACHE_FOUND)
        set(CMAKE_CXX_COMPILER_LAUNCHER ccache)
        set(CMAKE_C_COMPILER_LAUNCHER ccache)
        #set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache) # ccache for link is useless
    endif ()
endif ()

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    link_libraries(dl)
endif()

# --------------------- GCC
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if (ENABLE_CPP_20 AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcoroutines")
    endif()
    #-ftree-slp-vectorize with coroutine cause link error. disable it util gcc fix.
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fno-tree-slp-vectorize")
endif()
# --------------------- Msvc
# Resolves C1128 complained by MSVC: number of sections exceeded object file format limit: compile with /bigobj.
add_compile_options("$<$<CXX_COMPILER_ID:MSVC>:/bigobj>")
# Resolves C4737 complained by MSVC: C4737: Unable to perform required tail call. Performance may be degraded. "Release-Type only"
if(CMAKE_BUILD_TYPE STREQUAL "Release")
add_compile_options("$<$<CXX_COMPILER_ID:MSVC>:/EHa>")
endif()

message(STATUS "--------------------------------------------")
