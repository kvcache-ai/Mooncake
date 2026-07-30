# Copyright 2026 KVCache.AI

include_guard(GLOBAL)

set(MOONCAKE_GFLAGS_VERSION "2.3.0")
set(MOONCAKE_GLOG_VERSION "0.7.1")

set(_gflags_dir "${_MOONCAKE_DEPS_ROOT}/extern/gflags")
set(_glog_dir "${_MOONCAKE_DEPS_ROOT}/extern/glog")

_mooncake_require_vendored("${_gflags_dir}/src/gflags.cc" gflags
                           "${MOONCAKE_GFLAGS_VERSION}")
_mooncake_require_vendored("${_glog_dir}/src/logging.cc" glog
                           "${MOONCAKE_GLOG_VERSION}")

# gflags supports being embedded as a subproject. Pin every switch explicitly so
# a parent BUILD_SHARED_LIBS value cannot change the artifact we absorb.
set(GFLAGS_IS_SUBPROJECT TRUE)
set(GFLAGS_BUILD_SHARED_LIBS OFF)
set(GFLAGS_BUILD_STATIC_LIBS ON)
set(GFLAGS_BUILD_gflags_LIB ON)
set(GFLAGS_BUILD_gflags_nothreads_LIB OFF)
set(GFLAGS_BUILD_PACKAGING OFF)
set(GFLAGS_BUILD_TESTING OFF)
set(GFLAGS_INSTALL_HEADERS OFF)
set(GFLAGS_INSTALL_STATIC_LIBS OFF)
# Preserve the long-standing google:: namespace compatibility used throughout
# Mooncake while also exposing the upstream gflags:: namespace.
set(GFLAGS_NAMESPACE "google;gflags")
add_subdirectory("${_gflags_dir}" "${CMAKE_BINARY_DIR}/_deps/mooncake_gflags"
                 EXCLUDE_FROM_ALL)
_mooncake_set_bundled_properties(gflags_static)
set_target_properties(gflags_static PROPERTIES OUTPUT_NAME mooncake_gflags)
add_library(Mooncake::gflags ALIAS gflags_static)

# glog 0.7.1 raises its standalone build requirement to CMake 3.22, while
# Mooncake supports CMake 3.16. Build the release's normal source set directly
# and retain its feature probes and generated headers.
include(CheckCXXSourceCompiles)
include(CheckCXXSymbolExists)
include(CheckIncludeFileCXX)
include(CheckStructHasMember)
include(CheckTypeSize)
include(CMakePushCheckState)
include(GenerateExportHeader)

check_include_file_cxx(dlfcn.h HAVE_DLFCN_H)
check_include_file_cxx(elf.h HAVE_ELF_H)
check_include_file_cxx(glob.h HAVE_GLOB_H)
check_include_file_cxx(link.h HAVE_LINK_H)
check_include_file_cxx(pwd.h HAVE_PWD_H)
check_include_file_cxx(sys/exec_elf.h HAVE_SYS_EXEC_ELF_H)
check_include_file_cxx(sys/syscall.h HAVE_SYS_SYSCALL_H)
check_include_file_cxx(sys/time.h HAVE_SYS_TIME_H)
check_include_file_cxx(sys/types.h HAVE_SYS_TYPES_H)
check_include_file_cxx(sys/ucontext.h HAVE_SYS_UCONTEXT_H)
check_include_file_cxx(sys/utsname.h HAVE_SYS_UTSNAME_H)
check_include_file_cxx(sys/wait.h HAVE_SYS_WAIT_H)
check_include_file_cxx(syscall.h HAVE_SYSCALL_H)
check_include_file_cxx(syslog.h HAVE_SYSLOG_H)
check_include_file_cxx(ucontext.h HAVE_UCONTEXT_H)
check_include_file_cxx(unistd.h HAVE_UNISTD_H)

check_type_size(mode_t HAVE_MODE_T LANGUAGE CXX)
check_type_size(ssize_t HAVE_SSIZE_T LANGUAGE CXX)
check_cxx_symbol_exists(dladdr dlfcn.h HAVE_DLADDR)
check_cxx_symbol_exists(fcntl fcntl.h HAVE_FCNTL)
check_cxx_symbol_exists(posix_fadvise fcntl.h HAVE_POSIX_FADVISE)
check_cxx_symbol_exists(pread unistd.h HAVE_PREAD)
check_cxx_symbol_exists(pwrite unistd.h HAVE_PWRITE)
check_cxx_symbol_exists(sigaction csignal HAVE_SIGACTION)
check_cxx_symbol_exists(sigaltstack csignal HAVE_SIGALTSTACK)
check_cxx_symbol_exists(backtrace execinfo.h HAVE_EXECINFO_BACKTRACE)
check_cxx_symbol_exists(backtrace_symbols execinfo.h
                        HAVE_EXECINFO_BACKTRACE_SYMBOLS)
check_cxx_symbol_exists(_chsize_s io.h HAVE__CHSIZE_S)
check_cxx_symbol_exists(abi::__cxa_demangle cxxabi.h HAVE___CXA_DEMANGLE)
check_cxx_symbol_exists(__argv cstdlib HAVE___ARGV)
check_cxx_symbol_exists(getprogname cstdlib HAVE_GETPROGNAME)
check_cxx_symbol_exists(program_invocation_short_name cerrno
                        HAVE_PROGRAM_INVOCATION_SHORT_NAME)
check_cxx_source_compiles(
  "#include <cstdlib>\nextern char* __progname;\nint main() { return __progname != nullptr ? EXIT_SUCCESS : EXIT_FAILURE; }"
  HAVE___PROGNAME)
check_cxx_symbol_exists(gmtime_r "cstdlib;ctime" HAVE_GMTIME_R)
check_cxx_symbol_exists(localtime_r "cstdlib;ctime" HAVE_LOCALTIME_R)

set(GLOG_THREAD_LOCAL_STORAGE 1)
set(SIZEOF_VOID_P ${CMAKE_SIZEOF_VOID_P})
set(TEST_SRC_DIR \"${_glog_dir}\")
if((HAVE_ELF_H OR HAVE_SYS_EXEC_ELF_H) AND UNIX)
  set(HAVE_SYMBOLIZE 1)
endif()

if(HAVE_UCONTEXT_H AND NOT DEFINED PC_FROM_UCONTEXT)
  cmake_push_check_state(RESET)
  set(CMAKE_REQUIRED_DEFINITIONS -D_GNU_SOURCE)
  set(_glog_pc_fields
      "uc_mcontext.gregs[REG_PC]"
      "uc_mcontext.gregs[REG_EIP]"
      "uc_mcontext.gregs[REG_RIP]"
      "uc_mcontext.sc_ip"
      "uc_mcontext.pc"
      "uc_mcontext.uc_regs->gregs[PT_NIP]"
      "uc_mcontext.gregs[R15]"
      "uc_mcontext.arm_pc"
      "uc_mcontext.gp_regs[PT_NIP]"
      "uc_mcontext.mc_eip"
      "uc_mcontext.mc_rip"
      "uc_mcontext.__gregs[_REG_EIP]"
      "uc_mcontext.__gregs[_REG_RIP]"
      "uc_mcontext->ss.eip"
      "uc_mcontext->__ss.__eip"
      "uc_mcontext->ss.rip"
      "uc_mcontext->__ss.__rip"
      "uc_mcontext->ss.srr0"
      "uc_mcontext->__ss.__srr0")
  foreach(_glog_pc_field IN LISTS _glog_pc_fields)
    foreach(_glog_pc_header IN ITEMS ucontext.h signal.h)
      string(REGEX REPLACE "[^a-zA-Z0-9]" "_" _glog_pc_check
                           "HAVE_PC_FROM_UCONTEXT_${_glog_pc_field}")
      string(REGEX REPLACE "_+$" "" _glog_pc_check "${_glog_pc_check}")
      check_struct_has_member(
        ucontext_t "${_glog_pc_field}" "${_glog_pc_header}" "${_glog_pc_check}"
        LANGUAGE CXX)
      if(${_glog_pc_check})
        set(PC_FROM_UCONTEXT "${_glog_pc_field}")
        break()
      endif()
    endforeach()
    if(DEFINED PC_FROM_UCONTEXT)
      break()
    endif()
  endforeach()
  cmake_pop_check_state()
endif()

set(_glog_build_dir "${CMAKE_BINARY_DIR}/_deps/mooncake_glog")
file(MAKE_DIRECTORY "${_glog_build_dir}/glog")
configure_file("${_glog_dir}/src/config.h.cmake.in"
               "${_glog_build_dir}/config.h")

set(_glog_sources
    "${_glog_dir}/src/demangle.cc"
    "${_glog_dir}/src/flags.cc"
    "${_glog_dir}/src/logging.cc"
    "${_glog_dir}/src/raw_logging.cc"
    "${_glog_dir}/src/signalhandler.cc"
    "${_glog_dir}/src/stacktrace.cc"
    "${_glog_dir}/src/symbolize.cc"
    "${_glog_dir}/src/utilities.cc"
    "${_glog_dir}/src/vlog_is_on.cc")
if((CYGWIN OR WIN32) AND NOT UNIX)
  list(APPEND _glog_sources "${_glog_dir}/src/windows/port.cc")
endif()

add_library(mooncake_glog STATIC EXCLUDE_FROM_ALL ${_glog_sources})
generate_export_header(
  mooncake_glog
  EXPORT_MACRO_NAME
  GLOG_EXPORT
  NO_EXPORT_MACRO_NAME
  GLOG_NO_EXPORT
  DEPRECATED_MACRO_NAME
  GLOG_DEPRECATED
  EXPORT_FILE_NAME
  "${_glog_build_dir}/glog/export.h"
  STATIC_DEFINE
  GLOG_STATIC_DEFINE)
target_compile_features(mooncake_glog PUBLIC cxx_std_14)
target_compile_definitions(
  mooncake_glog
  PUBLIC GLOG_USE_GLOG_EXPORT GLOG_STATIC_DEFINE GLOG_USE_GFLAGS
  PRIVATE GLOG_NO_SYMBOLIZE_DETECTION)
if((CYGWIN OR WIN32) AND NOT UNIX)
  target_compile_definitions(mooncake_glog PRIVATE GLOG_USE_WINDOWS_PORT)
  target_include_directories(mooncake_glog SYSTEM
                             PUBLIC "${_glog_dir}/src/windows")
endif()
# glog's sources include a generically named "config.h". Keep its generated
# directory before legacy project-wide include paths (Transfer Engine also has a
# config.h), while presenting the same paths as system includes to consumers.
target_include_directories(mooncake_glog BEFORE PRIVATE "${_glog_build_dir}"
                                                        "${_glog_dir}/src")
target_include_directories(mooncake_glog SYSTEM INTERFACE "${_glog_build_dir}"
                                                          "${_glog_dir}/src")
target_link_libraries(
  mooncake_glog
  PUBLIC gflags::gflags
  PRIVATE Threads::Threads ${CMAKE_DL_LIBS})
_mooncake_set_bundled_properties(mooncake_glog)
set_target_properties(mooncake_glog PROPERTIES OUTPUT_NAME mooncake_glog)
if(UNIX AND NOT APPLE)
  # gflags/glog use explicit export annotations upstream. Consumers that absorb
  # these archives should still keep the implementation out of their dynamic
  # symbol tables.
  target_link_options(
    mooncake_glog INTERFACE
    "LINKER:--exclude-libs=libmooncake_glog.a:libmooncake_gflags.a")
endif()

add_library(glog ALIAS mooncake_glog)
add_library(glog::glog ALIAS mooncake_glog)
add_library(Mooncake::glog ALIAS mooncake_glog)
