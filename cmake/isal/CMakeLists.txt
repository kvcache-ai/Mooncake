# Optional Intel ISA-L CRC-64/ECMA acceleration (enabled with -DWITH_ISAL=ON).
#
# Added as a subdirectory rather than include()d so it gets a fresh compile-flag
# scope: the top-level project sets C/C++ options such as
# -fno-tree-slp-vectorize that the nasm assembler cannot parse. We reset the
# inherited flags here so only clean ASM_NASM flags reach nasm.
#
# Builds a tiny static library containing only ISA-L's PCLMULQDQ "by8" CRC-64
# assembly plus its shared constant table. We deliberately hardcode the by8
# variant instead of pulling in ISA-L's multibinary dispatcher. by8 needs only
# PCLMULQDQ (present on every x86-64 CPU since 2010), so a hardcoded call never
# faults with an illegal instruction. Benchmarking also showed the wider AVX-512
# (by16_10) variant only helps low-concurrency, large-object workloads whose
# data stays L3-resident; at the concurrency this store actually runs the
# checksum is memory/NIC bound and by8 already saturates the link.
#
# Requires the nasm assembler (>= 2.14.01). The source is fetched at a pinned
# tag so builds stay reproducible; point -DFETCHCONTENT_SOURCE_DIR_ISAL=<path>
# at a local checkout to build offline.

# Only the x86-64 by8 path is wired up here. On other architectures the by8
# assembly would not even assemble (or the symbol would be missing), so fail
# early with an actionable message instead of a confusing later link error.
# AArch64 (ISA-L ships a PMULL implementation) can be added later; until then
# WITH_ISAL=OFF gives the portable slicing-by-8 fallback on every platform.
if(NOT CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64)$")
  message(
    FATAL_ERROR
      "WITH_ISAL=ON currently supports x86-64 only (uses ISA-L's PCLMULQDQ by8 "
      "path), but CMAKE_SYSTEM_PROCESSOR is '${CMAKE_SYSTEM_PROCESSOR}'. "
      "Reconfigure with -DWITH_ISAL=OFF to use the portable slicing-by-8 "
      "fallback, which is correct on all architectures.")
endif()

# Do not let the parent scope's C/C++ compile options/definitions leak into the
# assembler command line.
set_property(DIRECTORY PROPERTY COMPILE_OPTIONS "")
set_property(DIRECTORY PROPERTY COMPILE_DEFINITIONS "")
set_property(DIRECTORY PROPERTY INCLUDE_DIRECTORIES "")

find_program(NASM_EXECUTABLE nasm)
if(NOT NASM_EXECUTABLE)
  message(
    FATAL_ERROR
      "WITH_ISAL=ON requires the nasm assembler (>= 2.14.01), which was not "
      "found. Install nasm or reconfigure with -DWITH_ISAL=OFF.")
endif()

execute_process(
  COMMAND ${NASM_EXECUTABLE} -v
  OUTPUT_VARIABLE _isal_nasm_version_output
  OUTPUT_STRIP_TRAILING_WHITESPACE)
string(REGEX MATCH "[0-9]+\\.[0-9]+\\.[0-9]+" _isal_nasm_version
             "${_isal_nasm_version_output}")
if(_isal_nasm_version VERSION_LESS "2.14.01")
  message(
    FATAL_ERROR
      "WITH_ISAL=ON requires nasm >= 2.14.01, but found ${_isal_nasm_version}. "
      "Upgrade nasm or reconfigure with -DWITH_ISAL=OFF.")
endif()

include(FetchContent)
FetchContent_Declare(
  isal
  GIT_REPOSITORY https://github.com/intel/isa-l.git
  GIT_TAG v2.32.1)
# ISA-L ships no CMakeLists, so only populate the source tree; its own autotools
# build is never invoked.
FetchContent_GetProperties(isal)
if(NOT isal_POPULATED)
  FetchContent_Populate(isal)
endif()

enable_language(ASM_NASM)

# Minimal, hand-verified file set: the by8 wrapper (which %includes the shared
# crc64_iso_norm_by8.asm implementation) and the constant table encoding the
# ECMA polynomial. crc64_iso_norm_by8.asm additionally pulls reg_sizes.asm from
# include/ and crc.inc / crc_const_extern.asm from crc/, hence both dirs are on
# the assembler include path below.
add_library(isal_crc64 STATIC ${isal_SOURCE_DIR}/crc/crc64_ecma_norm_by8.asm
                              ${isal_SOURCE_DIR}/crc/crc_const.asm)

target_include_directories(isal_crc64 PRIVATE ${isal_SOURCE_DIR}/include
                                              ${isal_SOURCE_DIR}/crc)

# PIC so the archive can also be absorbed into a shared libmooncake_common.
set_target_properties(isal_crc64 PROPERTIES POSITION_INDEPENDENT_CODE ON)
