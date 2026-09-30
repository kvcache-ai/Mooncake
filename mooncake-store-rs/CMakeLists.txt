# CMake integration for the Rust-native Mooncake Store (store-rs).
#
# The top-level CMakeLists.txt adds this directory only when WITH_STORE_RS=ON.
# CMake owns the TE/TENT shims and the Python component artifacts; Cargo builds
# the Rust commands and private PyO3 extension against those existing targets.

if(NOT TARGET transfer_engine)
  message(FATAL_ERROR "WITH_STORE_RS=ON requires WITH_TE=ON")
endif()
if(NOT TARGET tent_shared)
  message(FATAL_ERROR "WITH_STORE_RS=ON requires USE_TENT=ON")
endif()

find_program(CARGO_EXECUTABLE cargo)
if(NOT CARGO_EXECUTABLE)
  message(FATAL_ERROR "WITH_STORE_RS=ON requires cargo on PATH")
endif()

find_package(Python3 3.10 COMPONENTS Interpreter Development.Module REQUIRED)
if(NOT Python3_SOABI)
  message(FATAL_ERROR "WITH_STORE_RS=ON requires a CPython SOABI")
endif()

set(STORE_RS_SHIM_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/crates/mooncake-store-rs-transport-sys/src)
set(STORE_RS_YLT_INCLUDE_DIR
    ${yalantinglibs_SOURCE_DIR}/include)
set(STORE_RS_NATIVE_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/native-shims)
set(STORE_RS_CLI_CARGO_TARGET_DIR ${CMAKE_CURRENT_BINARY_DIR}/cargo-cli)
set(STORE_RS_PYTHON_CARGO_TARGET_DIR ${CMAKE_CURRENT_BINARY_DIR}/cargo-python)
set(STORE_RS_TEST_CARGO_TARGET_DIR ${CMAKE_CURRENT_BINARY_DIR}/cargo-tests)
set(STORE_RS_CLI_RELEASE_DIR ${STORE_RS_CLI_CARGO_TARGET_DIR}/release)
set(STORE_RS_PYTHON_RELEASE_DIR ${STORE_RS_PYTHON_CARGO_TARGET_DIR}/release)
set(STORE_RS_EXTENSION_SUFFIX
    ".${Python3_SOABI}${CMAKE_SHARED_MODULE_SUFFIX}")

add_library(mooncake_store_rs_classic_shim SHARED
            ${STORE_RS_SHIM_SOURCES}/classic_shim.cc)
target_compile_features(mooncake_store_rs_classic_shim PRIVATE cxx_std_20)
target_include_directories(
  mooncake_store_rs_classic_shim
  PRIVATE ${PROJECT_SOURCE_DIR}/mooncake-common/include
          ${STORE_RS_YLT_INCLUDE_DIR}
          ${STORE_RS_YLT_INCLUDE_DIR}/ylt/thirdparty
          ${STORE_RS_YLT_INCLUDE_DIR}/ylt/standalone)
target_link_libraries(mooncake_store_rs_classic_shim PRIVATE transfer_engine)
set_target_properties(
  mooncake_store_rs_classic_shim
  PROPERTIES OUTPUT_NAME mooncake_classic_shim
             LIBRARY_OUTPUT_DIRECTORY ${STORE_RS_NATIVE_OUTPUT_DIR}
             POSITION_INDEPENDENT_CODE ON
             INSTALL_RPATH "$ORIGIN")

add_library(mooncake_store_rs_tent_shim SHARED
            ${STORE_RS_SHIM_SOURCES}/tent_shim.cc)
target_compile_features(mooncake_store_rs_tent_shim PRIVATE cxx_std_20)
target_include_directories(
  mooncake_store_rs_tent_shim
  PRIVATE ${PROJECT_SOURCE_DIR}/mooncake-common/include
          ${STORE_RS_YLT_INCLUDE_DIR}
          ${STORE_RS_YLT_INCLUDE_DIR}/ylt/thirdparty
          ${STORE_RS_YLT_INCLUDE_DIR}/ylt/standalone)
target_link_libraries(mooncake_store_rs_tent_shim PRIVATE tent_shared)
set_target_properties(
  mooncake_store_rs_tent_shim
  PROPERTIES OUTPUT_NAME mooncake_tent_shim
             LIBRARY_OUTPUT_DIRECTORY ${STORE_RS_NATIVE_OUTPUT_DIR}
             POSITION_INDEPENDENT_CODE ON
             INSTALL_RPATH "$ORIGIN")

set(STORE_RS_CLASSIC_SHIM
    "$<TARGET_FILE:mooncake_store_rs_classic_shim>")
set(STORE_RS_TENT_SHIM "$<TARGET_FILE:mooncake_store_rs_tent_shim>")
set(STORE_RS_PYTHON_EXTENSION
    ${STORE_RS_PYTHON_RELEASE_DIR}/lib_store_rs${CMAKE_SHARED_LIBRARY_SUFFIX})

set(STORE_RS_CLI_CLIENT ${STORE_RS_CLI_RELEASE_DIR}/mooncake-store-client)
set(STORE_RS_CLI_ADMIN ${STORE_RS_CLI_RELEASE_DIR}/mooncake-store-admin)
set(STORE_RS_CLI_BENCH ${STORE_RS_CLI_RELEASE_DIR}/mooncake_store_bench)

add_custom_target(build_store_rs_cli
  COMMAND
    ${CMAKE_COMMAND} -E env
    "CARGO_TARGET_DIR=${STORE_RS_CLI_CARGO_TARGET_DIR}"
    "MOONCAKE_ROOT_DIR=${PROJECT_SOURCE_DIR}"
    "MOONCAKE_BUILD_DIR=${PROJECT_BINARY_DIR}"
    "MOONCAKE_STORE_RS_DIR=${CMAKE_CURRENT_SOURCE_DIR}"
    "MOONCAKE_CLASSIC_SHIM_LIB_PATH=${STORE_RS_CLASSIC_SHIM}"
    "MOONCAKE_TENT_SHIM_LIB_PATH=${STORE_RS_TENT_SHIM}"
    ${CARGO_EXECUTABLE} build -p mooncake-store-py --no-default-features --bins
    --release
  BYPRODUCTS ${STORE_RS_CLI_CLIENT} ${STORE_RS_CLI_ADMIN} ${STORE_RS_CLI_BENCH}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Building Store-RS command-line tools without Python bindings"
  VERBATIM)
add_dependencies(build_store_rs_cli transfer_engine tent_shared
                 mooncake_store_rs_classic_shim mooncake_store_rs_tent_shim)

add_custom_target(build_store_rs_python_extension
  COMMAND
    ${CMAKE_COMMAND} -E env
    "CARGO_TARGET_DIR=${STORE_RS_PYTHON_CARGO_TARGET_DIR}"
    "PYO3_PYTHON=${Python3_EXECUTABLE}"
    "MOONCAKE_ROOT_DIR=${PROJECT_SOURCE_DIR}"
    "MOONCAKE_BUILD_DIR=${PROJECT_BINARY_DIR}"
    "MOONCAKE_STORE_RS_DIR=${CMAKE_CURRENT_SOURCE_DIR}"
    "MOONCAKE_CLASSIC_SHIM_LIB_PATH=${STORE_RS_CLASSIC_SHIM}"
    "MOONCAKE_TENT_SHIM_LIB_PATH=${STORE_RS_TENT_SHIM}"
    ${CARGO_EXECUTABLE} build -p mooncake-store-py --no-default-features --lib
    --features python-extension --release
  BYPRODUCTS ${STORE_RS_PYTHON_EXTENSION}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Building private mooncake._store_rs extension"
  VERBATIM)
add_dependencies(build_store_rs_python_extension transfer_engine tent_shared
                 mooncake_store_rs_classic_shim mooncake_store_rs_tent_shim)

add_custom_target(build_store_rs ALL
  DEPENDS build_store_rs_cli build_store_rs_python_extension)

# The Python wheel installs the exact native CMake targets used by Cargo.
install(
  TARGETS mooncake_store_rs_classic_shim mooncake_store_rs_tent_shim tent_shared
  LIBRARY DESTINATION mooncake
  COMPONENT python)
install(
  FILES ${STORE_RS_PYTHON_EXTENSION}
  DESTINATION mooncake
  RENAME _store_rs${STORE_RS_EXTENSION_SUFFIX}
  COMPONENT python)
install(PROGRAMS ${STORE_RS_CLI_CLIENT} ${STORE_RS_CLI_ADMIN}
        DESTINATION mooncake COMPONENT python)
install(PROGRAMS ${STORE_RS_CLI_BENCH}
        DESTINATION mooncake RENAME mooncake-store-bench COMPONENT python)

# Unit tests use the default Python embedding feature; wheel builds use the
# extension-module feature above and therefore do not link libpython.
add_custom_target(build_store_rs_tests
  COMMAND
    ${CMAKE_COMMAND} -E env
    "CARGO_TARGET_DIR=${STORE_RS_TEST_CARGO_TARGET_DIR}"
    "PYO3_PYTHON=${Python3_EXECUTABLE}"
    "MOONCAKE_ROOT_DIR=${PROJECT_SOURCE_DIR}"
    "MOONCAKE_BUILD_DIR=${PROJECT_BINARY_DIR}"
    "MOONCAKE_STORE_RS_DIR=${CMAKE_CURRENT_SOURCE_DIR}"
    "MOONCAKE_CLASSIC_SHIM_LIB_PATH=${STORE_RS_CLASSIC_SHIM}"
    "MOONCAKE_TENT_SHIM_LIB_PATH=${STORE_RS_TENT_SHIM}"
    ${CARGO_EXECUTABLE} test --workspace --lib --no-run --release
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Building Store-RS Rust tests"
  VERBATIM)
add_dependencies(build_store_rs_tests transfer_engine tent_shared
                 mooncake_store_rs_classic_shim mooncake_store_rs_tent_shim)
