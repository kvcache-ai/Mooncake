// ASIO separate compilation implementation
// This file compiles all asio source code into a shared library
// to avoid ODR violations when multiple shared libraries use asio

// ASIO_SEPARATE_COMPILATION and ASIO_DYN_LINK are defined globally in CMake

#include <asio/impl/src.hpp>
