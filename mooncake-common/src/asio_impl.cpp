// ASIO separate compilation implementation
// This file compiles all asio source code into a shared library
// to avoid ODR violations when multiple shared libraries use asio

// Define ASIO macros before including headers to ensure proper compilation
#ifndef ASIO_SEPARATE_COMPILATION
#define ASIO_SEPARATE_COMPILATION
#endif

#ifndef ASIO_DYN_LINK
#define ASIO_DYN_LINK
#endif

#include <asio/impl/src.hpp>
