// ASIO separate compilation implementation. This translation unit is compiled
// into Mooncake's private static Asio target and absorbed by final artifacts.

// Define ASIO macros before including headers to ensure proper compilation
#ifndef ASIO_SEPARATE_COMPILATION
#define ASIO_SEPARATE_COMPILATION
#endif

#include <asio/impl/src.hpp>

#ifdef YLT_ENABLE_SSL
#include <asio/ssl/impl/src.hpp>
#endif
