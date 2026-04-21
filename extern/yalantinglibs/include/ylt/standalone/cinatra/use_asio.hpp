#pragma once

#if defined(ASIO_STANDALONE)
// MSVC : define environment path 'ASIO_STANDALONE_INCLUDE', e.g.
// 'E:\bdlibs\asio-1.10.6\include'

#include <asio.hpp>
#ifdef CINATRA_ENABLE_SSL
#include <asio/ssl.hpp>
#endif
#include <asio/steady_timer.hpp>

using tcp_socket = asio::ip::tcp::socket;
#ifdef CINATRA_ENABLE_SSL
using ssl_socket = asio::ssl::stream<asio::ip::tcp::socket>;
#endif
#endif
