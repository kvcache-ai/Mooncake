if (YLT_ENABLE_SSL)
    # Check if OpenSSL is actually Tongsuo (which supports NTLS)
    # We check for NTLS-specific functions that only exist in Tongsuo
    include(CheckCSourceCompiles)
    set(CMAKE_REQUIRED_LIBRARIES ${OPENSSL_SSL_LIBRARY} ${OPENSSL_CRYPTO_LIBRARY})
    set(CMAKE_REQUIRED_INCLUDES ${OPENSSL_INCLUDE_DIR})
    enable_language(C)
    check_c_source_compiles("
        #include <openssl/ssl.h>
        int main() {
            SSL_CTX *ctx = NULL;
            SSL_CTX_enable_ntls(ctx);
            return 0;
        }
    " YLT_HAVE_NTLS_SUPPORT)
    unset(CMAKE_REQUIRED_LIBRARIES)
    unset(CMAKE_REQUIRED_INCLUDES)
    if (YLT_HAVE_NTLS_SUPPORT)
        message(STATUS "Found NTLS support")
    else()
        message(STATUS "NTLS support not found")
    endif()
endif()

    