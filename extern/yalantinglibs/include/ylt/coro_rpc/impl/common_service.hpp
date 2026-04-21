/*
 * Copyright (c) 2023, Alibaba Group Holding Limited;
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <filesystem>
#include <ylt/easylog.hpp>

#ifdef YLT_ENABLE_SSL
#include <asio/ssl.hpp>
#endif

namespace coro_rpc {
/*!
 * \file common_service.hpp
 */

/*!
 * SSL config
 */
struct ssl_configure {
  std::string base_path;  //!< all config files base path
  std::string cert_file;  //!< relative path of certificate chain file
  std::string key_file;   //!< relative path of private key file
  std::string dh_file;    //!< relative path of tmp dh file (optional)
};
#ifdef YLT_ENABLE_NTLS
///*!
// * NTLS config for Tongsuo support
// */
// struct ntls_configure {
//  std::string base_path;           //!< all config files base path
//  std::string sign_cert_file;      //!< relative path of SM2 signing
//  certificate file std::string sign_key_file;       //!< relative path of SM2
//  signing private key file std::string enc_cert_file;       //!< relative path
//  of SM2 encryption certificate file std::string enc_key_file;        //!<
//  relative path of SM2 encryption private key file std::string ca_cert_file;
//  //!< relative path of CA certificate file (optional) bool
//  enable_client_verify = false; //!< enable client certificate verification
//  bool enable_ntls = true;         //!< enable NTLS mode
//};

/*!
 * NTLS mode enumeration
 */
enum class ntls_mode {
  tlcp_dual_cert,    //!< GB/T 38636-2020 TLCP with dual certificates (signing +
                     //!< encryption)
  tls13_single_cert  //!< RFC 8998 TLS 1.3 + GM with single certificate
};

/*!
 * Extended SSL config that supports both SSL and NTLS
 */
struct ssl_ntls_configure {
  std::string base_path;  //!< all config files base path

  // Traditional SSL/TLS configuration
  std::string cert_file;  //!< relative path of certificate chain file
  std::string key_file;   //!< relative path of private key file
  std::string dh_file;    //!< relative path of tmp dh file (optional)

  // TLCP dual certificate configuration (GB/T 38636-2020)
  std::string
      sign_cert_file;         //!< relative path of SM2 signing certificate file
  std::string sign_key_file;  //!< relative path of SM2 signing private key file
  std::string
      enc_cert_file;  //!< relative path of SM2 encryption certificate file
  std::string
      enc_key_file;  //!< relative path of SM2 encryption private key file

  // TLS 1.3 + GM single certificate configuration (RFC 8998)
  std::string gm_cert_file;  //!< relative path of single SM2 certificate file
                             //!< (for TLS 1.3 + GM)
  std::string gm_key_file;   //!< relative path of single SM2 private key file
                             //!< (for TLS 1.3 + GM)

  // Common NTLS configuration
  std::string
      ca_cert_file;  //!< relative path of CA certificate file (optional)
  std::string cipher_suites;  //!< NTLS cipher suites (e.g.,
                              //!< "ECC-SM2-SM4-GCM-SM3:ECC-SM2-SM4-CBC-SM3")
  std::string server_name = "localhost";  //!< server name for verification

  ntls_mode mode = ntls_mode::tlcp_dual_cert;  //!< NTLS mode selection
  bool enable_ntls = false;                    //!< enable NTLS mode
  bool enable_client_verify =
      false;                      //!< enable client certificate verification
  bool enable_dual_mode = false;  //!< enable both SSL and NTLS support
};
#endif  // YLT_ENABLE_NTLS

/*!
 * Check file (not a folder) exist
 *
 * just a helper function
 *
 * @param path
 * @return true if file exist, otherwise false
 */
inline bool file_exists(const auto &path) {
  std::error_code ec;
  if (!std::filesystem::is_directory(path, ec) &&
      std::filesystem::exists(path, ec)) {
    return true;
  }
  return false;
};

#ifdef YLT_ENABLE_SSL
/*!
 * Initialize SSL Context `context` with SSL Config `conf`
 *
 * If init fail, a log reported and return false.
 *
 * @param context instance of asio::ssl::context
 * @param conf object of ssl_configure
 * @return true if init success, otherwise false
 */
inline bool init_ssl_context_helper(asio::ssl::context &context,
                                    const ssl_configure &conf) {
  namespace fs = std::filesystem;
  try {
    context.set_options(asio::ssl::context::default_workarounds |
                        asio::ssl::context::no_sslv2 |
                        asio::ssl::context::single_dh_use);
    context.set_password_callback(
        [](std::size_t size,
           asio::ssl::context_base::password_purpose purpose) {
          return "test";
        });
    auto cert_file = fs::path(conf.base_path).append(conf.cert_file);
    auto key_file = fs::path(conf.base_path).append(conf.key_file);
    auto dh_file = fs::path(conf.base_path).append(conf.dh_file);

    ELOG_INFO << "current path " << fs::current_path().string();
    if (file_exists(cert_file)) {
      ELOG_INFO << "load " << cert_file.string();
      context.use_certificate_chain_file(cert_file.string());
    }
    else {
      ELOG_ERROR << "no certificate file " << cert_file.string();
      return false;
    }

    if (file_exists(key_file)) {
      ELOG_INFO << "load " << key_file.string();
      context.use_private_key_file(key_file.string(), asio::ssl::context::pem);
    }
    else {
      ELOG_ERROR << "no private file " << key_file.string();
      return false;
    }

    if (file_exists(dh_file)) {
      ELOG_INFO << "load " << dh_file.string();
      context.use_tmp_dh_file(dh_file.string());
    }
    else {
      ELOG_INFO << "no temp dh file " << dh_file.string();
    }

    return true;
  } catch (std::exception &e) {
    ELOG_INFO << e.what();
    return false;
  }
}

#ifdef YLT_ENABLE_NTLS
/*!
 * Initialize SSL Context for NTLS with Tongsuo
 *
 * @param context instance of asio::ssl::context
 * @param conf object of ssl_ntls_configure
 * @return true if init success, otherwise false
 */
inline bool init_ntls_context_helper(asio::ssl::context &context,
                                     const ssl_ntls_configure &conf) {
  namespace fs = std::filesystem;
  try {
    // Set context options via asio
    context.set_options(asio::ssl::context::default_workarounds |
                        asio::ssl::context::no_sslv2 |
                        asio::ssl::context::single_dh_use);

    // Configure NTLS via Tongsuo native APIs
    SSL_CTX *ctx = context.native_handle();
    if (!ctx) {
      ELOG_ERROR << "SSL_CTX native_handle is null";
      return false;
    }

    // Configure based on NTLS mode
    if (conf.mode == ntls_mode::tls13_single_cert) {
      // Enable strict SM TLS 1.3 (Tongsuo)
      SSL_CTX_enable_sm_tls13_strict(ctx);

      // RFC 8998 TLS 1.3 + GM single certificate mode
      ELOG_INFO << "Configuring RFC 8998 TLS 1.3 + GM single certificate mode";

      // Set TLS 1.3 version
      if (SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION) != 1) {
        ELOG_ERROR << "Failed to set minimum TLS version to 1.3";
        return false;
      }
      if (SSL_CTX_set_max_proto_version(ctx, TLS1_3_VERSION) != 1) {
        ELOG_ERROR << "Failed to set maximum TLS version to 1.3";
        return false;
      }

      // Set TLS 1.3 GM cipher suites
      std::string cipher_suites = conf.cipher_suites.empty()
                                      ? "TLS_SM4_GCM_SM3:TLS_SM4_CCM_SM3"
                                      : conf.cipher_suites;
      if (SSL_CTX_set_ciphersuites(ctx, cipher_suites.c_str()) != 1) {
        unsigned long err = ::ERR_get_error();
        ELOG_ERROR << "Failed to set TLS 1.3 GM cipher suites '"
                   << cipher_suites
                   << "': " << ::ERR_error_string(err, nullptr);
        return false;
      }
      ELOG_INFO << "TLS 1.3 GM cipher suites set to: " << cipher_suites;

      // Set signature algorithms for SM2
      if (SSL_CTX_set1_sigalgs_list(ctx, "sm2sig_sm3") != 1) {
        unsigned long err = ::ERR_get_error();
        ELOG_ERROR << "Failed to set SM2 signature algorithms: "
                   << ::ERR_error_string(err, nullptr);
        return false;
      }

      // Set supported curves for SM2
      if (SSL_CTX_set1_curves_list(ctx, "SM2") != 1) {
        unsigned long err = ::ERR_get_error();
        ELOG_ERROR << "Failed to set SM2 curves: "
                   << ::ERR_error_string(err, nullptr);
        return false;
      }
    }
    else {
      // Enable NTLS mode for Tongsuo
      SSL_CTX_enable_ntls(ctx);
      ELOG_INFO << "NTLS mode enabled successfully";

      // GB/T 38636-2020 TLCP dual certificate mode (default)
      ELOG_INFO << "Configuring GB/T 38636-2020 TLCP dual certificate mode";

      // Set TLCP cipher suites (SM2/SM3/SM4)
      std::string cipher_suites =
          conf.cipher_suites.empty() ? "ECC-SM2-SM4-GCM-SM3:ECC-SM2-SM4-CBC-SM3"
                                     : conf.cipher_suites;
      if (SSL_CTX_set_cipher_list(ctx, cipher_suites.c_str()) != 1) {
        unsigned long err = ::ERR_get_error();
        ELOG_WARN << "Failed to set TLCP cipher suites '" << cipher_suites
                  << "': " << ::ERR_error_string(err, nullptr);
      }
      else {
        ELOG_INFO << "TLCP cipher suites set to: " << cipher_suites;
      }
    }
    context.set_password_callback(
        [](std::size_t size,
           asio::ssl::context_base::password_purpose purpose) {
          return "test";
        });

    ELOG_INFO << "current path " << fs::current_path().string();

    // Load certificates based on NTLS mode
    if (conf.mode == ntls_mode::tls13_single_cert) {
      // RFC 8998 TLS 1.3 + GM single certificate mode
      auto gm_cert_file = fs::path(conf.base_path).append(conf.gm_cert_file);
      auto gm_key_file = fs::path(conf.base_path).append(conf.gm_key_file);

      // Load single GM certificate
      if (file_exists(gm_cert_file)) {
        if (SSL_CTX_use_certificate_file(ctx, gm_cert_file.string().c_str(),
                                         SSL_FILETYPE_PEM) != 1) {
          unsigned long err = ::ERR_get_error();
          ELOG_ERROR << "failed to load GM certificate: "
                     << ::ERR_error_string(err, nullptr);
          return false;
        }
        ELOG_INFO << "loaded GM certificate: " << gm_cert_file.string();
      }
      else {
        ELOG_ERROR << "no GM certificate file " << gm_cert_file.string();
        return false;
      }

      // Load single GM private key
      if (file_exists(gm_key_file)) {
        if (SSL_CTX_use_PrivateKey_file(ctx, gm_key_file.string().c_str(),
                                        SSL_FILETYPE_PEM) != 1) {
          unsigned long err = ::ERR_get_error();
          ELOG_ERROR << "failed to load GM private key: "
                     << ::ERR_error_string(err, nullptr);
          return false;
        }
        ELOG_INFO << "loaded GM private key: " << gm_key_file.string();
      }
      else {
        ELOG_ERROR << "no GM private key file " << gm_key_file.string();
        return false;
      }
    }
    else {
      // GB/T 38636-2020 TLCP dual certificate mode
      auto sign_cert_file =
          fs::path(conf.base_path).append(conf.sign_cert_file);
      auto sign_key_file = fs::path(conf.base_path).append(conf.sign_key_file);
      auto enc_cert_file = fs::path(conf.base_path).append(conf.enc_cert_file);
      auto enc_key_file = fs::path(conf.base_path).append(conf.enc_key_file);

      // Load SM2 signing certificate and key
      if (file_exists(sign_cert_file)) {
        if (SSL_CTX_use_sign_certificate_file(
                ctx, sign_cert_file.string().c_str(), SSL_FILETYPE_PEM) != 1) {
          unsigned long err = ::ERR_get_error();
          ELOG_ERROR << "failed to load SM2 signing certificate: "
                     << ::ERR_error_string(err, nullptr);
          return false;
        }
        ELOG_INFO << "loaded SM2 signing certificate: "
                  << sign_cert_file.string();
      }
      else {
        ELOG_ERROR << "no SM2 signing certificate file "
                   << sign_cert_file.string();
        return false;
      }

      if (file_exists(sign_key_file)) {
        if (SSL_CTX_use_sign_PrivateKey_file(
                ctx, sign_key_file.string().c_str(), SSL_FILETYPE_PEM) != 1) {
          unsigned long err = ::ERR_get_error();
          ELOG_ERROR << "failed to load SM2 signing private key: "
                     << ::ERR_error_string(err, nullptr);
          return false;
        }
        ELOG_INFO << "loaded SM2 signing private key: "
                  << sign_key_file.string();
      }
      else {
        ELOG_ERROR << "no SM2 signing key file " << sign_key_file.string();
        return false;
      }

      // Load SM2 encryption certificate and key
      if (file_exists(enc_cert_file)) {
        if (SSL_CTX_use_enc_certificate_file(
                ctx, enc_cert_file.string().c_str(), SSL_FILETYPE_PEM) != 1) {
          unsigned long err = ::ERR_get_error();
          ELOG_ERROR << "failed to load SM2 encryption certificate: "
                     << ::ERR_error_string(err, nullptr);
          return false;
        }
        ELOG_INFO << "loaded SM2 encryption certificate: "
                  << enc_cert_file.string();
      }
      else {
        ELOG_ERROR << "no SM2 encryption certificate file "
                   << enc_cert_file.string();
        return false;
      }

      if (file_exists(enc_key_file)) {
        if (SSL_CTX_use_enc_PrivateKey_file(ctx, enc_key_file.string().c_str(),
                                            SSL_FILETYPE_PEM) != 1) {
          unsigned long err = ::ERR_get_error();
          ELOG_ERROR << "failed to load SM2 encryption private key: "
                     << ::ERR_error_string(err, nullptr);
          return false;
        }
        ELOG_INFO << "loaded SM2 encryption private key: "
                  << enc_key_file.string();
      }
      else {
        ELOG_ERROR << "no SM2 encryption key file " << enc_key_file.string();
        return false;
      }
    }

    // Load CA certificate if provided (ASIO wrapper is fine)
    asio::error_code ec;
    if (!conf.ca_cert_file.empty()) {
      auto ca_cert_file = fs::path(conf.base_path).append(conf.ca_cert_file);
      if (file_exists(ca_cert_file)) {
        context.load_verify_file(ca_cert_file.string(), ec);
        if (ec) {
          ELOG_WARN << "failed to load CA certificate: " << ec.message()
                    << ", continuing without it";
        }
      }
    }

    // Set verification mode
    if (conf.enable_client_verify) {
      context.set_verify_mode(asio::ssl::verify_peer, ec);
    }
    else {
      context.set_verify_mode(asio::ssl::verify_none, ec);
    }
    if (ec) {
      ELOG_WARN << "failed to set verify mode: " << ec.message();
    }

    ELOG_INFO << "NTLS server context initialized successfully";
    return true;
  } catch (std::exception &e) {
    ELOG_ERROR << "NTLS context init error: " << e.what();
    return false;
  }
}
#endif  // YLT_ENABLE_NTLS
#endif
}  // namespace coro_rpc