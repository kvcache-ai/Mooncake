#pragma once
#include <cctype>
#include <string_view>

#include "utils.hpp"

namespace cinatra {
// most of this code is from cpprestsdk
class uri_t {
 public:
  std::string_view schema;
  std::string_view uinfo;
  std::string_view host;
  std::string_view port;
  std::string_view path;
  std::string_view query;
  std::string_view fragment;
  bool is_ssl;

  bool is_unreserved(int c) {
    return std::isalnum((char)c) || c == '-' || c == '.' || c == '_' ||
           c == '~';
  }

  bool is_sub_delim(int c) {
    switch (c) {
      case '!':
      case '$':
      case '&':
      case '\'':
      case '|':
      case '(':
      case ')':
      case '*':
      case '+':
      case ',':
      case ';':
      case '=':
        return true;
      default:
        return false;
    }
  }

  bool is_websocket() { return schema == "ws"sv || schema == "wss"sv; }

  bool is_user_info_character(int c) {
    return is_unreserved(c) || is_sub_delim(c) || c == '%' || c == ':';
  }
  bool is_path_character(int c) {
    return is_unreserved(c) || is_sub_delim(c) || c == '%' || c == '/' ||
           c == ':' || c == '@';
  }
  bool is_query_character(int c) { return is_path_character(c) || c == '?'; }

  bool is_authority_character(int c) {
    return is_unreserved(c) || is_sub_delim(c) || c == '%' || c == '@' ||
           c == ':' || c == '[' || c == ']';
  }
  bool is_fragment_character(int c) {
    // this is intentional, they have the same set of legal characters
    return is_query_character(c);
  }

  bool is_scheme_character(int c) {
    return std::isalnum((char)c) || c == '+' || c == '-' || c == '.';
  }

  bool parse_from(const char *encoded) {
    const char *p = encoded;

    // IMPORTANT -- A uri may either be an absolute uri, or an
    // relative-reference Absolute: 'http://host.com' Relative-Reference:
    // '//:host.com', '/path1/path2?query', './path1:path2' A Relative-Reference
    // can be disambiguated by parsing for a ':' before the first slash

    bool is_relative_reference = true;
    const char *p2 = p;
    for (; *p2 != ('/') && *p2 != ('\0'); p2++) {
      if (*p2 == (':')) {
        // found a colon, the first portion is a scheme
        is_relative_reference = false;
        break;
      }
    }

    if (!is_relative_reference) {
      // the first character of a scheme must be a letter
      if (!isalpha(*p)) {
        return false;
      }

      // start parsing the scheme, it's always delimited by a colon (must be
      // present)
      auto scheme_begin = p++;
      for (; *p != ':'; p++) {
        if (!is_scheme_character(*p)) {
          return false;
        }
      }
      auto scheme_end = p;
      schema = std::string_view(scheme_begin, scheme_end - scheme_begin);
      is_ssl = (schema == "https" || schema == "wss");

      // skip over the colon
      p++;
    }

    // the uri must have http or https
    if (schema.empty()) {
      return false;
    }

    // if we see two slashes next, then we're going to parse the authority
    // portion later on we'll break up the authority into the port and host
    const char *authority_begin = nullptr;
    const char *authority_end = nullptr;
    if (*p == ('/') && p[1] == ('/')) {
      // skip over the slashes
      p += 2;
      authority_begin = p;

      // the authority is delimited by a slash (resource), question-mark (query)
      // or octothorpe (fragment) or by EOS. The authority could be empty
      // ('file:///C:\file_name.txt')
      for (; *p != ('/') && *p != ('?') && *p != ('#') && *p != ('\0'); p++) {
        // We're NOT currently supporting IPvFuture or username/password in
        // authority IPv6 as the host (i.e. http://[:::::::]) is allowed as
        // valid URI and passed to subsystem for support.
        if (!is_authority_character(*p)) {
          return false;
        }
      }
      authority_end = p;

      // now lets see if we have a port specified -- by working back from the
      // end
      if (authority_begin != authority_end) {
        // the port is made up of all digits
        const char *port_begin = authority_end - 1;
        for (; isdigit(*port_begin) && port_begin != authority_begin;
             port_begin--) {
        }

        const char *host_begin = nullptr;
        const char *host_end = nullptr;
        if (*port_begin == (':')) {
          // has a port
          host_begin = authority_begin;
          host_end = port_begin;

          // skip the colon
          port_begin++;

          port = std::string_view(port_begin, authority_end - port_begin);
        }
        else {
          // no port
          host_begin = authority_begin;
          host_end = authority_end;
        }

        // look for a user_info component
        const char *u_end = host_begin;
        for (; is_user_info_character(*u_end) && u_end != host_end; u_end++) {
        }

        if (*u_end == ('@')) {
          host_begin = u_end + 1;
          auto uinfo_begin = authority_begin;
          auto uinfo_end = u_end;
          uinfo = std::string_view(uinfo_begin, uinfo_end - uinfo_begin);
        }
        host = std::string_view(host_begin, host_end - host_begin);
      }
    }

    // if we see a path character or a slash, then the
    // if we see a slash, or any other legal path character, parse the path next
    if (*p == ('/') || is_path_character(*p)) {
      auto path_begin = p;

      // the path is delimited by a question-mark (query) or octothorpe
      // (fragment) or by EOS
      for (; *p != ('?') && *p != ('#') && *p != ('\0'); p++) {
        if (!is_path_character(*p)) {
          return false;
        }
      }
      auto path_end = p;
      path = std::string_view(path_begin, path_end - path_begin);
    }

    // if we see a ?, then the query is next
    if (*p == ('?')) {
      // skip over the question mark
      p++;
      auto query_begin = p;

      // the query is delimited by a '#' (fragment) or EOS
      for (; *p != ('#') && *p != ('\0'); p++) {
        if (!is_query_character(*p)) {
          return false;
        }
      }
      auto query_end = p;
      query = std::string_view(query_begin, query_end - query_begin);
    }

    // if we see a #, then the fragment is next
    if (*p == ('#')) {
      // skip over the hash mark
      p++;
      auto fragment_begin = p;

      // the fragment is delimited by EOS
      for (; *p != ('\0'); p++) {
        if (!is_fragment_character(*p)) {
          return false;
        }
      }
      auto fragment_end = p;
      fragment =
          std::string_view(fragment_begin, fragment_end - fragment_begin);
    }

    return true;
  }

  std::string get_host() const { return std::string(host); }

  std::string get_port() const {
    std::string port_str;
    if (is_ssl) {
      if (port.empty()) {
        port_str = "443";
      }
      else {
        port_str = std::string(port);
      }
    }
    else {
      if (port.empty()) {
        port_str = "80";
      }
      else {
        port_str = std::string(port);
      }
    }

    return port_str;
  }

  std::string get_path() const {
    if (path.empty())
      return "/";

    return std::string(path);
  }

  std::string get_query() const { return std::string(query); }
};

struct context {
  std::string host;
  std::string port;
  std::string path;
  std::string query;
  std::string body;
  http_method method = http_method::NIL;

  context() = default;
  context(const uri_t &u, http_method mthd)
      : host(u.get_host()),
        port(u.get_port()),
        path(u.get_path()),
        query(u.get_query()),
        method(mthd) {}
  context(const uri_t &u, http_method mthd, std::string b)
      : host(u.get_host()),
        port(u.get_port()),
        path(u.get_path()),
        query(u.get_query()),
        body(std::move(b)),
        method(mthd) {}
};
}  // namespace cinatra