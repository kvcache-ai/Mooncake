#pragma once

#include <cassert>
#include <map>
#include <system_error>

namespace iguana {

class iguana_category : public std::error_category {
 public:
  virtual const char *name() const noexcept override {
    return "iguana::category";
  }
  virtual std::string message(int err_val) const override {
    for (auto &pair : err_map_) {
      if (pair.second == err_val) {
        return pair.first;
      }
    }

    return "unrecognized error";
  }

  int add_message(const std::string &msg) {
    if (auto it = err_map_.find(msg); it != err_map_.end()) {
      return it->second;
    }
    else {
      err_++;
      err_map_.emplace(msg, err_);
      return err_;
    }
  }

  int err_ = 0;
  std::map<std::string, int> err_map_;
};

inline iguana::iguana_category &category() {
  static iguana::iguana_category instance;
  return instance;
}

inline std::error_code make_error_code(const std::string data = "") {
  assert(!data.empty());

  auto err = iguana::category().add_message(data);
  return std::error_code(err, iguana::category());
}

// dom parse error code
enum class dom_errc {
  ok = 0,
  wrong_type,
};

class iguana_dom_category : public std::error_category {
 public:
  virtual const char *name() const noexcept override {
    return "iguana::dom_category";
  }
  virtual std::string message(int err_val) const override {
    switch (static_cast<dom_errc>(err_val)) {
      case dom_errc::ok:
        return "ok";
      case dom_errc::wrong_type: {
        auto it = detail_msg_map_.find(dom_errc::wrong_type);
        if (it != detail_msg_map_.end()) {
          return std::string("wrong type, ")
              .append("real type is ")
              .append(it->second);
        }
        else {
          return "wrong type";
        }
      }
      default:
        return "(unrecognized error)";
    }
  }

  //    private:
  std::map<iguana::dom_errc, std::string> detail_msg_map_;
};

inline iguana::iguana_dom_category &dom_category() {
  static iguana::iguana_dom_category instance;
  return instance;
}

inline std::error_code make_error_code(iguana::dom_errc err,
                                       const std::string &error_msg) {
  auto &instance = iguana::dom_category();
  if (!error_msg.empty()) {
    instance.detail_msg_map_.emplace(err, error_msg);
  }

  return std::error_code((int)err, instance);
}
}  // namespace iguana
