#pragma once
#include <string>

#include "struct_token.hpp"

char parameter_value[27] = "abcdefghijklmnopqrstuvwxyz";

std::string code_generate_header(const std::string &package_name) {
  std::string result = "#pragma once\n#include <ylt/struct_pb.hpp>\n\n";

  result.append("namespace ");
  result.append(package_name);
  result.append(" {\n\n");
  return result;
}

std::string code_generate_struct_default(const std::string &struct_name,
                                         bool enable_inherit) {
  std::string result = "struct ";
  result.append(struct_name);

  if (enable_inherit) {
    result.append(" : public iguana::base_impl<");
    result.append(struct_name);
    result.append("> ");
  }

  result.append(" {\n");

  if (enable_inherit) {
    result.append("\t");
    result.append(struct_name);
    result.append("() = default;\n\t");
  }
  return result;
}

std::string code_generate_struct_constructor(
    const std::string &struct_name, const std::vector<struct_token> lists,
    bool add_optional) {
  std::string result = struct_name + "(";
  int i = 0;
  int list_size = lists.size() - 1;
  for (auto it = lists.begin(); it != lists.end(); it++) {
    if (it->type == struct_token_type::pod) {
      if (it->lable == lable_type::lable_repeated) {
        result.append("std::vector<");
        result.append(it->type_name);
        result.append("> ");
        result += parameter_value[i];
      }
      else {
        result.append(it->type_name);
        result.append(" ");
        result += parameter_value[i];
      }
      if (i != list_size)
        result.append(", ");
      i++;
    }
    else if (it->type == struct_token_type::proto_string) {
      if (it->lable == lable_type::lable_repeated) {
        if (add_optional) {
          result.append("std::optional<");
        }
        result.append("std::vector<std::string>");
        if (add_optional) {
          result.append("> ");
        }
        result += parameter_value[i];
      }
      else {
        if (add_optional) {
          result.append("std::optional<");
        }

        result.append("std::");
        result.append(it->type_name);
        if (add_optional) {
          result.append(">");
        }
        result.append(" ");
        result += parameter_value[i];
      }
      if (i != list_size)
        result.append(", ");
      i++;
    }
    else if (it->type == struct_token_type::enum_type) {
      // enum no repeated type
      result.append(it->type_name);
      result.append(" ");
      result += parameter_value[i];
      if (i != list_size)
        result.append(", ");
      i++;
    }
    else {
      if (it->lable == lable_type::lable_repeated) {
        if (add_optional) {
          result.append("std::optional<");
        }

        result.append("std::vector<");

        result.append(it->type_name);
        result.append(">");

        if (add_optional) {
          result.append("> ");
        }

        result += parameter_value[i];
      }
      else {
        if (add_optional) {
          result.append("std::optional<");
        }
        result.append(it->type_name);
        if (add_optional) {
          result.append(">");
        }
        result.append(" ");
        result += parameter_value[i];
      }
      if (i != list_size)
        result.append(", ");
      i++;
    }
  }
  result.append(") : ");

  int j = 0;
  for (auto ll : lists) {
    if (ll.type == struct_token_type::pod ||
        ll.type == struct_token_type::message ||
        ll.type == struct_token_type::enum_type) {
      result.append(ll.var_name);
      result.append("(");
      if (ll.lable == lable_type::lable_repeated) {
        result.append("std::move(");
        result += parameter_value[j];
        result.append(")");
        if (j != list_size)
          result.append("), ");
        else
          result.append(") {}");
      }
      else {
        result += parameter_value[j];
        if (j != list_size)
          result.append("), ");
        else
          result.append(") {}");
      }
    }
    else if (ll.type == struct_token_type::proto_string) {
      result.append(ll.var_name);
      result.append("(");
      result.append("std::move(");
      result += parameter_value[j];
      result.append(")");
      if (j != list_size)
        result.append("), ");
      else
        result.append(") {}");
    }
    j++;
  }
  result.append("\n");
  return result;
}

std::string code_generate_body(const std::vector<struct_token> &lists,
                               bool add_optional) {
  std::string result;
  for (auto ll : lists) {
    result.append("\t");
    if (ll.lable == lable_type::lable_repeated) {
      if (ll.type == struct_token_type::proto_string) {
        if (add_optional) {
          result.append("std::optional<");
        }

        result.append("std::vector<std::string>");
        if (add_optional) {
          result.append("> ");
        }
        result.append(ll.var_name);
        result.append(";\n");
      }
      else {
        if (ll.type == struct_token_type::message && add_optional) {
          result.append("std::optional<");
        }
        result.append("std::vector<");
        result.append(ll.type_name);
        result.append(">");
        if (ll.type == struct_token_type::message && add_optional) {
          result.append("> ");
        }
        result.append(ll.var_name);
        result.append(";\n");
      }
    }
    else {
      if (ll.type != struct_token_type::pod &&
          ll.type != struct_token_type::enum_type && add_optional) {
        result.append("std::optional<");
      }

      if (ll.type == struct_token_type::proto_string)
        result.append("std::");
      result.append(ll.type_name);
      if (ll.type != struct_token_type::pod &&
          ll.type != struct_token_type::enum_type && add_optional) {
        result.append(">");
      }
      result.append(" ");
      result.append(ll.var_name);
      result.append(";\n");
    }
  }
  result.append("};\n");

  return result;
}

std::string code_generate_ylt_macro(const std::string &struct_name,
                                    const std::vector<struct_token> &lists) {
  std::string result = "YLT_REFL(";
  result.append(struct_name);
  result.append(", ");
  int i = 0;
  int list_size = lists.size() - 1;
  for (auto ll : lists) {
    if (i != list_size) {
      result.append(ll.var_name);
      result.append(", ");
    }
    else {
      result.append(ll.var_name);
    }
    i++;
  }
  result.append(");\n\n");
  return result;
}

std::string code_generate_enum(const struct_enum &enum_inst) {
  std::string result = "enum class ";
  if (enum_inst.enum_name_.empty())
    return "";
  result.append(enum_inst.enum_name_);
  result.append(" {\n");
  if (enum_inst.fields_.size() == 0) {
    result.append("}\n");
  }
  else {
    for (auto i : enum_inst.fields_) {
      result.append("\t");
      result.append(i.first);
      result.append(" = ");
      result.append(std::to_string(i.second));
      result.append(",\n");
    }
    result.append("};\n\n");
  }
  return result;
}

std::string code_generate_last_part() { return "}"; }