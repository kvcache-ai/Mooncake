#pragma once
#include <google/protobuf/descriptor.h>

#include <string>
#include <vector>

enum class struct_token_type {
  pod,
  proto_string,
  message,
  enum_type,
  null_type
};

enum class lable_type {
  lable_optional,
  lable_required,
  lable_repeated,
  lable_null
};

struct struct_token {
  struct_token() {
    this->var_name = "";
    this->type_name = "";
    this->type = struct_token_type::null_type;
    this->lable = lable_type::lable_null;
  }

  void clear();
  std::string var_name;
  std::string type_name;
  struct_token_type type;
  lable_type lable;
};

class struct_tokenizer {
 public:
  struct_tokenizer() { clear(); }

  void tokenizer(const google::protobuf::Descriptor* descriptor) {
    struct_name_ = descriptor->name();
    for (int j = 0; j < descriptor->field_count(); ++j) {
      struct_token token = {};
      const google::protobuf::FieldDescriptor* field = descriptor->field(j);

      token.var_name = field->name();
      if (field->type() == google::protobuf::FieldDescriptor::TYPE_MESSAGE) {
        token.type_name = field->message_type()->name();
        token.type = struct_token_type::message;
      }
      else if (field->type() ==
               google::protobuf::FieldDescriptor::TYPE_STRING) {
        token.type_name = field->type_name();
        token.type = struct_token_type::proto_string;
        // std::cout << "string var anme is: " << token.var_name << std::endl;
      }
      else {
        token.type_name = field->type_name();
        if (token.type_name == "bytes") {
          token.type = struct_token_type::proto_string;
          token.type_name = "string";
        }
        else if (token.type_name == "enum") {
          const google::protobuf::EnumDescriptor* enum_desc =
              field->enum_type();
          token.type_name = enum_desc->name();
          token.type = struct_token_type::enum_type;
        }
        else {
          token.type = struct_token_type::pod;
          if (token.type_name.find("int") != std::string::npos)
            token.type_name += "_t";
        }
      }

      if (field->label() ==
          google::protobuf::FieldDescriptor::Label::LABEL_REPEATED)
        token.lable = lable_type::lable_repeated;
      else if (field->label() ==
               google::protobuf::FieldDescriptor::Label::LABEL_OPTIONAL)
        token.lable = lable_type::lable_optional;
      else if (field->label() ==
               google::protobuf::FieldDescriptor::Label::LABEL_REQUIRED)
        token.lable = lable_type::lable_required;

      token_lists_.emplace_back(token);
    }
  }

  void clear() {
    token_lists_.clear();
    struct_name_ = "";
  }

  std::vector<struct_token>& get_tokens() { return token_lists_; }
  std::string& get_struct_name() { return struct_name_; }

 private:
  std::vector<struct_token> token_lists_;
  std::string struct_name_;  // struct name
};

struct struct_enum {
  struct_enum() { clear(); }

  void clear() {
    this->enum_name_ = "";
    this->fields_.clear();
  }

  void get_enum_fields(const google::protobuf::Descriptor* descriptor) {
    for (int e = 0; e < descriptor->enum_type_count(); ++e) {
      const google::protobuf::EnumDescriptor* enum_desc =
          descriptor->enum_type(e);
      enum_name_ = enum_desc->name();
      for (int v = 0; v < enum_desc->value_count(); ++v) {
        const google::protobuf::EnumValueDescriptor* value_desc =
            enum_desc->value(v);
        fields_.push_back({value_desc->name(), value_desc->number()});
      }
    }
  }
  std::string enum_name_;
  std::vector<std::pair<std::string, int>> fields_;
};