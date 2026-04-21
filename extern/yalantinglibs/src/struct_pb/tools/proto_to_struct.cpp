#include <google/protobuf/compiler/code_generator.h>
#include <google/protobuf/compiler/plugin.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include <cinttypes>
#include <fstream>
#include <iostream>
#include <memory>

#include "struct_code_generator.hpp"
#include "struct_token.hpp"

bool write_to_output(google::protobuf::io::ZeroCopyOutputStream* output,
                     const void* data, int size) {
  const uint8_t* in = reinterpret_cast<const uint8_t*>(data);
  int in_size = size;

  void* out;
  int out_size;

  while (true) {
    if (!output->Next(&out, &out_size)) {
      return false;
    }

    if (in_size <= out_size) {
      memcpy(out, in, in_size);
      output->BackUp(out_size - in_size);
      return true;
    }

    memcpy(out, in, out_size);
    in += out_size;
    in_size -= out_size;
  }
}

class struct_code_generator : public google::protobuf::compiler::CodeGenerator {
 public:
  virtual ~struct_code_generator() {}

  virtual bool Generate(const google::protobuf::FileDescriptor* file,
                        const std::string& parameter,
                        google::protobuf::compiler::GeneratorContext* context,
                        std::string* error) const override {
    std::string filename = file->name() + ".h";

    auto output = context->Open(filename);

    std::string package_name = file->package();

    bool enable_optional = false;
    bool enable_inherit = false;

    if (parameter.find("add_optional") != std::string::npos) {
      enable_optional = true;
    }

    if (parameter.find("enable_inherit") != std::string::npos) {
      enable_inherit = true;
    }

    // Use ZeroCopyOutputStream
    google::protobuf::io::ZeroCopyOutputStream* zero_copy_output = output;

    std::vector<struct_tokenizer> proto_module_info;
    std::vector<struct_enum> proto_enum_info;

    for (int i = 0; i < file->message_type_count(); ++i) {
      // struct name
      const google::protobuf::Descriptor* descriptor = file->message_type(i);

      for (int k = 0; k < descriptor->nested_type_count(); ++k) {
        struct_tokenizer tokenizer_nested;
        tokenizer_nested.clear();

        const google::protobuf::Descriptor* nested_type =
            descriptor->nested_type(k);
        tokenizer_nested.tokenizer(nested_type);
        proto_module_info.emplace_back(tokenizer_nested);
      }

      struct_enum enum_token;
      enum_token.clear();
      enum_token.get_enum_fields(descriptor);
      proto_enum_info.emplace_back(enum_token);

      struct_tokenizer tokenizer;
      tokenizer.clear();
      tokenizer.tokenizer(descriptor);
      proto_module_info.emplace_back(tokenizer);
    }

    std::string struct_header = code_generate_header(package_name);
    write_to_output(zero_copy_output, (const void*)struct_header.c_str(),
                    struct_header.size());

    // codegen struct enum
    for (auto enum_inst : proto_enum_info) {
      std::string enum_str = "";
      enum_str = code_generate_enum(enum_inst);
      write_to_output(zero_copy_output, (const void*)enum_str.c_str(),
                      enum_str.size());
    }

    // codegen struct
    std::vector<std::string> struct_module_contents;

    for (auto single_struct : proto_module_info) {
      std::string struct_default_str = "";
      std::string struct_constructor_str = "";
      std::string struct_body_str = "";
      std::string struct_macro_str = "";

      struct_default_str = code_generate_struct_default(
          single_struct.get_struct_name(), enable_inherit);
      struct_constructor_str = code_generate_struct_constructor(
          single_struct.get_struct_name(), single_struct.get_tokens(),
          enable_optional);
      struct_body_str =
          code_generate_body(single_struct.get_tokens(), enable_optional);
      struct_macro_str = code_generate_ylt_macro(
          single_struct.get_struct_name(), single_struct.get_tokens());

      write_to_output(zero_copy_output, (const void*)struct_default_str.c_str(),
                      struct_default_str.size());
      if (enable_inherit)
        write_to_output(zero_copy_output,
                        (const void*)struct_constructor_str.c_str(),
                        struct_constructor_str.size());
      write_to_output(zero_copy_output, (const void*)struct_body_str.c_str(),
                      struct_body_str.size());
      write_to_output(zero_copy_output, (const void*)struct_macro_str.c_str(),
                      struct_macro_str.size());
    }
    std::string namespace_last_part = "";
    namespace_last_part = code_generate_last_part();
    write_to_output(zero_copy_output, (const void*)namespace_last_part.c_str(),
                    namespace_last_part.size());

    delete zero_copy_output;
    return true;
  }
};

int main(int argc, char* argv[]) {
  google::protobuf::compiler::PluginMain(argc, argv,
                                         new struct_code_generator());
  return 0;
}