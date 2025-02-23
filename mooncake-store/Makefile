# Protoc compiler and gRPC plugin
PROTOC = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH = `which $(GRPC_CPP_PLUGIN)`

# Directories
PROTO_DIR = proto
SRC_DIR = src
INCLUDE_DIR = include

# Proto files
PROTO_FILES = $(PROTO_DIR)/master.proto

# Generated files
PROTO_GEN_SRC = $(SRC_DIR)/master.pb.cpp $(SRC_DIR)/master.grpc.pb.cpp
PROTO_GEN_INCLUDE = $(INCLUDE_DIR)/master.pb.h $(INCLUDE_DIR)/master.grpc.pb.h

# Default target
all: $(PROTO_GEN_SRC) $(PROTO_GEN_INCLUDE)

# Generate protobuf files
$(PROTO_GEN_INCLUDE) $(PROTO_GEN_SRC): $(PROTO_FILES)
	$(PROTOC) -I$(PROTO_DIR) --cpp_out=$(SRC_DIR) --grpc_out=$(SRC_DIR) --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<
	mv $(SRC_DIR)/master.pb.h $(INCLUDE_DIR)/
	mv $(SRC_DIR)/master.grpc.pb.h $(INCLUDE_DIR)/
	mv $(SRC_DIR)/master.pb.cc $(SRC_DIR)/master.pb.cpp
	mv $(SRC_DIR)/master.grpc.pb.cc $(SRC_DIR)/master.grpc.pb.cpp

# Clean generated files
clean:
	rm -f $(PROTO_GEN_SRC) $(PROTO_GEN_INCLUDE)

.PHONY: all clean
