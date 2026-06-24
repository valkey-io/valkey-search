# generate protobuf files here...

set(PROTOGEN_BASE_INPUTS
        src/coordinator/coordinator
        src/rdb_section
        src/index_schema
        third_party/hnswlib/index
)

set(PROTOGEN_PROTO_SRC "${PROTOGEN_BASE_INPUTS}")
list(TRANSFORM PROTOGEN_PROTO_SRC PREPEND "${CMAKE_SOURCE_DIR}/")
list(TRANSFORM PROTOGEN_PROTO_SRC APPEND ".proto")

protobuf_generate(
        OUT_VAR PROTO_SRCS
        LANGUAGE cpp
        PROTOC_OUT_DIR ${VALKEY_PROTOGEN_BINARY_DIR}
        IMPORT_DIRS ${CMAKE_SOURCE_DIR}
        PROTOS
        ${PROTOGEN_PROTO_SRC}
)

protobuf_generate(
        OUT_VAR PROTO_GRPC_SRCS
        LANGUAGE grpc
        PROTOC_OUT_DIR ${VALKEY_PROTOGEN_BINARY_DIR}
        IMPORT_DIRS ${CMAKE_SOURCE_DIR}
        PROTOS
        ${PROTOGEN_PROTO_SRC}
        PLUGIN protoc-gen-grpc=\$<TARGET_FILE:gRPC::grpc_cpp_plugin>
        GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
)

set(PROTOGEN_OUTPUTS ${PROTO_SRCS} ${PROTO_GRPC_SRCS})
add_custom_target(protogen ALL DEPENDS ${PROTOGEN_OUTPUTS})