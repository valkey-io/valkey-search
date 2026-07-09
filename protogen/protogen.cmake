function(protogen_setup output_dir)
    set(PROTOGEN_BASE_INPUTS ${ARGN})

    set(PROTOGEN_PROTO_SRC "${PROTOGEN_BASE_INPUTS}")
    list(TRANSFORM PROTOGEN_PROTO_SRC PREPEND "${CMAKE_SOURCE_DIR}/")
    list(TRANSFORM PROTOGEN_PROTO_SRC APPEND ".proto")

    protobuf_generate(
        OUT_VAR PROTO_SRCS
        LANGUAGE cpp
        PROTOC_OUT_DIR ${output_dir}
        IMPORT_DIRS ${CMAKE_SOURCE_DIR}
        PROTOS
        ${PROTOGEN_PROTO_SRC}
    )

    protobuf_generate(
        OUT_VAR PROTO_GRPC_SRCS
        LANGUAGE grpc
        PROTOC_OUT_DIR ${output_dir}
        IMPORT_DIRS ${CMAKE_SOURCE_DIR}
        PROTOS
        ${PROTOGEN_PROTO_SRC}
        PLUGIN protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
        GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
    )

    set(PROTOGEN_OUTPUTS ${PROTO_SRCS} ${PROTO_GRPC_SRCS})
    add_custom_target(protogen ALL DEPENDS ${PROTOGEN_OUTPUTS})
endfunction()
