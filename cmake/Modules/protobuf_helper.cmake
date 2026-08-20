function (add_generated_proto_sources target fname)
    file (RELATIVE_PATH __basedir
            "${CMAKE_SOURCE_DIR}"
            "${CMAKE_CURRENT_SOURCE_DIR}"
    )
    set (__fbase "${VALKEY_PROTOGEN_BINARY_DIR}/${__basedir}/${fname}")
    foreach (suffix IN ITEMS .pb.cc .pb.h .grpc.pb.cc .grpc.pb.h)
        set(__fname "${__fbase}${suffix}")
        target_sources(${target} PRIVATE "${__fname}")
        set_source_files_properties(${__fname} PROPERTIES GENERATED TRUE)
    endforeach ()
    target_include_directories(${target} PRIVATE ${VALKEY_PROTOGEN_BINARY_DIR})
    add_dependencies(${target} protogen)
endfunction()