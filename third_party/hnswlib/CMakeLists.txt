include (protobuf_helper)
set(SRCS_HNSWLIB_VMSDK
    bruteforce.h
    hnswalg.h
    hnswlib.h
    space_ip.h
    space_l2.h
    stop_condition.h
    visited_list_pool.h)

add_library(hnswlib OBJECT ${SRCS_HNSWLIB_VMSDK})
add_generated_proto_sources(hnswlib index)
target_include_directories(hnswlib INTERFACE .)
target_link_libraries(hnswlib PUBLIC gRPC::grpc++)
target_compile_definitions(hnswlib INTERFACE VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES)