# Proto libraries for coordinator (generated targets)
valkey_search_create_proto_grpc_library("src/coordinator/coordinator.proto"
                                        coordinator_grpc_proto)
valkey_search_create_proto_library("src/coordinator/coordinator.proto"
                                   coordinator_cc_proto)
target_link_libraries(coordinator_grpc_proto PUBLIC coordinator_cc_proto)

# =============================================================================
# libvs_coordinator: gRPC client/server, metadata management, converters
# =============================================================================
set(SRCS_VS_COORDINATOR
    ${CMAKE_CURRENT_LIST_DIR}/client.cc
    ${CMAKE_CURRENT_LIST_DIR}/grpc_suspender.cc
    ${CMAKE_CURRENT_LIST_DIR}/info_converter.cc
    ${CMAKE_CURRENT_LIST_DIR}/metadata_manager.cc
    ${CMAKE_CURRENT_LIST_DIR}/search_converter.cc
    ${CMAKE_CURRENT_LIST_DIR}/server.cc)

valkey_search_add_static_library(libvs_coordinator "${SRCS_VS_COORDINATOR}")

target_include_directories(libvs_coordinator PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_include_directories(libvs_coordinator PRIVATE ${CMAKE_BINARY_DIR})

# Depends on query (search_converter and server use search/predicate types)
target_link_libraries(libvs_coordinator PUBLIC libvs_query)

# Proto dependencies
target_link_libraries(libvs_coordinator PUBLIC coordinator_cc_proto)
target_link_libraries(libvs_coordinator PUBLIC coordinator_grpc_proto)

# Third-party
target_link_libraries(libvs_coordinator PUBLIC highwayhash)

# gRPC (platform-conditional)
if(APPLE)
  target_link_libraries(libvs_coordinator PUBLIC gRPC::grpc gRPC::grpc++)
else()
  target_link_libraries(libvs_coordinator PUBLIC ${GRPC_LIB})
endif()

# SDK dependencies
target_link_libraries(libvs_coordinator PUBLIC vmsdklib)
target_link_libraries(libvs_coordinator PUBLIC valkey_module)
