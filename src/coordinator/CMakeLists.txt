# Protocol buffer libraries
valkey_search_create_proto_grpc_library("src/coordinator/coordinator.proto"
                                        coordinator_grpc_proto)

valkey_search_create_proto_library("src/coordinator/coordinator.proto"
                                   coordinator_cc_proto)

target_link_libraries(coordinator_grpc_proto PUBLIC coordinator_cc_proto)

# Single base library that consolidates ALL common dependencies
add_library(coordinator_base INTERFACE)
target_link_libraries(coordinator_base INTERFACE coordinator_cc_proto)
target_link_libraries(coordinator_base INTERFACE coordinator_grpc_proto)
target_link_libraries(coordinator_base INTERFACE managed_pointers)
target_link_libraries(coordinator_base INTERFACE type_conversions)
target_link_libraries(coordinator_base INTERFACE status_macros)
target_link_libraries(coordinator_base INTERFACE valkey_module)
target_link_libraries(coordinator_base INTERFACE log)
target_link_libraries(coordinator_base INTERFACE metrics)
target_link_libraries(coordinator_base INTERFACE latency_sampler)
target_link_libraries(coordinator_base INTERFACE index_schema)
target_link_libraries(coordinator_base INTERFACE index_base)
target_link_libraries(coordinator_base INTERFACE numeric)
target_link_libraries(coordinator_base INTERFACE tag)
target_link_libraries(coordinator_base INTERFACE search)
target_link_libraries(coordinator_base INTERFACE predicate_header)
target_link_libraries(coordinator_base INTERFACE schema_manager)
target_link_libraries(coordinator_base INTERFACE rdb_serialization)
target_link_libraries(coordinator_base INTERFACE utils)
target_link_libraries(coordinator_base INTERFACE highwayhash)
target_link_libraries(coordinator_base INTERFACE vector_base)
target_link_libraries(coordinator_base INTERFACE response_generator)
target_link_libraries(coordinator_base INTERFACE thread_pool)
if(APPLE)
  target_link_libraries(coordinator_base INTERFACE gRPC::grpc gRPC::grpc++)
  target_link_libraries(coordinator_base INTERFACE absl::base)
else()
  target_link_libraries(coordinator_base INTERFACE ${GRPC_LIB})
endif()

# 1. Utility - header-only utilities
set(SRCS_UTIL ${CMAKE_CURRENT_LIST_DIR}/util.h)

add_library(util INTERFACE ${SRCS_UTIL})
target_include_directories(util INTERFACE ${CMAKE_CURRENT_LIST_DIR})

# 2. gRPC Suspender
set(SRCS_GRPC_SUSPENDER ${CMAKE_CURRENT_LIST_DIR}/grpc_suspender.cc
                        ${CMAKE_CURRENT_LIST_DIR}/grpc_suspender.h)

valkey_search_add_static_library(grpc_suspender "${SRCS_GRPC_SUSPENDER}")
target_include_directories(grpc_suspender PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(grpc_suspender PUBLIC coordinator_base)

# 3. Client
set(SRCS_CLIENT ${CMAKE_CURRENT_LIST_DIR}/client.cc
                ${CMAKE_CURRENT_LIST_DIR}/client.h)

valkey_search_add_static_library(client "${SRCS_CLIENT}")
target_include_directories(client PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_include_directories(client PRIVATE ${CMAKE_BINARY_DIR})
target_link_libraries(client PUBLIC coordinator_base)

# 4. Client Pool
set(SRCS_CLIENT_POOL ${CMAKE_CURRENT_LIST_DIR}/client_pool.h)

add_library(client_pool INTERFACE ${SRCS_CLIENT_POOL})
target_include_directories(client_pool INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(client_pool INTERFACE coordinator_base)
target_link_libraries(client_pool INTERFACE client)

# 5. Info Converter
set(SRCS_INFO_CONVERTER ${CMAKE_CURRENT_LIST_DIR}/info_converter.cc
                        ${CMAKE_CURRENT_LIST_DIR}/info_converter.h)

valkey_search_add_static_library(info_converter "${SRCS_INFO_CONVERTER}")
target_include_directories(info_converter PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(info_converter PUBLIC coordinator_base)

# 6. Search Converter
set(SRCS_SEARCH_CONVERTER ${CMAKE_CURRENT_LIST_DIR}/search_converter.cc
                          ${CMAKE_CURRENT_LIST_DIR}/search_converter.h)

valkey_search_add_static_library(search_converter "${SRCS_SEARCH_CONVERTER}")
target_include_directories(search_converter PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(search_converter PUBLIC coordinator_base)

# 7. Metadata Manager
set(SRCS_METADATA_MANAGER ${CMAKE_CURRENT_LIST_DIR}/metadata_manager.cc
                          ${CMAKE_CURRENT_LIST_DIR}/metadata_manager.h)

valkey_search_add_static_library(metadata_manager "${SRCS_METADATA_MANAGER}")
target_include_directories(metadata_manager PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(metadata_manager PUBLIC coordinator_base)
target_link_libraries(metadata_manager PUBLIC client_pool)
target_link_libraries(metadata_manager PUBLIC util)

# 8. Server
set(SRCS_SERVER ${CMAKE_CURRENT_LIST_DIR}/server.cc
                ${CMAKE_CURRENT_LIST_DIR}/server.h)

valkey_search_add_static_library(server "${SRCS_SERVER}")
target_include_directories(server PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_include_directories(server PRIVATE ${CMAKE_BINARY_DIR})
target_link_libraries(server PUBLIC coordinator_base)
target_link_libraries(server PUBLIC metadata_manager)
target_link_libraries(server PUBLIC search_converter)
target_link_libraries(server PUBLIC util)
