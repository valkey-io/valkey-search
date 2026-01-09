valkey_search_create_proto_grpc_library("src/coordinator/coordinator.proto"
                                        coordinator_grpc_proto)

# Create the proto code (no grpc)
valkey_search_create_proto_library("src/coordinator/coordinator.proto"
                                   coordinator_cc_proto)

target_link_libraries(coordinator_grpc_proto PUBLIC coordinator_cc_proto)
set(SRCS_SERVER ${CMAKE_CURRENT_LIST_DIR}/server.cc
                ${CMAKE_CURRENT_LIST_DIR}/server.h)

valkey_search_add_static_library(server "${SRCS_SERVER}")
target_include_directories(server PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_include_directories(server PRIVATE ${CMAKE_BINARY_DIR})
target_link_libraries(server PUBLIC coordinator_cc_proto)
target_link_libraries(server PUBLIC grpc_suspender)
target_link_libraries(server PUBLIC metadata_manager)
target_link_libraries(server PUBLIC search_converter)
target_link_libraries(server PUBLIC util)
target_link_libraries(server PUBLIC metrics)
target_link_libraries(server PUBLIC vector_base)
target_link_libraries(server PUBLIC response_generator)
target_link_libraries(server PUBLIC search)
target_link_libraries(server PUBLIC vmsdklib)
target_link_libraries(server PUBLIC valkey_module)
target_link_libraries(server PUBLIC coordinator_grpc_proto)
if(APPLE)
  target_link_libraries(server PUBLIC gRPC::grpc gRPC::grpc++)
else()
  target_link_libraries(server PUBLIC ${GRPC_LIB})
endif()

set(SRCS_CLIENT ${CMAKE_CURRENT_LIST_DIR}/client.cc
                ${CMAKE_CURRENT_LIST_DIR}/client.h)

valkey_search_add_static_library(client "${SRCS_CLIENT}")
target_include_directories(client PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_include_directories(client PRIVATE ${CMAKE_BINARY_DIR})
target_link_libraries(client PUBLIC coordinator_cc_proto)
target_link_libraries(client PUBLIC grpc_suspender)
target_link_libraries(client PUBLIC metrics)
target_link_libraries(client PUBLIC vmsdklib)
target_link_libraries(client PUBLIC coordinator_grpc_proto)
if(APPLE)
  target_link_libraries(client PUBLIC gRPC::grpc gRPC::grpc++)
else()
  target_link_libraries(client PUBLIC ${GRPC_LIB})
endif()

set(SRCS_CLIENT_POOL ${CMAKE_CURRENT_LIST_DIR}/client_pool.h)

add_library(client_pool INTERFACE ${SRCS_CLIENT_POOL})
target_include_directories(client_pool INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(client_pool INTERFACE client)
target_link_libraries(client_pool INTERFACE vmsdklib)

set(SRCS_UTIL ${CMAKE_CURRENT_LIST_DIR}/util.h)

add_library(util INTERFACE ${SRCS_UTIL})
target_include_directories(util INTERFACE ${CMAKE_CURRENT_LIST_DIR})

set(SRCS_METADATA_MANAGER ${CMAKE_CURRENT_LIST_DIR}/metadata_manager.cc
                          ${CMAKE_CURRENT_LIST_DIR}/metadata_manager.h)

valkey_search_add_static_library(metadata_manager "${SRCS_METADATA_MANAGER}")
target_include_directories(metadata_manager PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(metadata_manager PUBLIC client_pool)
target_link_libraries(metadata_manager PUBLIC coordinator_cc_proto)
target_link_libraries(metadata_manager PUBLIC util)
target_link_libraries(metadata_manager PUBLIC rdb_serialization)
target_link_libraries(metadata_manager PUBLIC vmsdklib)
target_link_libraries(metadata_manager PUBLIC valkey_module)
target_link_libraries(metadata_manager PUBLIC highwayhash)

set(SRCS_SEARCH_CONVERTER
    ${CMAKE_CURRENT_LIST_DIR}/search_converter.cc
    ${CMAKE_CURRENT_LIST_DIR}/search_converter.h
    ${CMAKE_CURRENT_LIST_DIR}/info_converter.cc
    ${CMAKE_CURRENT_LIST_DIR}/info_converter.h)

valkey_search_add_static_library(search_converter "${SRCS_SEARCH_CONVERTER}")
target_include_directories(search_converter PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(search_converter PUBLIC coordinator_cc_proto)
target_link_libraries(search_converter PUBLIC index_schema)
target_link_libraries(search_converter PUBLIC schema_manager)
target_link_libraries(search_converter PUBLIC index_base)
target_link_libraries(search_converter PUBLIC numeric)
target_link_libraries(search_converter PUBLIC tag)
target_link_libraries(search_converter PUBLIC predicate_header)
target_link_libraries(search_converter PUBLIC search)
target_link_libraries(search_converter PUBLIC vmsdklib)

set(SRCS_GRPC_SUSPENDER ${CMAKE_CURRENT_LIST_DIR}/grpc_suspender.cc
                        ${CMAKE_CURRENT_LIST_DIR}/grpc_suspender.h)

valkey_search_add_static_library(grpc_suspender "${SRCS_GRPC_SUSPENDER}")
target_include_directories(grpc_suspender PUBLIC ${CMAKE_CURRENT_LIST_DIR})
if(APPLE)
  target_link_libraries(grpc_suspender PUBLIC absl::base)
else()
  target_link_libraries(grpc_suspender PUBLIC ${GRPC_LIB})
endif()
