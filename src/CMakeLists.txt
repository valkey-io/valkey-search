add_subdirectory(coordinator)
add_subdirectory(commands)
add_subdirectory(indexes)
add_subdirectory(expr)
add_subdirectory(query)
add_subdirectory(utils)

set(SRCS_MODULE_LOADER ${CMAKE_CURRENT_LIST_DIR}/module_loader.cc)

# This is the module target
valkey_search_add_shared_library(libsearch ${SRCS_MODULE_LOADER})
target_include_directories(libsearch PUBLIC ${CMAKE_CURRENT_LIST_DIR})
if(UNIX AND NOT APPLE)
  target_link_options(
    libsearch PRIVATE
    "LINKER:--version-script=${CMAKE_SOURCE_DIR}/vmsdk/versionscript.lds")
endif()

target_link_libraries(libsearch PUBLIC keyspace_event_manager)
target_link_libraries(libsearch PUBLIC valkey_search)
target_link_libraries(libsearch PUBLIC commands)
target_link_libraries(libsearch PUBLIC vmsdklib)
target_link_libraries(libsearch PUBLIC valkey_module)
target_link_libraries(libsearch PUBLIC server_events)
target_link_libraries(libsearch PUBLIC expr)

list(APPEND THIRD_PARTY_LIBS coordinator_grpc_proto)
list(APPEND THIRD_PARTY_LIBS coordinator_cc_proto)

valkey_search_create_proto_library("src/rdb_section.proto"
                                   "rdb_section_cc_proto")
target_link_libraries(rdb_section_cc_proto PUBLIC index_schema_cc_proto)
target_link_libraries(rdb_section_cc_proto PUBLIC coordinator_cc_proto)

if(UNIX AND NOT APPLE)
  # Wrap ${THIRD_PARTY_LIBS} with "--start/--end group"
  target_link_options(libsearch PRIVATE -Wl,--start-group)
endif()
target_link_libraries(libsearch PRIVATE ${THIRD_PARTY_LIBS})

# Remove the "lib" prefix from the module
set_target_properties(libsearch PROPERTIES PREFIX "")

if(UNIX AND NOT APPLE)
  target_link_options(libsearch PRIVATE -Wl,--allow-multiple-definition)
endif()

if(UNIX AND NOT APPLE)
  # Append -Wl,--end-group as the LAST argument
  target_link_libraries(libsearch PRIVATE lib_to_add_end_group_flag)
endif()

set(SRCS_VALKEY_SEARCH
    ${CMAKE_CURRENT_LIST_DIR}/attribute.cc
    ${CMAKE_CURRENT_LIST_DIR}/attribute.h
    ${CMAKE_CURRENT_LIST_DIR}/valkey_search.cc
    ${CMAKE_CURRENT_LIST_DIR}/valkey_search.h
    ${CMAKE_CURRENT_LIST_DIR}/valkey_search_options.cc
    ${CMAKE_CURRENT_LIST_DIR}/valkey_search_options.h)

valkey_search_add_static_library(valkey_search "${SRCS_VALKEY_SEARCH}")
target_include_directories(valkey_search PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(valkey_search PUBLIC attribute_data_type)
target_link_libraries(valkey_search PUBLIC index_schema)
target_link_libraries(valkey_search PUBLIC metrics)
target_link_libraries(valkey_search PUBLIC schema_manager)
target_link_libraries(valkey_search PUBLIC vector_externalizer)
target_link_libraries(valkey_search PUBLIC client_pool)
target_link_libraries(valkey_search PUBLIC grpc_suspender)
target_link_libraries(valkey_search PUBLIC metadata_manager)
target_link_libraries(valkey_search PUBLIC server)
target_link_libraries(valkey_search PUBLIC util)
target_link_libraries(valkey_search PUBLIC string_interning)
target_link_libraries(valkey_search PUBLIC vmsdklib)
target_link_libraries(valkey_search PUBLIC valkey_module)
target_link_libraries(valkey_search PUBLIC acl)

set(SRCS_KEYSPACE_EVENT_MANAGER
    ${CMAKE_CURRENT_LIST_DIR}/keyspace_event_manager.cc
    ${CMAKE_CURRENT_LIST_DIR}/keyspace_event_manager.h)

valkey_search_add_static_library(keyspace_event_manager
                                 "${SRCS_KEYSPACE_EVENT_MANAGER}")
target_include_directories(keyspace_event_manager
                           PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(keyspace_event_manager PUBLIC attribute_data_type)
target_link_libraries(keyspace_event_manager PUBLIC vector_externalizer)
target_link_libraries(keyspace_event_manager PUBLIC patricia_tree)
target_link_libraries(keyspace_event_manager PUBLIC vmsdklib)
target_link_libraries(keyspace_event_manager PUBLIC valkey_module)

set(SRCS_ATTRIBUTE ${CMAKE_CURRENT_LIST_DIR}/attribute.h)

add_library(attribute INTERFACE ${SRCS_ATTRIBUTE})
target_include_directories(attribute INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(attribute INTERFACE index_schema_cc_proto)
target_link_libraries(attribute INTERFACE index_base)
target_link_libraries(attribute INTERFACE vmsdklib)
target_link_libraries(attribute INTERFACE valkey_module)

set(SRCS_METRICS ${CMAKE_CURRENT_LIST_DIR}/metrics.h)

add_library(metrics INTERFACE ${SRCS_METRICS})
target_include_directories(metrics INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(metrics INTERFACE vmsdklib)

set(SRCS_INDEX_SCHEMA ${CMAKE_CURRENT_LIST_DIR}/index_schema.cc
                      ${CMAKE_CURRENT_LIST_DIR}/index_schema.h)

valkey_search_add_static_library(index_schema "${SRCS_INDEX_SCHEMA}")
target_include_directories(index_schema PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(index_schema PUBLIC attribute)
target_link_libraries(index_schema PUBLIC attribute_data_type)
target_link_libraries(index_schema PUBLIC vmsdklib)
target_link_libraries(index_schema PUBLIC index_schema_cc_proto)
target_link_libraries(index_schema PUBLIC keyspace_event_manager)
target_link_libraries(index_schema PUBLIC metrics)
target_link_libraries(index_schema PUBLIC rdb_serialization)
target_link_libraries(index_schema PUBLIC vector_externalizer)
target_link_libraries(index_schema PUBLIC index_base)
target_link_libraries(index_schema PUBLIC numeric)
target_link_libraries(index_schema PUBLIC tag)
target_link_libraries(index_schema PUBLIC text)

target_link_libraries(index_schema PUBLIC vector_base)
target_link_libraries(index_schema PUBLIC vector_flat)
target_link_libraries(index_schema PUBLIC vector_hnsw)
target_link_libraries(index_schema PUBLIC string_interning)
target_link_libraries(index_schema PUBLIC valkey_module)

set(SRCS_ATTRIBUTE_DATA_TYPE ${CMAKE_CURRENT_LIST_DIR}/attribute_data_type.cc
                             ${CMAKE_CURRENT_LIST_DIR}/attribute_data_type.h)

valkey_search_add_static_library(attribute_data_type
                                 "${SRCS_ATTRIBUTE_DATA_TYPE}")
target_include_directories(attribute_data_type PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(attribute_data_type PUBLIC index_schema_cc_proto)
target_link_libraries(attribute_data_type PUBLIC valkey_module)
target_link_libraries(attribute_data_type PUBLIC vmsdklib)

set(SRCS_RDB_SERIALIZATION ${CMAKE_CURRENT_LIST_DIR}/rdb_serialization.cc
                           ${CMAKE_CURRENT_LIST_DIR}/rdb_serialization.h)

valkey_search_add_static_library(rdb_serialization ${SRCS_RDB_SERIALIZATION})
target_include_directories(rdb_serialization PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(rdb_serialization PUBLIC iostream)
target_link_libraries(rdb_serialization PUBLIC valkey_module)
target_link_libraries(rdb_serialization PUBLIC vmsdklib)
target_link_libraries(rdb_serialization PUBLIC rdb_section_cc_proto)

set(SRCS_SCHEMA_MANAGER ${CMAKE_CURRENT_LIST_DIR}/schema_manager.cc
                        ${CMAKE_CURRENT_LIST_DIR}/schema_manager.h)

valkey_search_add_static_library(schema_manager "${SRCS_SCHEMA_MANAGER}")
target_include_directories(schema_manager PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(schema_manager PUBLIC vmsdklib)
target_link_libraries(schema_manager PUBLIC index_schema)
target_link_libraries(schema_manager PUBLIC index_schema_cc_proto)
target_link_libraries(schema_manager PUBLIC metrics)
target_link_libraries(schema_manager PUBLIC rdb_serialization)
target_link_libraries(schema_manager PUBLIC vector_externalizer)
target_link_libraries(schema_manager PUBLIC coordinator_cc_proto)
target_link_libraries(schema_manager PUBLIC metadata_manager)
target_link_libraries(schema_manager PUBLIC valkey_module)

set(SRCS_SERVER_EVENTS ${CMAKE_CURRENT_LIST_DIR}/server_events.cc
                       ${CMAKE_CURRENT_LIST_DIR}/server_events.h)

valkey_search_add_static_library(server_events "${SRCS_SERVER_EVENTS}")
target_include_directories(server_events PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(server_events PUBLIC schema_manager)
target_link_libraries(server_events PUBLIC valkey_search)
target_link_libraries(server_events PUBLIC metadata_manager)
target_link_libraries(server_events PUBLIC valkey_module)

set(SRCS_VECTOR_EXTERNALIZER ${CMAKE_CURRENT_LIST_DIR}/vector_externalizer.cc
                             ${CMAKE_CURRENT_LIST_DIR}/vector_externalizer.h)

valkey_search_add_static_library(vector_externalizer
                                 "${SRCS_VECTOR_EXTERNALIZER}")
target_include_directories(vector_externalizer PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(vector_externalizer PUBLIC attribute_data_type)
target_link_libraries(vector_externalizer PUBLIC index_schema_cc_proto)
target_link_libraries(vector_externalizer PUBLIC lru)
target_link_libraries(vector_externalizer PUBLIC string_interning)
target_link_libraries(vector_externalizer PUBLIC vmsdklib)
target_link_libraries(vector_externalizer PUBLIC valkey_module)

set(SRCS_ACL ${CMAKE_CURRENT_LIST_DIR}/acl.cc ${CMAKE_CURRENT_LIST_DIR}/acl.h)
valkey_search_add_static_library(acl "${SRCS_ACL}")
target_include_directories(acl PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(acl PUBLIC index_schema_cc_proto)
