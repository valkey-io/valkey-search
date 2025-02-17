add_subdirectory(coordinator)
add_subdirectory(utils)
add_subdirectory(commands)
add_subdirectory(query)
add_subdirectory(indexes)
set(SRCS_MODULE_LOADER ${CMAKE_CURRENT_LIST_DIR}/module_loader.cc)

# This is the module target
valkey_search_add_shared_library(valkeysearch ${SRCS_MODULE_LOADER})
target_include_directories(valkeysearch PUBLIC ${CMAKE_CURRENT_LIST_DIR})

target_link_libraries(valkeysearch PUBLIC keyspace_event_manager)
target_link_libraries(valkeysearch PUBLIC valkey_search)
target_link_libraries(valkeysearch PUBLIC commands)
target_link_libraries(valkeysearch PUBLIC module)
target_link_libraries(valkeysearch PUBLIC valkey_module)
target_link_libraries(valkeysearch PUBLIC server_events)

list(APPEND THIRD_PARTY_LIBS coordinator_grpc_proto)
list(APPEND THIRD_PARTY_LIBS coordinator_cc_proto)

# The amount of static libraries is insane (especially Abseil / Protobuf). To
# avoid linking hell (order of libraries), we use gcc's `-Wl,--start-group` to
# avoid ordering issues
target_link_libraries(valkeysearch
                      PRIVATE "$<LINK_GROUP:RESCAN,${THIRD_PARTY_LIBS}>")

# Remove the "lib" prefix from the module
set_target_properties(valkeysearch PROPERTIES PREFIX "")

if(UNIX AND NOT APPLE)
  target_link_options(valkeysearch PRIVATE -Wl,--allow-multiple-definition)
endif()

set(SRCS_VALKEY_SEARCH ${CMAKE_CURRENT_LIST_DIR}/valkey_search.cc
                       ${CMAKE_CURRENT_LIST_DIR}/valkey_search.h)

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
target_link_libraries(valkey_search PUBLIC command_parser)
target_link_libraries(valkey_search PUBLIC latency_sampler)
target_link_libraries(valkey_search PUBLIC log)
target_link_libraries(valkey_search PUBLIC managed_pointers)
target_link_libraries(valkey_search PUBLIC memory_allocation)
target_link_libraries(valkey_search PUBLIC thread_pool)
target_link_libraries(valkey_search PUBLIC utils)
target_link_libraries(valkey_search PUBLIC status_macros)
target_link_libraries(valkey_search PUBLIC valkey_module)

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
target_link_libraries(keyspace_event_manager PUBLIC type_conversions)
target_link_libraries(keyspace_event_manager PUBLIC utils)
target_link_libraries(keyspace_event_manager PUBLIC status_macros)
target_link_libraries(keyspace_event_manager PUBLIC valkey_module)

set(SRCS_ATTRIBUTE ${CMAKE_CURRENT_LIST_DIR}/attribute.h)

add_library(attribute INTERFACE ${SRCS_ATTRIBUTE})
target_include_directories(attribute INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(attribute INTERFACE index_schema_cc_proto)
target_link_libraries(attribute INTERFACE index_base)
target_link_libraries(attribute INTERFACE managed_pointers)
target_link_libraries(attribute INTERFACE valkey_module)

set(SRCS_METRICS ${CMAKE_CURRENT_LIST_DIR}/metrics.h)

add_library(metrics INTERFACE ${SRCS_METRICS})
target_include_directories(metrics INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(metrics INTERFACE latency_sampler)

set(SRCS_INDEX_SCHEMA ${CMAKE_CURRENT_LIST_DIR}/index_schema.cc
                      ${CMAKE_CURRENT_LIST_DIR}/index_schema.h)

valkey_search_add_static_library(index_schema "${SRCS_INDEX_SCHEMA}")
target_include_directories(index_schema PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(index_schema PUBLIC attribute)
target_link_libraries(index_schema PUBLIC attribute_data_type)
target_link_libraries(index_schema PUBLIC index_schema_cc_proto)
target_link_libraries(index_schema PUBLIC keyspace_event_manager)
target_link_libraries(index_schema PUBLIC metrics)
target_link_libraries(index_schema PUBLIC rdb_io_stream)
target_link_libraries(index_schema PUBLIC vector_externalizer)
target_link_libraries(index_schema PUBLIC index_base)
target_link_libraries(index_schema PUBLIC numeric)
target_link_libraries(index_schema PUBLIC tag)
target_link_libraries(index_schema PUBLIC vector_base)
target_link_libraries(index_schema PUBLIC vector_flat)
target_link_libraries(index_schema PUBLIC vector_hnsw)
target_link_libraries(index_schema PUBLIC string_interning)
target_link_libraries(index_schema PUBLIC log)
target_link_libraries(index_schema PUBLIC managed_pointers)
target_link_libraries(index_schema PUBLIC module_type)
target_link_libraries(index_schema PUBLIC thread_pool)
target_link_libraries(index_schema PUBLIC time_sliced_mrmw_mutex)
target_link_libraries(index_schema PUBLIC type_conversions)
target_link_libraries(index_schema PUBLIC utils)
target_link_libraries(index_schema PUBLIC status_macros)
target_link_libraries(index_schema PUBLIC valkey_module)

set(SRCS_ATTRIBUTE_DATA_TYPE ${CMAKE_CURRENT_LIST_DIR}/attribute_data_type.cc
                             ${CMAKE_CURRENT_LIST_DIR}/attribute_data_type.h)

valkey_search_add_static_library(attribute_data_type
                                 "${SRCS_ATTRIBUTE_DATA_TYPE}")
target_include_directories(attribute_data_type PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(attribute_data_type PUBLIC index_schema_cc_proto)
target_link_libraries(attribute_data_type PUBLIC managed_pointers)
target_link_libraries(attribute_data_type PUBLIC type_conversions)
target_link_libraries(attribute_data_type PUBLIC utils)
target_link_libraries(attribute_data_type PUBLIC valkey_module)

set(SRCS_RDB_IO_STREAM ${CMAKE_CURRENT_LIST_DIR}/rdb_io_stream.h)

add_library(rdb_io_stream INTERFACE ${SRCS_RDB_IO_STREAM})
target_include_directories(rdb_io_stream INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(rdb_io_stream INTERFACE iostream)
target_link_libraries(rdb_io_stream INTERFACE managed_pointers)
target_link_libraries(rdb_io_stream INTERFACE valkey_module)

set(SRCS_SCHEMA_MANAGER ${CMAKE_CURRENT_LIST_DIR}/schema_manager.cc
                        ${CMAKE_CURRENT_LIST_DIR}/schema_manager.h)

valkey_search_add_static_library(schema_manager "${SRCS_SCHEMA_MANAGER}")
target_include_directories(schema_manager PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(schema_manager PUBLIC index_schema)
target_link_libraries(schema_manager PUBLIC index_schema_cc_proto)
target_link_libraries(schema_manager PUBLIC metrics)
target_link_libraries(schema_manager PUBLIC rdb_io_stream)
target_link_libraries(schema_manager PUBLIC vector_externalizer)
target_link_libraries(schema_manager PUBLIC coordinator_cc_proto)
target_link_libraries(schema_manager PUBLIC metadata_manager)
target_link_libraries(schema_manager PUBLIC log)
target_link_libraries(schema_manager PUBLIC managed_pointers)
target_link_libraries(schema_manager PUBLIC thread_pool)
target_link_libraries(schema_manager PUBLIC utils)
target_link_libraries(schema_manager PUBLIC status_macros)
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
target_link_libraries(vector_externalizer PUBLIC managed_pointers)
target_link_libraries(vector_externalizer PUBLIC utils)
target_link_libraries(vector_externalizer PUBLIC valkey_module)
