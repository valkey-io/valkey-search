# Single base library that consolidates ALL common dependencies
add_library(query_base INTERFACE)
target_link_libraries(query_base INTERFACE attribute_data_type)
target_link_libraries(query_base INTERFACE index_schema)
target_link_libraries(query_base INTERFACE index_base)
target_link_libraries(query_base INTERFACE vector_base)
target_link_libraries(query_base INTERFACE numeric)
target_link_libraries(query_base INTERFACE tag)
target_link_libraries(query_base INTERFACE vector_flat)
target_link_libraries(query_base INTERFACE vector_hnsw)
target_link_libraries(query_base INTERFACE hnswlib_vmsdk)
target_link_libraries(query_base INTERFACE client_pool)
target_link_libraries(query_base INTERFACE coordinator_cc_proto)
target_link_libraries(query_base INTERFACE search_converter)
target_link_libraries(query_base INTERFACE util)
target_link_libraries(query_base INTERFACE managed_pointers)
target_link_libraries(query_base INTERFACE type_conversions)
target_link_libraries(query_base INTERFACE status_macros)
target_link_libraries(query_base INTERFACE valkey_module)
target_link_libraries(query_base INTERFACE log)
target_link_libraries(query_base INTERFACE thread_pool)
target_link_libraries(query_base INTERFACE time_sliced_mrmw_mutex)
target_link_libraries(query_base INTERFACE metrics)
target_link_libraries(query_base INTERFACE filter_parser)
target_link_libraries(query_base INTERFACE latency_sampler)
target_link_libraries(query_base INTERFACE string_interning)

# 1. Planner
set(SRCS_PLANNER ${CMAKE_CURRENT_LIST_DIR}/planner.cc
                 ${CMAKE_CURRENT_LIST_DIR}/planner.h)

valkey_search_add_static_library(planner "${SRCS_PLANNER}")
target_include_directories(planner PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(planner PUBLIC query_base)

# 2. Predicate
set(SRCS_PREDICATE ${CMAKE_CURRENT_LIST_DIR}/predicate.cc
                   ${CMAKE_CURRENT_LIST_DIR}/predicate.h)

valkey_search_add_static_library(predicate "${SRCS_PREDICATE}")
target_include_directories(predicate PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(predicate PUBLIC query_base)

# Predicate header-only interface
set(SRCS_PREDICATE_HEADER ${CMAKE_CURRENT_LIST_DIR}/predicate.h)

add_library(predicate_header INTERFACE ${SRCS_PREDICATE_HEADER})
target_include_directories(predicate_header INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(predicate_header INTERFACE query_base)

# 3. Search
set(SRCS_SEARCH ${CMAKE_CURRENT_LIST_DIR}/search.cc
                ${CMAKE_CURRENT_LIST_DIR}/search.h)

valkey_search_add_static_library(search "${SRCS_SEARCH}")
target_include_directories(search PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(search PUBLIC query_base)
target_link_libraries(search PUBLIC planner)
target_link_libraries(search PUBLIC predicate)

# Search header-only interface
set(SRCS_SEARCH_HEADER ${CMAKE_CURRENT_LIST_DIR}/search.h)

add_library(search_header INTERFACE ${SRCS_SEARCH_HEADER})
target_include_directories(search_header INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(search_header INTERFACE query_base)
target_link_libraries(search_header INTERFACE predicate_header)

# 4. Fanout
set(SRCS_FANOUT ${CMAKE_CURRENT_LIST_DIR}/fanout.cc
                ${CMAKE_CURRENT_LIST_DIR}/fanout.h)

valkey_search_add_static_library(fanout "${SRCS_FANOUT}")
target_include_directories(fanout PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(fanout PUBLIC query_base)
target_link_libraries(fanout PUBLIC search)

# 5. Response Generator
set(SRCS_RESPONSE_GENERATOR ${CMAKE_CURRENT_LIST_DIR}/response_generator.cc
                            ${CMAKE_CURRENT_LIST_DIR}/response_generator.h
                            ${CMAKE_CURRENT_LIST_DIR}/info_fanout.cc)

valkey_search_add_static_library(response_generator "${SRCS_RESPONSE_GENERATOR}")
target_include_directories(response_generator PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(response_generator PUBLIC query_base)
target_link_libraries(response_generator PUBLIC predicate_header)
target_link_libraries(response_generator PUBLIC search_header)

# 6. Info Fanout
set(SRCS_INFO_FANOUT ${CMAKE_CURRENT_LIST_DIR}/info_fanout.cc
                     ${CMAKE_CURRENT_LIST_DIR}/info_fanout.h)

valkey_search_add_static_library(info_fanout "${SRCS_INFO_FANOUT}")
target_include_directories(info_fanout PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(info_fanout PUBLIC query_base)
target_link_libraries(info_fanout PUBLIC fanout)
target_link_libraries(info_fanout PUBLIC info_converter)
target_link_libraries(info_fanout PUBLIC schema_manager)

# 7. Primary Info Fanout
set(SRCS_PRIMARY_INFO_FANOUT ${CMAKE_CURRENT_LIST_DIR}/primary_info_fanout.cc
                             ${CMAKE_CURRENT_LIST_DIR}/primary_info_fanout.h)

valkey_search_add_static_library(primary_info_fanout "${SRCS_PRIMARY_INFO_FANOUT}")
target_include_directories(primary_info_fanout PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(primary_info_fanout PUBLIC query_base)
target_link_libraries(primary_info_fanout PUBLIC fanout)
target_link_libraries(primary_info_fanout PUBLIC info_converter)
target_link_libraries(primary_info_fanout PUBLIC schema_manager)
