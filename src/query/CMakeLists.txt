# =============================================================================
# libvs_query: Query planning, search, predicates, fanout, response generation
#              Also includes expr sources (folded from src/expr/)
# =============================================================================
set(SRCS_VS_QUERY
    # Query sources
    ${CMAKE_CURRENT_LIST_DIR}/predicate.cc
    ${CMAKE_CURRENT_LIST_DIR}/search.cc
    ${CMAKE_CURRENT_LIST_DIR}/content_resolution.cc
    ${CMAKE_CURRENT_LIST_DIR}/fanout.cc
    ${CMAKE_CURRENT_LIST_DIR}/primary_info_fanout_operation.cc
    ${CMAKE_CURRENT_LIST_DIR}/cluster_info_fanout_operation.cc
    ${CMAKE_CURRENT_LIST_DIR}/response_generator.cc
    ${CMAKE_CURRENT_LIST_DIR}/planner.cc
    # Expr sources (folded from src/expr/)
    ${CMAKE_CURRENT_LIST_DIR}/../expr/expr.cc
    ${CMAKE_CURRENT_LIST_DIR}/../expr/value.cc)

valkey_search_add_static_library(libvs_query "${SRCS_VS_QUERY}")

target_include_directories(libvs_query PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_include_directories(libvs_query PUBLIC ${CMAKE_CURRENT_LIST_DIR}/../expr)

# Dependency on indexes library (which transitively brings in core)
target_link_libraries(libvs_query PUBLIC libvs_indexes)

# Proto dependencies (fanout uses coordinator proto types directly)
target_link_libraries(libvs_query PUBLIC coordinator_cc_proto)
target_link_libraries(libvs_query PUBLIC coordinator_grpc_proto)

# SDK dependencies
target_link_libraries(libvs_query PUBLIC vmsdklib)
target_link_libraries(libvs_query PUBLIC valkey_module)
