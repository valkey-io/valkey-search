set(SRCS_EXPR
    ${CMAKE_CURRENT_LIST_DIR}/expr.cc ${CMAKE_CURRENT_LIST_DIR}/expr.h
    ${CMAKE_CURRENT_LIST_DIR}/value.cc ${CMAKE_CURRENT_LIST_DIR}/value.h)

valkey_search_add_static_library(expr "${SRCS_EXPR}")
target_include_directories(commands PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(commands PUBLIC ft_create_parser)
target_link_libraries(commands PUBLIC ft_search_parser)
target_link_libraries(commands PUBLIC index_schema)
target_link_libraries(commands PUBLIC index_schema_cc_proto)
target_link_libraries(commands PUBLIC metrics)
target_link_libraries(commands PUBLIC schema_manager)
target_link_libraries(commands PUBLIC valkey_search)
target_link_libraries(commands PUBLIC vector_base)
target_link_libraries(commands PUBLIC fanout)
target_link_libraries(commands PUBLIC response_generator)
target_link_libraries(commands PUBLIC search)
target_link_libraries(commands PUBLIC vmsdklib)
target_link_libraries(commands PUBLIC valkey_module)
