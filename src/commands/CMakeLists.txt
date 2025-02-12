set(SRCS_COMMANDS
    ${CMAKE_CURRENT_LIST_DIR}/ft_create.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_dropindex.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_info.cc ${CMAKE_CURRENT_LIST_DIR}/ft_list.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_search.cc ${CMAKE_CURRENT_LIST_DIR}/commands.h
    ${CMAKE_CURRENT_LIST_DIR}/ft_search.h)

valkey_search_add_static_library(commands ${SRCS_COMMANDS})
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
target_link_libraries(commands PUBLIC command_parser)
target_link_libraries(commands PUBLIC managed_pointers)
target_link_libraries(commands PUBLIC type_conversions)
target_link_libraries(commands PUBLIC utils)
target_link_libraries(commands PUBLIC status_macros)
target_link_libraries(commands PUBLIC valkey_module)

set(SRCS_FT_SEARCH_PARSER ${CMAKE_CURRENT_LIST_DIR}/ft_search_parser.cc
                          ${CMAKE_CURRENT_LIST_DIR}/ft_search_parser.h)

valkey_search_add_static_library(ft_search_parser ${SRCS_FT_SEARCH_PARSER})
target_include_directories(ft_search_parser PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(ft_search_parser PUBLIC filter_parser)
target_link_libraries(ft_search_parser PUBLIC index_schema)
target_link_libraries(ft_search_parser PUBLIC metrics)
target_link_libraries(ft_search_parser PUBLIC schema_manager)
target_link_libraries(ft_search_parser PUBLIC index_base)
target_link_libraries(ft_search_parser PUBLIC search)
target_link_libraries(ft_search_parser PUBLIC command_parser)
target_link_libraries(ft_search_parser PUBLIC managed_pointers)
target_link_libraries(ft_search_parser PUBLIC type_conversions)
target_link_libraries(ft_search_parser PUBLIC status_macros)
target_link_libraries(ft_search_parser PUBLIC valkey_module)

set(SRCS_FT_CREATE_PARSER ${CMAKE_CURRENT_LIST_DIR}/ft_create_parser.cc
                          ${CMAKE_CURRENT_LIST_DIR}/ft_create_parser.h)

valkey_search_add_static_library(ft_create_parser ${SRCS_FT_CREATE_PARSER})
target_include_directories(ft_create_parser PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(ft_create_parser PUBLIC attribute_data_type)
target_link_libraries(ft_create_parser PUBLIC index_schema_cc_proto)
target_link_libraries(ft_create_parser PUBLIC index_base)
target_link_libraries(ft_create_parser PUBLIC vector_base)
target_link_libraries(ft_create_parser PUBLIC command_parser)
target_link_libraries(ft_create_parser PUBLIC type_conversions)
target_link_libraries(ft_create_parser PUBLIC status_macros)
target_link_libraries(ft_create_parser PUBLIC valkey_module)

set(SRCS_FILTER_PARSER ${CMAKE_CURRENT_LIST_DIR}/filter_parser.cc
                       ${CMAKE_CURRENT_LIST_DIR}/filter_parser.h)

valkey_search_add_static_library(filter_parser ${SRCS_FILTER_PARSER})
target_include_directories(filter_parser PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(filter_parser PUBLIC index_schema)
target_link_libraries(filter_parser PUBLIC index_base)
target_link_libraries(filter_parser PUBLIC numeric)
target_link_libraries(filter_parser PUBLIC tag)
target_link_libraries(filter_parser PUBLIC predicate)
target_link_libraries(filter_parser PUBLIC status_macros)
