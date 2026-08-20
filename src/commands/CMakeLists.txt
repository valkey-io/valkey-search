include (object_library_helper)

set(SRCS_COMMANDS
    ft_aggregate.cc
    ft_aggregate.h
    ft_aggregate_parser.cc
    ft_aggregate_parser.h
    ft_aggregate_exec.cc
    ft_aggregate_exec.h
    ft_create.cc
    ft_debug.cc
    ft_dropindex.cc
    ft_info.cc 
    ft_internal_update.cc
    ft_list.cc
    ft_search.cc
    commands.h
    commands.cc
    ft_search.h)

add_library(commands_basic OBJECT ${SRCS_COMMANDS})
target_include_directories(commands_basic PUBLIC .)
target_link_libraries(commands_basic PRIVATE mydeps)

add_library(ft_search_parser OBJECT
        ft_search_parser.cc
        ft_search_parser.h
        ft_info_parser.cc
        ft_info_parser.h)
target_include_directories(ft_search_parser PUBLIC .)
target_link_libraries(ft_search_parser PRIVATE mydeps)

add_library(ft_create_parser OBJECT
        ft_create_parser.cc
        ft_create_parser.h)
target_include_directories(ft_create_parser PUBLIC .)
target_link_libraries(ft_create_parser PRIVATE mydeps)

add_library(filter_parser OBJECT
        filter_parser.cc
        filter_parser.h)

target_include_directories(filter_parser PUBLIC .)
target_link_libraries(filter_parser PRIVATE mydeps)

create_object_interface(commands
        commands_basic
        ft_search_parser
        ft_create_parser
        filter_parser)