# =============================================================================
# libvs_commands: All FT.* command implementations and parsers
# =============================================================================
set(SRCS_VS_COMMANDS
    ${CMAKE_CURRENT_LIST_DIR}/commands.cc
    ${CMAKE_CURRENT_LIST_DIR}/filter_parser.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_aggregate.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_aggregate_parser.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_aggregate_exec.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_create.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_create_parser.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_debug.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_dropindex.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_info.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_info_parser.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_internal_update.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_list.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_search.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_search_parser.cc)

valkey_search_add_static_library(libvs_commands "${SRCS_VS_COMMANDS}")

target_include_directories(libvs_commands PUBLIC ${CMAKE_CURRENT_LIST_DIR})

# Dependency on query library (which transitively brings in indexes and core)
target_link_libraries(libvs_commands PUBLIC libvs_query)

# SDK dependencies
target_link_libraries(libvs_commands PUBLIC vmsdklib)
target_link_libraries(libvs_commands PUBLIC valkey_module)
