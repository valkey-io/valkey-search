set(SRCS_STATUS_MACROS
    ${CMAKE_CURRENT_LIST_DIR}/status_builder.cc
    ${CMAKE_CURRENT_LIST_DIR}/source_location.h
    ${CMAKE_CURRENT_LIST_DIR}/status_builder.h
    ${CMAKE_CURRENT_LIST_DIR}/status_macros.h)

valkey_search_add_static_library(status_macros "${SRCS_STATUS_MACROS}")
target_include_directories(status_macros PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(status_macros PUBLIC valkey_module)
