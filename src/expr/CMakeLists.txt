set(SRCS_EXPR
    ${CMAKE_CURRENT_LIST_DIR}/expr.cc ${CMAKE_CURRENT_LIST_DIR}/expr.h
    ${CMAKE_CURRENT_LIST_DIR}/value.cc ${CMAKE_CURRENT_LIST_DIR}/value.h)

valkey_search_add_static_library(expr "${SRCS_EXPR}")
target_include_directories(expr PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(expr PUBLIC scanner)
target_link_libraries(expr PUBLIC vmsdklib)
target_link_libraries(expr PUBLIC valkey_module)
