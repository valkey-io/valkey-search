set(SRCS_EXPR_TEST
    ${CMAKE_CURRENT_LIST_DIR}/expr_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/value_test.cc)

add_executable(expr_test ${SRCS_EXPR_TEST})
target_include_directories(expr_test
                           PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(expr_test PUBLIC testing_common_base)
finalize_test_flags(expr_test)
