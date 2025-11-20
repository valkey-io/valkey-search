set(SRCS_EXPR_TEST
    ${CMAKE_CURRENT_LIST_DIR}/expr_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/value_test.cc)

add_executable(expr_test ${SRCS_EXPR_TEST})
target_include_directories(expr_test
                           PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(expr_test PUBLIC expr)
target_link_libraries(expr_test PUBLIC client)
target_link_libraries(expr_test PUBLIC client_pool)
target_link_libraries(expr_test PUBLIC coordinator_cc_proto)
target_link_libraries(expr_test PUBLIC metadata_manager)
target_link_libraries(expr_test PUBLIC util)
target_link_libraries(expr_test PUBLIC testing_common)
target_link_libraries(expr_test PUBLIC vmsdklib)
target_link_libraries(expr_test PUBLIC valkey_module)
finalize_test_flags(expr_test)
