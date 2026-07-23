set(SRCS_EXPR_TEST
    expr_test.cc
    value_test.cc)

add_executable(expr_test ${SRCS_EXPR_TEST})
target_include_directories(expr_test
                           PUBLIC .)
target_link_libraries(expr_test PUBLIC expr)
target_link_libraries(expr_test PUBLIC client)
target_link_libraries(expr_test PUBLIC client_pool)
target_link_libraries(expr_test PUBLIC coordinator_cc_proto)
target_link_libraries(expr_test PUBLIC metadata_manager)
target_link_libraries(expr_test PUBLIC util)
target_link_libraries(expr_test PUBLIC testing_common_base)
target_link_libraries(expr_test PUBLIC vmsdklib)
target_link_libraries(expr_test PUBLIC valkey_module)
finalize_test_flags(expr_test)
