set(VMSDK_TESTING_INFRA_SRCS
    ${CMAKE_CURRENT_LIST_DIR}/utils.cc ${CMAKE_CURRENT_LIST_DIR}/utils.h
    ${CMAKE_CURRENT_LIST_DIR}/module.h)

valkey_search_add_static_library(vmsdk_testing_infra
                                 "${VMSDK_TESTING_INFRA_SRCS}")
