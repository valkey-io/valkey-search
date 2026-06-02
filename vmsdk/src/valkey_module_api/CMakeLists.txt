set(SRCS_VALKEY_MODULE ${CMAKE_CURRENT_LIST_DIR}/valkey_module.h)

add_library(valkey_module INTERFACE ${SRCS_VALKEY_MODULE})
target_include_directories(valkey_module INTERFACE ${CMAKE_CURRENT_LIST_DIR})

