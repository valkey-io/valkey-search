set(SRCS_VALKEY_MODULE valkey_module.h)

add_library(valkey_module INTERFACE ${SRCS_VALKEY_MODULE})
target_include_directories(valkey_module INTERFACE .)