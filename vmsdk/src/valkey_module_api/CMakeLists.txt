set(SRCS_VALKEY_MODULE ${CMAKE_CURRENT_LIST_DIR}/valkey_module.h)

add_library(valkey_module INTERFACE ${SRCS_VALKEY_MODULE})
target_include_directories(valkey_module INTERFACE ${CMAKE_CURRENT_LIST_DIR})
if(APPLE)
  target_link_libraries(valkey_module INTERFACE absl::base absl::core_headers)
else()
  target_link_libraries(valkey_module INTERFACE ${GRPC_LIB})
endif()
