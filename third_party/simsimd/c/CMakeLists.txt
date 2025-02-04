set(SRCS_LIB ${CMAKE_CURRENT_LIST_DIR}/lib.c)

add_library(simsimd_c STATIC ${SRCS_LIB})
target_include_directories(simsimd_c PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(simsimd_c PUBLIC simsimd)
