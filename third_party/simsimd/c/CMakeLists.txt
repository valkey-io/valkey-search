set(SRCS_LIB lib.c)

add_library(simsimd_c OBJECT "${SRCS_LIB}")
target_include_directories(simsimd_c PUBLIC ../include)
target_link_libraries(simsimd_c PUBLIC simsimd)