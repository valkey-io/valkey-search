set(VMSDK_TESTING_SRCS
    utils.cc
    utils.h
    module.h)

add_library(vmsdk_testing SHARED ${VMSDK_TESTING_SRCS})
target_link_libraries(vmsdk_testing PUBLIC absl::base absl::strings)
target_include_directories(vmsdk_testing PUBLIC .)
target_include_directories(vmsdk_testing PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../../include)
target_include_directories(vmsdk_testing PRIVATE ${PROJECT_SOURCE_DIR})