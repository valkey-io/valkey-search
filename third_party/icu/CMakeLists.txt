# ICU (International Components for Unicode) - Static linking
set(ICU_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/source)
set(ICU_BUILD_DIR ${CMAKE_BINARY_DIR}/icu/install)

if(NOT EXISTS ${ICU_SOURCE_DIR})
  message(FATAL_ERROR "ICU source not found in third_party/icu/source - source should be committed to repository")
endif()

if(NOT EXISTS ${ICU_BUILD_DIR}/lib/libicudata.a)
  message(FATAL_ERROR "ICU static libraries not found in build directory. Run: ./build.sh --configure")
endif()

add_library(icu INTERFACE)
target_link_libraries(icu INTERFACE 
  ${ICU_BUILD_DIR}/lib/libicudata.a
  ${ICU_BUILD_DIR}/lib/libicui18n.a
  ${ICU_BUILD_DIR}/lib/libicuuc.a
)

target_include_directories(icu INTERFACE 
  ${ICU_SOURCE_DIR}/common 
  ${ICU_SOURCE_DIR}/i18n
)

target_compile_definitions(icu INTERFACE U_STATIC_IMPLEMENTATION)
