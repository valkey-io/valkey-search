include(FetchContent)
include(ProcessorCount)
ProcessorCount(PROCESSOR_COUNT)

set(MODULES_DIR "${CMAKE_BINARY_DIR}/.deps")
file(MAKE_DIRECTORY "${MODULES_DIR}")

set(MODULE_HIGHWAYHASH "highwayhash")
set(MODULE_GRPC "grpc")
set(MODULE_GOOGLE_TEST "googletest")

set(MODULES_INSTALL_PREFIX "${MODULES_DIR}/install")

find_program(_GIT_EXE git REQUIRED)

# Helper method: checkout submodule for a given branch/tag and place it under
# the ".deps" folder
function(checkout_submodule_branch MODULE_NAME GIT_URL GIT_BRANCH HINT_PATH)
  # If ${HINT_PATH} does not exist, perform checkout
  if(NOT EXISTS "${HINT_PATH}")
    message(
      STATUS
        "Removing old directory ${MODULES_DIR}/${MODULE_NAME} before checking out"
    )
    file(REMOVE_RECURSE "${MODULES_DIR}/${MODULE_NAME}")
    message(
      STATUS
        "Fetching content ${MODULE_NAME} version ${GIT_BRANCH} - this may take a while, be patient"
    )
    execute_process(
      COMMAND
        ${_GIT_EXE} clone --branch=${GIT_BRANCH} ${GIT_URL}
        ${MODULES_DIR}/${MODULE_NAME} --recurse-submodules --shallow-submodules
        --depth=1 --single-branch
      OUTPUT_VARIABLE _GIT_CHECKOUT_OUTPUT
      OUTPUT_QUIET ERROR_QUIET
      RESULT_VARIABLE _CHECKOUT_RES)

    if(NOT _CHECKOUT_RES EQUAL 0)
      message(
        FATAL_ERROR "Failed to checkout module ${MODULE_NAME}. ${GIT_URL}")
    endif()
    message(STATUS "Content placed in ${MODULES_DIR}/${MODULE_NAME}")
  else()
    message(STATUS "Using cached content ${MODULES_DIR}/${MODULE_NAME}")
  endif()
endfunction()

# Helper method: build the submodule and install it
function(build_submodule MODULE_NAME MODULE_CMAKE_ARGS HINT_FILE)
  if(NOT EXISTS "${HINT_FILE}")
    file(MAKE_DIRECTORY ${MODULES_DIR}/${MODULE_NAME}/build-release)
    set(ENV{CXXFLAGS}
        "-Wno-missing-requires -Wno-attributes -Wno-deprecated -Wno-return-type -Wno-stringop-overflow -Wno-deprecated-declarations"
    )
    execute_process(
      COMMAND sh -c "${CMAKE_COMMAND} .. ${MODULE_CMAKE_ARGS}"
      WORKING_DIRECTORY ${MODULES_DIR}/${MODULE_NAME}/build-release
      RESULT_VARIABLE _PROC_RES)
    if(NOT _PROC_RES EQUAL 0)
      message(FATAL_ERROR "CMake configure error")
    endif()
    execute_process(
      COMMAND
        sh -c
        "${CMAKE_COMMAND} --build . -j ${PROCESSOR_COUNT} --target install"
      WORKING_DIRECTORY ${MODULES_DIR}/${MODULE_NAME}/build-release
      RESULT_VARIABLE _PROC_RES)
    if(NOT _PROC_RES EQUAL 0)
      message(FATAL_ERROR "Install error")
    endif()
  else()
    message(STATUS "Submodule ${MODULE_NAME} is already built and installed")
  endif()
endfunction()

if(WITH_SUBMODULES_SYSTEM)
  message(STATUS "Checking for system provided grpc_cpp_plugin")
  find_program(GRPC_CPP_PLUGIN_PATH_EXE grpc_cpp_plugin REQUIRED)
  message(STATUS "Found ${GRPC_CPP_PLUGIN_PATH_EXE}")
  set(GRPC_CPP_PLUGIN_PATH
      "${GRPC_CPP_PLUGIN_PATH_EXE}"
      PARENT_SCOPE)
  find_program(PROTOC_sys_EXE protoc REQUIRED)
  set(protoc_EXE
      "${PROTOC_sys_EXE}"
      PARENT_SCOPE)

  # Use system provided libhighwayhash
  find_path(LIBHIGHWAYHASH_LIBDIR libhighwayhash.a REQUIRED PATH_SUFFIXES lib)
  find_path(LIBHIGHWAYHASH_INCLUDE highwayhash/highwayhash.h REQUIRED)
  set(LIBHIGHWAYHASH "${LIBHIGHWAYHASH_LIBDIR}/libhighwayhash.a")

  add_library(highwayhash STATIC IMPORTED GLOBAL)
  set_target_properties(
    highwayhash PROPERTIES IMPORTED_LOCATION "${LIBHIGHWAYHASH}"
                           INCLUDE_DIRECTORIES "${LIBHIGHWAYHASH_INCLUDE}")

  set(HIGHWAY_HASH_INCLUDE_PATH
      "${LIBHIGHWAYHASH_INCLUDE}"
      PARENT_SCOPE)
  message(STATUS "libhighwayhash.a found: ${LIBHIGHWAYHASH}")
  message(STATUS "highwayhash include path: ${LIBHIGHWAYHASH_INCLUDE}")
else()
  # Pull submodules with a given branch / tag and build them
  checkout_submodule_branch(${MODULE_GRPC} "https://github.com/grpc/grpc"
                            "v1.70.1" "${MODULES_DIR}/grpc/build-release")

  checkout_submodule_branch(
    ${MODULE_HIGHWAYHASH} "https://github.com/google/highwayhash.git" "master"
    "${MODULES_DIR}/highwayhash")

  checkout_submodule_branch(
    ${MODULE_GOOGLE_TEST} "https://github.com/google/googletest.git" "main"
    "${MODULES_DIR}/googletest/build-release")

  set(MODULES_COMMON_CMAKE_ARGS
      "-DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_INSTALL_PREFIX=${MODULES_INSTALL_PREFIX} -DCMAKE_CXX_STANDARD=20"
  )
  set(GOOGLE_TEST_CMAKE_ARGS "${MODULES_COMMON_CMAKE_ARGS}")
  set(GRPC_CMAKE_ARGS
      "${MODULES_COMMON_CMAKE_ARGS} -DSKIP_INSTALL_ALL=ON -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF -DgRPC_SSL_PROVIDER=package"
  )

  add_subdirectory(${MODULES_DIR}/highwayhash ${CMAKE_BINARY_DIR}/highwayhash)
  build_submodule(${MODULE_GOOGLE_TEST} "${GOOGLE_TEST_CMAKE_ARGS}"
                  "${MODULES_INSTALL_PREFIX}/include/gtest/gtest.h")
  build_submodule(${MODULE_GRPC} "${GRPC_CMAKE_ARGS}"
                  "${MODULES_INSTALL_PREFIX}/include/absl/base/options.h")

  set(HIGHWAY_HASH_INCLUDE_PATH
      "${MODULES_DIR}/highwayhash"
      PARENT_SCOPE)
  set(GRPC_CPP_PLUGIN_PATH
      "${CMAKE_BINARY_DIR}/.deps/install/bin/grpc_cpp_plugin"
      PARENT_SCOPE)
  set(protoc_EXE
      "${CMAKE_BINARY_DIR}/.deps/install/bin/protoc"
      PARENT_SCOPE)

  include_directories("${CMAKE_BINARY_DIR}/.deps/install/include")
  # Make sure that find_package will find our installed submodules
  set(__CMAKE_PREFIX_PATH
      "${__CMAKE_PREFIX_PATH}:${CMAKE_BINARY_DIR}/.deps/install/lib/cmake/absl")
  set(__CMAKE_PREFIX_PATH
      "${__CMAKE_PREFIX_PATH}:${CMAKE_BINARY_DIR}/.deps/install/lib/cmake/GTest"
  )
  set(__CMAKE_PREFIX_PATH
      "${__CMAKE_PREFIX_PATH}:${CMAKE_BINARY_DIR}/.deps/install/lib/cmake/grpc")
  set(__CMAKE_PREFIX_PATH
      "${__CMAKE_PREFIX_PATH}:${CMAKE_BINARY_DIR}/.deps/install/lib/cmake/protobuf"
  )
  set(__CMAKE_PREFIX_PATH
      "${__CMAKE_PREFIX_PATH}:${CMAKE_BINARY_DIR}/.deps/install/lib/cmake/utf8_range"
  )
  set(ENV{CMAKE_PREFIX_PATH} "${__CMAKE_PREFIX_PATH}")
  message(STATUS "CMAKE_PREFIX_PATH=${__CMAKE_PREFIX_PATH}")
endif()
