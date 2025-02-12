include(FetchContent)
include(ProcessorCount)

ProcessorCount(PROCESSOR_COUNT)

set(MODULES_DIR "${CMAKE_BINARY_DIR}/.deps")
file(MAKE_DIRECTORY "${MODULES_DIR}")

set(MODULE_GRPC "grpc")
set(MODULE_HIGHWAYHASH "highwayhash")
set(MODULE_GOOGLE_TEST "googletest")

set(MODULES_INSTALL_PREFIX "${MODULES_DIR}/install")

find_program(_GIT_EXE git REQUIRED)

# Helper method: checkout submodule for a given branch/tag and place it under
# the ".deps" folder
function(checkout_submodule_branch MODULE_NAME GIT_URL GIT_BRANCH)
  if(NOT EXISTS "${MODULES_DIR}/${MODULE_NAME}")
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
function(build_submodule MODULE_NAME MODULE_CMAKE_ARGS)
  file(MAKE_DIRECTORY ${MODULES_DIR}/${MODULE_NAME}/build-release)
  execute_process(
    COMMAND sh -c "${CMAKE_COMMAND} .. ${MODULE_CMAKE_ARGS}"
    WORKING_DIRECTORY ${MODULES_DIR}/${MODULE_NAME}/build-release
    RESULT_VARIABLE _PROC_RES)
  if(NOT _PROC_RES EQUAL 0)
    message(FATAL_ERROR "CMake configure error")
  endif()
  execute_process(
    COMMAND sh -c
            "${CMAKE_COMMAND} --build . -j ${PROCESSOR_COUNT} --target install"
    WORKING_DIRECTORY ${MODULES_DIR}/${MODULE_NAME}/build-release
    RESULT_VARIABLE _PROC_RES)
  if(NOT _PROC_RES EQUAL 0)
    message(FATAL_ERROR "Install error")
  endif()
endfunction()

# Pull submodules with a given branch / tag and build them
checkout_submodule_branch(${MODULE_GRPC} "https://github.com/grpc/grpc"
                          "v1.70.1")

checkout_submodule_branch(${MODULE_HIGHWAYHASH}
                          "https://github.com/google/highwayhash.git" "master")

checkout_submodule_branch(${MODULE_GOOGLE_TEST}
                          "https://github.com/google/googletest.git" "main")

set(MODULES_COMMON_CMAKE_ARGS
    "-DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_INSTALL_PREFIX=${MODULES_INSTALL_PREFIX} -DCMAKE_CXX_STANDARD=20"
)
set(GOOGLE_TEST_CMAKE_ARGS "${MODULES_COMMON_CMAKE_ARGS}")
set(GRPC_CMAKE_ARGS
    "${MODULES_COMMON_CMAKE_ARGS} -DSKIP_INSTALL_ALL=ON -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF -DgRPC_SSL_PROVIDER=package"
)

build_submodule(${MODULE_GOOGLE_TEST} "${GOOGLE_TEST_CMAKE_ARGS}")
build_submodule(${MODULE_GRPC} "${GRPC_CMAKE_ARGS}")

add_subdirectory(${MODULES_DIR}/highwayhash ${CMAKE_BINARY_DIR}/highwayhash)
