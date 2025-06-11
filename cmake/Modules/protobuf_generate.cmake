message(
  STATUS "Processing file ${CMAKE_CURRENT_LIST_DIR}/protobuf_generate.cmake")

set(MODULE_BASE_DIR "${CMAKE_BINARY_DIR}/.deps")
set(MODULES_BIN_DIR "${MODULE_BASE_DIR}/install/bin")

# Check (based on timestamp) if we need to re-generate protbuf files
function(_valkey_search_is_proto_gen_required PROTO_PATH TARGET_PATH
         GEN_REQUIRED)
  if(NOT EXISTS "${TARGET_PATH}")
    # If the genenrated file does not exist, create it
    set(${GEN_REQUIRED}
        ON
        PARENT_SCOPE)
  else()
    set(PROTO_PATH "${CMAKE_SOURCE_DIR}/${PROTO_PATH}")
    file(TIMESTAMP "${PROTO_PATH}" PROTO_TS "%s")
    math(EXPR proto_timestamp "${PROTO_TS}")

    file(TIMESTAMP "${TARGET_PATH}" TARGET_TS "%s")
    math(EXPR target_timestamp "${TARGET_TS}")

    if(proto_timestamp GREATER target_timestamp)
      set(${GEN_REQUIRED}
          ON
          PARENT_SCOPE)
    else()
      set(${GEN_REQUIRED}
          OFF
          PARENT_SCOPE)
    endif()
  endif()
endfunction()

# Generate grpc files for C++ at output directory "${OUT_GEN_DIR}" and create a
# static library target named ${TARGET_NAME} from the generated files
function(valkey_search_create_proto_grpc_library PROTO_PATH TARGET_NAME)
  set(PROTO_OUT_DIR ${CMAKE_BINARY_DIR})
  get_filename_component(PROTO_FILE_DIR "${PROTO_PATH}" PATH)
  get_filename_component(PROTO_FILE_BASE_NAME "${PROTO_PATH}" NAME_WE)

  set(GEN_FILE_H
      "${PROTO_OUT_DIR}/${PROTO_FILE_DIR}/${PROTO_FILE_BASE_NAME}.grpc.pb.h")
  set(GEN_FILE_CC
      "${PROTO_OUT_DIR}/${PROTO_FILE_DIR}/${PROTO_FILE_BASE_NAME}.grpc.pb.cc")
  list(APPEND GEN_FILES "${GEN_FILE_H}")
  list(APPEND GEN_FILES "${GEN_FILE_CC}")

  # To avoid rebuilding everytime we run "cmake", check if regenrting is
  # actually required
  _valkey_search_is_proto_gen_required(${PROTO_PATH} ${GEN_FILE_H}
                                       GEN_H_REQUIRED)
  _valkey_search_is_proto_gen_required(${PROTO_PATH} ${GEN_FILE_CC}
                                       GEN_CC_REQUIRED)
  if(${GEN_CC_REQUIRED} OR ${GEN_FILE_H})
    message(STATUS "Generating files from ${PROTO_PATH} (gRPC)")
    execute_process(
      COMMAND ${protoc_EXE} --grpc_out=${PROTO_OUT_DIR} -I${CMAKE_SOURCE_DIR}
              --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_PATH} ${PROTO_PATH}
      RESULT_VARIABLE protoc_grpc_RES
      OUTPUT_VARIABLE protoc_grpc_STDOUT
      ERROR_VARIABLE protoc_grpc_STDERR)

    if(NOT protoc_grpc_RES EQUAL 0)
      message(WARNING "Failed to generate gRPC files.")
      message(WARNING "STDOUT: ${protoc_grpc_STDOUT}")
      message(WARNING "STDERR: ${protoc_grpc_STDERR}")
      message(FATAL_ERROR "Exiting")
    endif()
  else()
    foreach(GENERATED_FILE ${GEN_FILES})
      message(STATUS "Generated file: ${GENERATED_FILE} is up-to-date")
    endforeach()
  endif()

  add_library(${TARGET_NAME} STATIC ${GEN_FILES})
  message(STATUS "Created protobuf (gRPC) target: ${TARGET_NAME}")
  valkey_search_target_update_compile_flags(${TARGET_NAME})
  target_link_libraries(${TARGET_NAME} PUBLIC ${Protobuf_LIBRARIES})
endfunction()

# Helper method: create static library from a single proto file. "PROTO_PATH"
# contains the relative path to the proto file from top level source directory.
# "TARGET_NAME" is the user provided target name to create
function(valkey_search_create_proto_library PROTO_PATH TARGET_NAME)
  get_filename_component(PROTO_DIR "${PROTO_PATH}" PATH)
  set(PROTO_OUT_DIR ${CMAKE_BINARY_DIR})
  get_filename_component(PROTO_FILE_DIR "${PROTO_PATH}" PATH)
  get_filename_component(PROTO_FILE_BASE_NAME "${PROTO_PATH}" NAME_WE)

  set(GEN_FILE_H
      "${PROTO_OUT_DIR}/${PROTO_FILE_DIR}/${PROTO_FILE_BASE_NAME}.pb.h")
  set(GEN_FILE_CC
      "${PROTO_OUT_DIR}/${PROTO_FILE_DIR}/${PROTO_FILE_BASE_NAME}.pb.cc")
  list(APPEND GEN_FILES "${GEN_FILE_H}")
  list(APPEND GEN_FILES "${GEN_FILE_CC}")

  # To avoid rebuilding everytime we run "cmake", check if regenrting is
  # actually required
  _valkey_search_is_proto_gen_required(${PROTO_PATH} ${GEN_FILE_H}
                                       GEN_H_REQUIRED)
  _valkey_search_is_proto_gen_required(${PROTO_PATH} ${GEN_FILE_CC}
                                       GEN_CC_REQUIRED)
  if(${GEN_CC_REQUIRED} OR ${GEN_FILE_H})
    message(STATUS "Generating files from ${PROTO_PATH}")
    execute_process(
      COMMAND ${protoc_EXE} --cpp_out=${PROTO_OUT_DIR} -I${CMAKE_SOURCE_DIR}
              ${PROTO_PATH}
      RESULT_VARIABLE protoc_RES
      OUTPUT_VARIABLE protoc_STDOUT
      ERROR_VARIABLE protoc_STDERR)

    if(NOT protoc_RES EQUAL 0)
      message(WARNING "Failed to generate protobuf files.")
      message(WARNING "STDOUT: ${protoc_STDOUT}")
      message(WARNING "STDERR: ${protoc_STDERR}")
      message(FATAL_ERROR "Exiting")
    endif()
  else()
    foreach(GENERATED_FILE ${GEN_FILES})
      message(STATUS "Generated file: ${GENERATED_FILE} is up-to-date")
    endforeach()
  endif()

  add_library(${TARGET_NAME} STATIC ${GEN_FILES})
  valkey_search_target_update_compile_flags(${TARGET_NAME})
  target_link_libraries(${TARGET_NAME} PUBLIC ${Protobuf_LIBRARIES})
  target_include_directories(${TARGET_NAME} PUBLIC ${CMAKE_BINARY_DIR})
  target_include_directories(${TARGET_NAME} PUBLIC ${PROTO_OUT_DIR})
  target_include_directories(${TARGET_NAME} PUBLIC ${Protobuf_INCLUDE_DIRS})
  unset(PROTO_OUT_DIR CACHE)
  message(STATUS "Created protobuf target ${TARGET_NAME}")
endfunction()
