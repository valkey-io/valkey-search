set(MODULE_BASE_DIR "${CMAKE_BINARY_DIR}/.deps")
set(MODULES_BIN_DIR "${MODULE_BASE_DIR}/install/bin")
set(MODULES_LIB_DIR "${CMAKE_BINARY_DIR}/.deps/install/lib")

set(grpc_cpp_plugin_location ${MODULES_BIN_DIR}/grpc_cpp_plugin)
set(Protobuf_PROTOC_EXECUTABLE ${MODULES_BIN_DIR}/protoc)

message(STATUS "gRPC C++ plugin: ${grpc_cpp_plugin_location}")
message(STATUS "protoc: ${Protobuf_PROTOC_EXECUTABLE}")

# Protobuf compiler and library
set(protobuf_generate_PROTOC_EXE ${Protobuf_PROTOC_EXECUTABLE})
message(
  STATUS
    "protobuf_generate_PROTOC_EXE is set to ${protobuf_generate_PROTOC_EXE}")

find_library(
  PROTOBUF_LIBS protobuf
  PATHS ${MODULES_LIB_DIR}
  NO_DEFAULT_PATH REQUIRED)
message(STATUS "PROTOBUF_LIBS => ${PROTOBUF_LIBS}")

# TODO: we should narrow down this list, some of the libraries here some useless
# to us (e.g. Rust) - removing them however, causing an undefined symbol error.
# Probably we should build gRPC differently to resolve this issue
set(ABSL_LIBS_NAME
    absl_bad_any_cast_impl
    absl_bad_optional_access
    absl_bad_variant_access
    absl_base
    absl_city
    absl_civil_time
    absl_cord
    absl_cord_internal
    absl_cordz_functions
    absl_cordz_handle
    absl_cordz_info
    absl_cordz_sample_token
    absl_crc32c
    absl_crc_cord_state
    absl_crc_cpu_detect
    absl_crc_internal
    absl_debugging_internal
    absl_demangle_rust
    absl_decode_rust_punycode
    absl_demangle_internal
    absl_die_if_null
    absl_examine_stack
    absl_exponential_biased
    absl_failure_signal_handler
    absl_flags_commandlineflag
    absl_flags_commandlineflag_internal
    absl_flags_config
    absl_flags_internal
    absl_flags_marshalling
    absl_flags_parse
    absl_flags_private_handle_accessor
    absl_flags_program_name
    absl_flags_reflection
    absl_flags_usage
    absl_flags_usage_internal
    absl_graphcycles_internal
    absl_hash
    absl_hashtablez_sampler
    absl_int128
    absl_kernel_timeout_internal
    absl_leak_check
    absl_log_entry
    absl_log_flags
    absl_log_globals
    absl_log_initialize
    absl_log_internal_check_op
    absl_log_internal_conditions
    absl_log_internal_fnmatch
    absl_log_internal_format
    absl_log_internal_globals
    absl_log_internal_log_sink_set
    absl_log_internal_message
    absl_log_internal_nullguard
    absl_log_internal_proto
    absl_log_severity
    absl_log_sink
    absl_low_level_hash
    absl_malloc_internal
    absl_periodic_sampler
    absl_poison
    absl_random_distributions
    absl_random_internal_distribution_test_util
    absl_random_internal_platform
    absl_random_internal_pool_urbg
    absl_random_internal_randen
    absl_random_internal_randen_hwaes
    absl_random_internal_randen_hwaes_impl
    absl_random_internal_randen_slow
    absl_random_internal_seed_material
    absl_random_seed_gen_exception
    absl_random_seed_sequences
    absl_raw_hash_set
    absl_raw_logging_internal
    absl_scoped_set_env
    absl_spinlock_wait
    absl_stacktrace
    absl_status
    absl_statusor
    absl_str_format_internal
    absl_strerror
    absl_string_view
    absl_strings
    absl_strings_internal
    absl_symbolize
    absl_synchronization
    absl_throw_delegate
    absl_time
    absl_time_zone
    absl_utf8_for_code_point
    absl_vlog_config_internal)

set(ABSL_LIBS "")
message(STATUS "Collecting absl libs")
foreach(LIBNAME ${ABSL_LIBS_NAME})
  find_library(
    __LIB ${LIBNAME} REQUIRED
    PATHS ${MODULES_LIB_DIR}
    NO_DEFAULT_PATH)
  list(APPEND ABSL_LIBS ${__LIB})
  message(STATUS "  Found library ${__LIB}")
  unset(__LIB CACHE)
endforeach()

set(GRPC_LIBS_NAME
    grpc++
    grpc++_alts
    grpc++_error_details
    grpc++_reflection
    grpc++_unsecure
    grpc
    grpc_authorization_provider
    grpc_plugin_support
    grpc_unsecure
    grpcpp_channelz)

message(STATUS "Collecting grpc libs")
set(GRPC_LIBS "")
foreach(LIBNAME ${GRPC_LIBS_NAME})
  find_library(
    __LIB ${LIBNAME} REQUIRED
    PATHS ${MODULES_LIB_DIR}
    NO_DEFAULT_PATH)
  list(APPEND GRPC_LIBS ${__LIB})
  message(STATUS "  Found library ${__LIB}")
  unset(__LIB CACHE)
endforeach()

find_library(
  LIBGPR gpr REQUIRED
  PATHS ${MODULES_LIB_DIR}
  NO_DEFAULT_PATH)

# upb libs
set(UPB_LIBS_NAMES
    upb
    upb_base_lib
    upb_json_lib
    upb_mem_lib
    upb_message_lib
    upb_mini_descriptor_lib
    upb_textformat_lib
    upb_wire_lib)

message(STATUS "Collecting upb libs")
set(UPB_LIBS "")
foreach(LIBNAME ${UPB_LIBS_NAMES})
  find_library(
    __LIB ${LIBNAME} REQUIRED
    PATHS ${MODULES_LIB_DIR}
    NO_DEFAULT_PATH)
  list(APPEND UPB_LIBS ${__LIB})
  message(STATUS "  Found library ${__LIB}")
  unset(__LIB CACHE)
endforeach()

# UTF-8 libs
set(UTF8_LIBS_NAMES utf8_range utf8_range_lib utf8_validity)

message(STATUS "Collecting utf8 libs")
set(UTF8_LIBS "")
foreach(LIBNAME ${UTF8_LIBS_NAMES})
  find_library(
    __LIB ${LIBNAME} REQUIRED
    PATHS ${MODULES_LIB_DIR}
    NO_DEFAULT_PATH)
  list(APPEND UTF8_LIBS ${__LIB})
  message(STATUS "  Found library ${__LIB}")
  unset(__LIB CACHE)
endforeach()

set(GTEST_LIBS_NAME gtest gtest_main gmock)
message(STATUS "Collecting GTest libs")
set(GTEST_LIBS "")
foreach(LIBNAME ${GTEST_LIBS_NAME})
  find_library(
    __LIB ${LIBNAME} REQUIRED
    PATHS ${MODULES_LIB_DIR}
    NO_DEFAULT_PATH)
  list(APPEND GTEST_LIBS ${__LIB})
  message(STATUS "  Found library ${__LIB}")
  unset(__LIB CACHE)
endforeach()


find_library(
  LIBZ z REQUIRED
  PATHS ${MODULES_LIB_DIR}
  NO_DEFAULT_PATH)

message(STATUS "LIBZ => ${LIBZ}")

find_library(
  LIBRE2 re2 REQUIRED
  PATHS ${MODULES_LIB_DIR}
  NO_DEFAULT_PATH)
message(STATUS "LIBRE2 => ${LIBRE2}")

find_library(
  LIBCARES cares REQUIRED
  PATHS ${MODULES_LIB_DIR}
  NO_DEFAULT_PATH)
message(STATUS "LIBCARES => ${LIBCARES}")

find_library(
  LIBADDRESS_SORTING address_sorting REQUIRED
  PATHS ${MODULES_LIB_DIR}
  NO_DEFAULT_PATH)
message(STATUS "LIBADDRESS_SORTING => ${LIBADDRESS_SORTING}")

find_package(OpenSSL REQUIRED)
message(STATUS "OpenSSL include dir: ${OPENSSL_INCLUDE_DIR}")
message(STATUS "OpenSSL libraries: ${OPENSSL_LIBRARIES}")
include_directories(${OPENSSL_INCLUDE_DIR})

list(APPEND THIRD_PARTY_LIBS ${ABSL_LIBS})
list(APPEND THIRD_PARTY_LIBS ${PROTOBUF_LIBS})
list(APPEND THIRD_PARTY_LIBS ${GRPC_LIBS})
list(APPEND THIRD_PARTY_LIBS ${LIBGPR})
list(APPEND THIRD_PARTY_LIBS ${UPB_LIBS})
list(APPEND THIRD_PARTY_LIBS ${LIBZ})
list(APPEND THIRD_PARTY_LIBS ${LIBRE2})
list(APPEND THIRD_PARTY_LIBS ${LIBCARES})
list(APPEND THIRD_PARTY_LIBS ${UTF8_LIBS})
list(APPEND THIRD_PARTY_LIBS ${LIBADDRESS_SORTING})
list(APPEND THIRD_PARTY_LIBS ${OPENSSL_LIBRARIES})
list(APPEND THIRD_PARTY_LIBS ${LIB_HIGHWAY_HASHING})
