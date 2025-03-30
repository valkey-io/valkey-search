# TODO: we should narrow down this list, some of the libraries here some useless
# to us (e.g. Rust) - removing them however, causing an undefined symbol error.
# Probably we should build gRPC differently to resolve this issue
set(ABSL_ALL_TARGETS
    absl::bad_any_cast_impl
    absl::bad_optional_access
    absl::bad_variant_access
    absl::base
    absl::city
    absl::civil_time
    absl::cord
    absl::cord_internal
    absl::cordz_functions
    absl::cordz_handle
    absl::cordz_info
    absl::cordz_sample_token
    absl::crc32c
    absl::crc_cord_state
    absl::crc_cpu_detect
    absl::crc_internal
    absl::debugging_internal
    absl::demangle_rust
    absl::decode_rust_punycode
    absl::demangle_internal
    absl::die_if_null
    absl::examine_stack
    absl::exponential_biased
    absl::failure_signal_handler
    absl::flags_commandlineflag
    absl::flags_commandlineflag_internal
    absl::flags_config
    absl::flags_internal
    absl::flags_marshalling
    absl::flags_parse
    absl::flags_private_handle_accessor
    absl::flags_program_name
    absl::flags_reflection
    absl::flags_usage
    absl::flags_usage_internal
    absl::graphcycles_internal
    absl::hash
    absl::hashtablez_sampler
    absl::int128
    absl::kernel_timeout_internal
    absl::leak_check
    absl::log_entry
    absl::log_flags
    absl::log_globals
    absl::log_initialize
    absl::log_internal_check_op
    absl::log_internal_conditions
    absl::log_internal_fnmatch
    absl::log_internal_format
    absl::log_internal_globals
    absl::log_internal_log_sink_set
    absl::log_internal_message
    absl::log_internal_nullguard
    absl::log_internal_proto
    absl::log_severity
    absl::log_sink
    absl::low_level_hash
    absl::malloc_internal
    absl::periodic_sampler
    absl::poison
    absl::random_distributions
    absl::random_internal_distribution_test_util
    absl::random_internal_platform
    absl::random_internal_pool_urbg
    absl::random_internal_randen
    absl::random_internal_randen_hwaes
    absl::random_internal_randen_hwaes_impl
    absl::random_internal_randen_slow
    absl::random_internal_seed_material
    absl::random_seed_gen_exception
    absl::random_seed_sequences
    absl::raw_hash_set
    absl::raw_logging_internal
    absl::scoped_set_env
    absl::spinlock_wait
    absl::stacktrace
    absl::status
    absl::statusor
    absl::str_format_internal
    absl::strerror
    absl::string_view
    absl::strings
    absl::strings_internal
    absl::symbolize
    absl::synchronization
    absl::throw_delegate
    absl::time
    absl::time_zone
    absl::utf8_for_code_point
    absl::vlog_config_internal)

# Any target wishes to build against Abseil, should link against our interface
# library "absl::all"
message(STATUS "Collecting absl libs. CMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}")

find_package(absl REQUIRED CONFIG)
add_library(absl::all INTERFACE IMPORTED GLOBAL)
get_target_property(__ASBL_INCLUDE_PATH absl::base
                    INTERFACE_INCLUDE_DIRECTORIES)
set(ABSL_INCLUDE_PATH ${__ASBL_INCLUDE_PATH})

target_include_directories(absl::all INTERFACE "${__ASBL_INCLUDE_PATH}")
message(STATUS "ASBL_INCLUDE_PATH => ${__ASBL_INCLUDE_PATH}")

set(ABSL_LIBS "")
foreach(ABSL_TARGET ${ABSL_ALL_TARGETS})
  target_link_libraries(absl::all INTERFACE ${ABSL_TARGET})
endforeach()

list(APPEND THIRD_PARTY_LIBS absl::all)
list(APPEND THIRD_PARTY_LIBS highwayhash)

find_package(protobuf REQUIRED CONFIG)
list(APPEND THIRD_PARTY_LIBS protobuf::libprotobuf)
get_target_property(__PROTOBUF_INCLUDE_PATH protobuf::libprotobuf
                    INTERFACE_INCLUDE_DIRECTORIES)
set(PROTOBUF_INCLUDE_PATH ${__PROTOBUF_INCLUDE_PATH})
message(STATUS "PROTOBUF_INCLUDE_PATH => ${__PROTOBUF_INCLUDE_PATH}")
include_directories(${__PROTOBUF_INCLUDE_PATH})

find_package(gRPC REQUIRED CONFIG)
list(APPEND THIRD_PARTY_LIBS gRPC::grpc++)
list(APPEND THIRD_PARTY_LIBS gRPC::grpc)
