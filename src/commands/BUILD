package(
    default_applicable_licenses = ["//:license"],
    default_visibility = ["//:__subpackages__"],
)

cc_library(
    name = "commands",
    srcs = [
        "ft_create.cc",
        "ft_dropindex.cc",
        "ft_info.cc",
        "ft_list.cc",
        "ft_search.cc",
    ],
    hdrs = [
        "commands.h",
        "ft_search.h",
    ],
    linkopts = [
        # The Redis module SDK defines many global variables in the header file
        # which violates One Definition Rule (ODR). This causes linker errors
        # if we don't allow multiple definitions.
        "-z muldefs",
    ],
    deps = [
        ":ft_create_parser",
        ":ft_search_parser",
        "//src:acl",
        "//src:index_schema",
        "//src:index_schema_cc_proto",
        "//src:metrics",
        "//src:schema_manager",
        "//src:valkey_search",
        "//src/indexes:vector_base",
        "//src/query:fanout",
        "//src/query:response_generator",
        "//src/query:search",
        "//vmsdk/src:command_parser",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:type_conversions",
        "//vmsdk/src:utils",
        "//vmsdk/src/status:status_macros",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_library(
    name = "ft_search_parser",
    srcs = ["ft_search_parser.cc"],
    hdrs = ["ft_search_parser.h"],
    deps = [
        ":filter_parser",
        "//src:index_schema",
        "//src:metrics",
        "//src:schema_manager",
        "//src/indexes:index_base",
        "//src/query:search",
        "//vmsdk/src:command_parser",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:type_conversions",
        "//vmsdk/src/status:status_macros",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_library(
    name = "ft_create_parser",
    srcs = ["ft_create_parser.cc"],
    hdrs = ["ft_create_parser.h"],
    deps = [
        "//src:attribute_data_type",
        "//src:index_schema_cc_proto",
        "//src/indexes:index_base",
        "//src/indexes:vector_base",
        "//vmsdk/src:command_parser",
        "//vmsdk/src:type_conversions",
        "//vmsdk/src/status:status_macros",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_library(
    name = "filter_parser",
    srcs = ["filter_parser.cc"],
    hdrs = ["filter_parser.h"],
    deps = [
        "//src:index_schema",
        "//src/indexes:index_base",
        "//src/indexes:numeric",
        "//src/indexes:tag",
        "//src/query:predicate",
        "//vmsdk/src/status:status_macros",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
    ],
)
