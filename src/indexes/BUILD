package(
    default_applicable_licenses = ["//:license"],
    default_visibility = ["//:__subpackages__"],
)

cc_library(
    name = "index_base",
    hdrs = [
        "index_base.h",
    ],
    deps = [
        "//src:index_schema_cc_proto",
        "//src:rdb_serialization",
        "//src/utils:string_interning",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
    ],
)

vector_opt = [
    # HNSWlib does not guarantee memory alignment for it's storage. We need
    # to disable this check otherwise we will get a SIGILL.
    "-fno-sanitize=alignment",
    "-fexceptions",
    "-ftree-vectorize",
    "-fassociative-math",
    "-fno-signed-zeros",
    "-fno-trapping-math",
]

cc_library(
    name = "vector_base",
    srcs = [
        "vector_base.cc",
    ],
    hdrs = [
        "vector_base.h",
    ],
    copts = vector_opt,
    features = ["-use_header_modules"],
    deps = [
        ":index_base",
        ":numeric",
        ":tag",
        "//src:attribute_data_type",
        "//src:index_schema_cc_proto",
        "//src:rdb_serialization",
        "//src:vector_externalizer",
        "//src/query:predicate",
        "//src/utils:allocator",
        "//src/utils:intrusive_ref_count",
        "//src/utils:string_interning",
        "//third_party/hnswlib:hnswlib_vmsdk",
        "//third_party/hnswlib:iostream",
        "//vmsdk/src:log",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:type_conversions",
        "//vmsdk/src/status:status_macros",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_library(
    name = "vector_hnsw",
    srcs = [
        "vector_hnsw.cc",
    ],
    hdrs = [
        "vector_hnsw.h",
    ],
    copts = vector_opt,
    features = ["-use_header_modules"],
    deps = [
        ":index_base",
        ":vector_base",
        "//src:attribute_data_type",
        "//src:metrics",
        "//src:rdb_serialization",
        "//src/utils:string_interning",
        "//third_party/hnswlib:hnswlib_vmsdk",
        "//vmsdk/src:log",
        "//vmsdk/src:memory_allocation_overrides",
        "//vmsdk/src:utils",
        "//vmsdk/src/status:status_macros",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "numeric",
    srcs = [
        "numeric.cc",
    ],
    hdrs = [
        "numeric.h",
    ],
    deps = [
        ":index_base",
        "//src:rdb_serialization",
        "//src/query:predicate_header",
        "//src/utils:segment_tree",
        "//src/utils:string_interning",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_library(
    name = "tag",
    srcs = [
        "tag.cc",
    ],
    hdrs = [
        "tag.h",
    ],
    deps = [
        ":index_base",
        "//src:rdb_serialization",
        "//src/query:predicate_header",
        "//src/utils:patricia_tree",
        "//src/utils:string_interning",
        "//vmsdk/src:managed_pointers",
        "//vmsdk/src:type_conversions",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_library(
    name = "vector_flat",
    srcs = [
        "vector_flat.cc",
    ],
    hdrs = [
        "vector_flat.h",
    ],
    copts = vector_opt,
    features = ["-use_header_modules"],
    deps = [
        ":index_base",
        ":vector_base",
        "//src:attribute_data_type",
        "//src:metrics",
        "//src:rdb_serialization",
        "//src/utils:string_interning",
        "//third_party/hnswlib:hnswlib_vmsdk",
        "//vmsdk/src:log",
        "//vmsdk/src:memory_allocation_overrides",
        "//vmsdk/src/status:status_macros",
        "//vmsdk/src/valkey_module_api:valkey_module",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
    ],
)
