load("@pip//:requirements.bzl", "requirement")

package(
    default_applicable_licenses=["//:license"],
    default_visibility=["//testing:__subpackages__"],
)

py_test(
    name="stability_test",
    srcs=["stability_test.py"],
    deps=[
        ":stability_runner",
        ":utils",
        requirement("valkey"),
        requirement("absl-py"),
    ],
    data=[
        "//src:valkeysearch",
    ],
    timeout="eternal",  # 3600 seconds, for slow machines
)

py_test(
    name="vector_search_integration_test",
    srcs=["vector_search_integration_test.py"],
    deps=[
        ":utils",
        requirement("valkey"),
        requirement("absl-py"),
        requirement("numpy"),
    ],
    data=[
        "//src:valkeysearch",
    ],
)

py_library(
    name="stability_runner",
    srcs=["stability_runner.py"],
    deps=[
        ":utils",
        requirement("valkey"),
    ],
)

py_library(
    name="utils",
    srcs=["utils.py"],
    deps=[
        requirement("valkey"),
        requirement("numpy"),
    ],
)
