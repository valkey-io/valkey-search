"""
This module contains useful functions and macros for building redis modules.
"""

def redis_module_library(name, deps, additional_link_opts = [], **kwargs):
    """
    Use redis_module_library to build a .so file for to use as a Redis module.

    redis_module_library ensures only necessary symbols are exported and
    prevents linker errors caused by module SDK loading in multiple packages.
    The output will be statically linked to ensure consistency.
    """
    native.cc_shared_library(
        name = name,
        # TODO(b/317402988) Investigate remaining dynamic linkages.
        user_link_flags = [
            # Prevent accidental symbol collisions by only exporting necessary
            # symbols. See yaqs.corp.google.com/eng/q/3879504732816932864.
            "-Wl,--version-script=$(location //vmsdk:versionscript.lds)",
        ] + additional_link_opts,
        additional_linker_inputs = [
            "//vmsdk:versionscript.lds",
        ],
        deps = [
            "//vmsdk/src:memory_allocation_overrides",
        ] + deps,
        **kwargs
    )


