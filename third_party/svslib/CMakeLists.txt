include(FetchContent)

# SVS runtime nightly selection.
# Two nightly builds expose different APIs; switch the active set() to match
# the feature branch you are building.

# Exposes: get_memory_usage(), get_memory_breakdown()
# (required by svs-memory-reporting / intel-svs-vamana)
set(SVS_URL
    "https://github.com/intel/ScalableVectorSearch/releases/download/nightly/svs-cpp-runtime-bindings-nightly-2026-08-17.tar.gz"
    CACHE STRING "URL to fetch SVS runtime library")
set(SVS_URL_HASH SHA256=295193173276f1bd8ada478c1636432fcde118ca35694d58bd661b03c640852e)

# Exposes: deferred_compression_threshold, initial_storage_kind,
#          get_current_storage_kind (required by svs-deferred-compression-rebased)
# set(SVS_URL
#     "https://github.com/intel/ScalableVectorSearch/releases/download/nightly/svs-cpp-runtime-bindings-nightly-2026-06-23-1416.tar.gz"
#     CACHE STRING "URL to fetch SVS runtime library")
# set(SVS_URL_HASH SHA256=c09e2800805594914be2bfd9c7610de20b3d52b15321437140e76e924418cbac)

FetchContent_Declare(svs URL "${SVS_URL}" URL_HASH "${SVS_URL_HASH}")
FetchContent_MakeAvailable(svs)
set(SVS_PREFIX_PATH "${svs_SOURCE_DIR}" CACHE INTERNAL "")
