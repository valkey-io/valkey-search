include(FetchContent)

set(SVS_URL
    "https://github.com/intel/ScalableVectorSearch/releases/download/nightly/svs-cpp-runtime-bindings-nightly-2026-08-17.tar.gz"
    CACHE STRING "URL to fetch SVS runtime library")

FetchContent_Declare(svs URL "${SVS_URL}"
                        URL_HASH SHA256=295193173276f1bd8ada478c1636432fcde118ca35694d58bd661b03c640852e)
FetchContent_MakeAvailable(svs)
set(SVS_PREFIX_PATH "${svs_SOURCE_DIR}" CACHE INTERNAL "")
