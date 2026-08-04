include(FetchContent)

set(SVS_URL
    "https://github.com/intel/ScalableVectorSearch/releases/download/v0.4.0/svs-cpp-runtime-bindings.tar.gz"
    CACHE STRING "URL to fetch SVS runtime library")

FetchContent_Declare(svs URL "${SVS_URL}")
FetchContent_MakeAvailable(svs)
set(SVS_PREFIX_PATH "${svs_SOURCE_DIR}" CACHE INTERNAL "")
