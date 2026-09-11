add_subdirectory(simsimd)
add_subdirectory(hdrhistogram_c)
add_subdirectory(hnswlib)
add_subdirectory(snowball)
add_subdirectory(icu)

# third_party/svslib (SVS runtime FetchContent) is not wired here yet.
# Phase 2 replaces FetchContent with a git submodule; there is no benefit to
# fetching the pre-built runtime tarball while vector_svs itself is not
# compiled (see src/indexes/CMakeLists.txt).
