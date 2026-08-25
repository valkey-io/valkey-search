if(NOT TARGET highwayhash)
    find_path(HIGHWAYHASH_INCLUDE_DIR highwayhash/highwayhash.h)
    find_library(HIGHWAYHASH_LIBRARY NAMES highwayhash libhighwayhash)
    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(highwayhash DEFAULT_MSG HIGHWAYHASH_INCLUDE_DIR HIGHWAYHASH_LIBRARY)

    if(highwayhash_FOUND)
        add_library(highwayhash UNKNOWN IMPORTED)
        set_target_properties(highwayhash PROPERTIES
                IMPORTED_LOCATION "${HIGHWAYHASH_LIBRARY}"
                INTERFACE_INCLUDE_DIRECTORIES "${HIGHWAYHASH_INCLUDE_DIR}")
    endif()
endif()
