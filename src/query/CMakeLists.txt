add_library(query OBJECT
        predicate.cc
        predicate.h
        search.cc
        search.h
        content_resolution.cc
        content_resolution.h
        fanout.cc
        fanout.h
        primary_info_fanout_operation.cc
        primary_info_fanout_operation.h
        cluster_info_fanout_operation.cc
        cluster_info_fanout_operation.h
        response_generator.cc
        response_generator.h
        planner.cc
        planner.h)

target_link_libraries(query PRIVATE mydeps)
target_include_directories(query PRIVATE .)