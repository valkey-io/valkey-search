add_library (expr OBJECT
        expr.cc
        expr.h
        value.cc
        value.h
)

target_include_directories(expr PRIVATE .)
target_link_libraries(expr PUBLIC absl::status absl::statusor)
target_link_libraries(expr PUBLIC mydeps)