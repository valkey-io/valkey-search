# Fuzzing

AFL persistent mode fuzzers for valkey-search internals.

## Directory Structure

```
fuzzing/
  CMakeLists.txt              # Adds common/ + each target subdirectory
  run_all_fuzzers.sh          # Build, run all targets, collect results
  common/
    fuzz_common.h/cc          # Shared stubs, FuzzIndexSchema, init functions
    CMakeLists.txt            # Builds fuzz_common static library
  query_parser/
    fuzz_query_parser.cc      # Harness: PreParseQueryString() + AFL main()
    CMakeLists.txt            # Builds fuzz_query_parser executable
    seeds/                    # Seed inputs (checked into git)
    query.dict                # AFL dictionary for query syntax
  ft_create_parser/
    fuzz_ft_create_parser.cc  # Harness: ParseFTCreateArgs() + AFL main()
    CMakeLists.txt            # Builds fuzz_ft_create_parser executable
    seeds/                    # Seed inputs (FT.CREATE arg strings)
    ft_create.dict            # AFL dictionary for FT.CREATE keywords
  ft_search_parser/
    fuzz_ft_search_parser.cc  # Harness: SearchCommand::ParseCommand() + AFL main()
    CMakeLists.txt            # Builds fuzz_ft_search_parser executable
    seeds/                    # Seed inputs (FT.SEARCH option strings)
    ft_search.dict            # AFL dictionary for FT.SEARCH keywords
  ft_aggregate_parser/
    fuzz_ft_aggregate_parser.cc # Harness: CreateAggregateParser().Parse() + AFL main()
    CMakeLists.txt            # Builds fuzz_ft_aggregate_parser executable
    seeds/                    # Seed inputs (FT.AGGREGATE option strings)
    ft_aggregate.dict         # AFL dictionary for FT.AGGREGATE keywords
  expr/
    fuzz_expr.cc              # Harness: Expression::Compile() + AFL main()
    CMakeLists.txt            # Builds fuzz_expr executable
    seeds/                    # Seed inputs (expression strings)
    expr.dict                 # AFL dictionary for expression syntax
```

## Building

### Standard Build (no instrumentation)

Builds the fuzz harnesses without AFL instrumentation. Useful for manual testing.

```bash
./build.sh
# Binaries: .build-release/tests/fuzz_query_parser
#           .build-release/tests/fuzz_ft_create_parser
#           .build-release/tests/fuzz_ft_search_parser
#           .build-release/tests/fuzz_ft_aggregate_parser
#           .build-release/tests/fuzz_expr
```

### Instrumented Build (for AFL fuzzing)

The `--fuzz` flag sets `CC=afl-gcc CXX=afl-g++` and uses a dedicated `.build-fuzz` directory:

```bash
./build.sh --fuzz
# Binaries: .build-fuzz/tests/fuzz_query_parser
#           .build-fuzz/tests/fuzz_ft_create_parser
#           .build-fuzz/tests/fuzz_ft_search_parser
#           .build-fuzz/tests/fuzz_ft_aggregate_parser
#           .build-fuzz/tests/fuzz_expr
```

For AFL + AddressSanitizer (catches memory errors during fuzzing):

```bash
./build.sh --fuzz --asan
# Binaries: .build-fuzz-asan/tests/fuzz_query_parser
#           .build-fuzz-asan/tests/fuzz_ft_create_parser
#           .build-fuzz-asan/tests/fuzz_ft_search_parser
#           .build-fuzz-asan/tests/fuzz_ft_aggregate_parser
#           .build-fuzz-asan/tests/fuzz_expr
```

Uses `afl-gcc` mode. The codebase has compatibility issues with clang-17, so `afl-clang-fast` is not currently supported.

## Running

### Manual Testing (no instrumentation needed)

```bash
echo "*" | .build-release/tests/fuzz_query_parser
echo "@field:value" | .build-release/tests/fuzz_query_parser
echo "(hello|world) @tag:{a|b}" | .build-release/tests/fuzz_query_parser
```

### Single Target

```bash
afl-fuzz -i fuzzing/query_parser/seeds \
         -o fuzzing/afl_out/query_parser \
         -x fuzzing/query_parser/query.dict \
         -- .build-fuzz/tests/fuzz_query_parser
```

### All Targets

Build and run in one step:

```bash
./fuzzing/run_all_fuzzers.sh --build --duration 300
```

Or run against an existing instrumented build:

```bash
./fuzzing/run_all_fuzzers.sh --duration 300
```

Options:
- `--build` — build with AFL instrumentation before fuzzing
- `--asan` — enable AddressSanitizer (`AFL_USE_ASAN=1`)
- `--duration <seconds>` — time per target (default: 300)
- `--build-dir <path>` — build directory (default: `.build-fuzz`)

Results are saved to `fuzzing/fuzz_results.txt`.

## Architecture

### Common Library (`fuzz_common`)

Static library providing shared infrastructure for all fuzz targets:

- **Opaque struct definitions** for Valkey module types (`ValkeyModuleCtx`, `ValkeyModuleString`, etc.)
- **Stub implementations** for Valkey module API function pointers (string ops, context management, DB operations)
- **`FuzzIndexSchema`** — `IndexSchema` subclass with no-op virtual overrides (no gtest/gmock dependency)
- **`FuzzSearchParameters`** — concrete `SearchParameters` for fuzzing
- **`InitValkeyModuleStubs()`** — sets up basic API stubs
- **`InitFuzzEnvironment()`** — full init including singletons and index schema

### Per-Target Harnesses

Each target is a thin harness that includes `fuzz_common.h`, calls `InitFuzzEnvironment()`, and runs the AFL persistent mode loop.

All targets use AFL persistent mode (`__AFL_LOOP(1000)`) for 100-1000x performance over fork-based fuzzing.

## Adding a New Fuzz Target

1. Create `fuzzing/<target_name>/` directory
2. Add `fuzz_<target_name>.cc` with harness function + AFL main loop
3. Add `CMakeLists.txt`:
   ```cmake
   add_executable(fuzz_<target_name> fuzz_<target_name>.cc)
   target_link_libraries(fuzz_<target_name> PRIVATE fuzz_common)
   set_target_properties(fuzz_<target_name> PROPERTIES
       RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tests")
   if(UNIX AND NOT APPLE)
       target_link_libraries(fuzz_<target_name> PRIVATE lib_to_add_end_group_flag)
   endif()
   if(SAN_BUILD)
       target_link_options(fuzz_<target_name> PRIVATE "-fsanitize=${SAN_BUILD}")
   endif()
   ```
4. Add `seeds/` directory with appropriate seed files
5. Add `<name>.dict` if applicable
6. Add `add_subdirectory(<target_name>)` to `fuzzing/CMakeLists.txt`
7. Add target entry to `FUZZ_TARGETS` array in `run_all_fuzzers.sh`
