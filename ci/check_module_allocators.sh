#!/bin/bash -e
#
# Copyright (c) 2025, valkey-search contributors
# All rights reserved.
# SPDX-License-Identifier: BSD 3-Clause
#
# Guards the four invariants that keep the module's heap on ValkeyModule_Alloc:
#   1. static initializers are deferred until ValkeyModule_Alloc exists
#   2. no code calls the system allocator directly except two vendored C libs
#   3. no C++ heap object can cross a DSO boundary (libstdc++ is static)
#   4. nothing exported can collide with libstdc++.so.6
#
# Each is verified to fail on a build that violates it; a silent pass here means
# the module would crash at load or corrupt the heap.
#
# Usage: check_module_allocators.sh <module.so> <build-dir>

MODULE_SO="$1"
BUILD_DIR="$2"

if [ ! -f "${MODULE_SO}" ]; then
    echo "check_module_allocators: no such file: ${MODULE_SO}" >&2
    exit 1
fi

FAILED=0

#
# Check 1: static initializers are deferred.
#
# vmsdk/deferred_init.lds moves the module's C++ static initializers out of
# .init_array into .vmsdk_init_array, so the dynamic loader does not run them at
# dlopen() -- before ValkeyModule_Alloc exists. What is left in .init_array is
# crtbegin's frame_dummy, a single entry, which must keep running at load.
#
INIT_ARRAY_SZ=$(readelf -d "${MODULE_SO}" | awk '/\(INIT_ARRAYSZ\)/ {print $3}')
: "${INIT_ARRAY_SZ:=0}"
DEFERRED_HEX=$(readelf -S -W "${MODULE_SO}" | awk '
    { for (i = 1; i <= NF; i++)
        if ($i == ".vmsdk_init_array") { print $(i + 4); exit } }')
if [ -n "${DEFERRED_HEX}" ]; then
    DEFERRED_SZ=$((16#${DEFERRED_HEX}))
else
    DEFERRED_SZ=0
fi

if [ "${INIT_ARRAY_SZ}" -gt 8 ]; then
    echo "FAIL: ${MODULE_SO} has DT_INIT_ARRAYSZ=${INIT_ARRAY_SZ} (> 8 bytes)." >&2
    echo "      Static initializers would run at dlopen(), before" >&2
    echo "      ValkeyModule_Alloc is established. Is vmsdk/deferred_init.lds" >&2
    echo "      still being passed to the linker?" >&2
    FAILED=1
fi
if [ "${DEFERRED_SZ}" -eq 0 ]; then
    echo "FAIL: ${MODULE_SO} has no .vmsdk_init_array section, or it is empty." >&2
    echo "      vmsdk/deferred_init.lds did not take effect." >&2
    FAILED=1
fi
if [ "${FAILED}" -eq 0 ]; then
    echo "check_module_allocators: $((DEFERRED_SZ / 8)) static initializers deferred, \
$((INIT_ARRAY_SZ / 8)) left at load time"
fi

#
# Check 2: no new direct calls to the system allocator.
#
# The module routes C++ allocation through replaced operator new/delete and the
# __wrap_* macros. C code that calls malloc/free directly bypasses that, and its
# pointers can never be handed to ValkeyModule_Free. Two vendored C libraries do
# this today; both allocate and free entirely within themselves. The allowlist
# exists so that the set cannot silently grow.
#
# References from .data (the __real_malloc function pointers, which are the
# deliberate fallback) are not call sites and are ignored.
#
ALLOWED="libicuuc.a libhdrhistogram_c.a"

for archive in $(find "${BUILD_DIR}/src" "${BUILD_DIR}/vmsdk" \
                      "${BUILD_DIR}/third_party" "${BUILD_DIR}/icu/lib" \
                      -name "*.a" 2>/dev/null | sort); do
    base=$(basename "${archive}")
    case " ${ALLOWED} " in
        *" ${base} "*) continue ;;
    esac
    offenders=$(objdump -r "${archive}" 2>/dev/null | awk '
        /file format/            { obj = $1; next }
        /^RELOCATION RECORDS FOR/ { sec = $4; next }
        sec ~ /^\[\.text/ {
            sym = $3
            sub(/[-+]0x[0-9a-f]+$/, "", sym)
            if (sym == "malloc" || sym == "calloc" || sym == "realloc" ||
                sym == "free" || sym == "posix_memalign" ||
                sym == "aligned_alloc" || sym == "valloc") {
                print "    " obj " calls " sym
            }
        }' | sort -u)
    if [ -n "${offenders}" ]; then
        echo "FAIL: ${base} calls the system allocator directly:" >&2
        echo "${offenders}" >&2
        FAILED=1
    fi
done

#
# Check 3: no C++ heap object can cross a DSO boundary.
#
# The operator new/delete replacements only cover code linked into this .so.
# If the module still called into libstdc++.so, an object allocated there (by
# std::getline, std::filesystem::path, std::locale::name, ...) would be freed
# here with ValkeyModule_Free -- a jemalloc free of a libc malloc pointer.
# Linking libstdc++ statically removes the boundary; this confirms it stayed
# removed.
#
GLIBCXX_UNDEF=$(nm -D --undefined-only "${MODULE_SO}" 2>/dev/null |
                grep -c "GLIBCXX" || true)
if [ "${GLIBCXX_UNDEF}" -ne 0 ]; then
    echo "FAIL: ${MODULE_SO} has ${GLIBCXX_UNDEF} undefined GLIBCXX symbols," >&2
    echo "      so it is calling into libstdc++.so. Objects allocated there" >&2
    echo "      would be freed here with ValkeyModule_Free and crash. Is" >&2
    echo "      -static-libstdc++ still being passed to the linker?" >&2
    nm -D --undefined-only "${MODULE_SO}" | grep "GLIBCXX" | head -5 >&2
    FAILED=1
fi

#
# Check 4: nothing the module exports can collide with libstdc++.so.6.
#
# libstdc++ is linked statically, but exported symbols still take part in
# dynamic symbol resolution. The dangerous ones are the locale facet ids
# (std::num_put<char>::id and friends): they are STB_GNU_UNIQUE, which the
# dynamic linker unifies process-wide even for an RTLD_LOCAL dlopen. If
# libstdc++.so.6 is also present -- it arrives with any other C++ module, and
# valkey-json is loaded before search in the integration tests -- our facet ids
# and its become one object while the facet arrays stay separate, so the first
# ostream insertion dereferences the wrong facet and segfaults at module load.
#
# -Wl,--exclude-libs,ALL is what keeps this list empty. nm prints
# libstdc++.so.6's names with an @@GLIBCXX version suffix and ours without, so
# the suffix is stripped before comparing.
#
LIBSTDCXX=$(gcc -print-file-name=libstdc++.so.6 2>/dev/null || true)
if [ -n "${LIBSTDCXX}" ] && [ -f "${LIBSTDCXX}" ]; then
    CLASHES=$(comm -12 \
        <(nm -D --defined-only "${MODULE_SO}" |
              awk '{sub(/@@?.*$/, "", $3); print $3}' | sort -u) \
        <(nm -D --defined-only "${LIBSTDCXX}" |
              awk '{sub(/@@?.*$/, "", $3); print $3}' | sort -u))
    if [ -n "${CLASHES}" ]; then
        NCLASH=$(echo "${CLASHES}" | wc -l)
        echo "FAIL: ${MODULE_SO} exports ${NCLASH} symbol(s) that" >&2
        echo "      libstdc++.so.6 also defines. The locale facet ids among" >&2
        echo "      them are STB_GNU_UNIQUE and get merged across the two" >&2
        echo "      libstdc++ copies as soon as another C++ module is loaded," >&2
        echo "      crashing at module load. Is -Wl,--exclude-libs,ALL still" >&2
        echo "      being passed to the linker?" >&2
        echo "${CLASHES}" | head -5 | sed 's/^/    /' >&2
        FAILED=1
    fi
fi

if [ "${FAILED}" -ne 0 ]; then
    echo "" >&2
    echo "See vmsdk/src/memory_allocation_overrides.h for how module memory" >&2
    echo "is expected to be allocated." >&2
    exit 1
fi

echo "check_module_allocators: OK"
