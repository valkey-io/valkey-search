#!/bin/bash -e
#
# Copyright (c) 2025, valkey-search contributors
# All rights reserved.
# SPDX-License-Identifier: BSD 3-Clause
#
# Guards the invariants that keep the module's heap on ValkeyModule_Alloc:
#   1. static initializers are deferred until ValkeyModule_Alloc exists
#   2. the module defines the C allocator itself and imports none of it
#   3. no C++ heap object can cross a DSO boundary (libstdc++ is static)
#   4. nothing exported can collide with libstdc++.so.6
#   5. no unhandled libc function hands us memory allocated by libc
#   6. no new shared library dependency appears unreviewed
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

# Symbols the module defines for itself in vmsdk/src/memory_allocation_c_api.cc.
ALLOCATORS="malloc free calloc realloc aligned_alloc posix_memalign valloc
            malloc_usable_size strdup realpath getcwd"

undefined_syms() {
    nm -D --undefined-only "$1" 2>/dev/null | sed 's/@.*//' | awk '{print $2}' |
        sort -u
}

# Names a library defines with a binding the dynamic linker resolves against.
#
# GLOBAL, WEAK and UNIQUE all participate; LOCAL does not, which is why the
# version script marks the allocator entry points local. UNIQUE is
# STB_GNU_UNIQUE, and it matters that this is read from readelf's Bind column
# rather than from nm: nm renders UNIQUE as a lowercase "u", which looks exactly
# like a local symbol and is trivially excluded by a filter meant to drop
# locals. The locale facet ids that check 4 exists to catch are UNIQUE, so
# dropping that class silently defeats the check.
#
# Version suffixes are stripped: libstdc++.so.6 carries @@GLIBCXX_3.4 and the
# module does not.
exported_syms() {
    readelf --dyn-syms -W "$1" 2>/dev/null |
        awk 'NR > 3 && $7 != "UND" && $8 != "" &&
             ($5 == "GLOBAL" || $5 == "WEAK" || $5 == "UNIQUE") {
                 sub(/@@?.*$/, "", $8); print $8 }' | sort -u
}

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
# Check 2: the module owns its allocator.
#
# memory_allocation_c_api.cc defines malloc and friends, and versionscript.lds
# marks them local so that every reference from inside the module -- libstdc++'s
# operator new, abseil, protobuf, gRPC, ICU, hdrhistogram, rax -- binds to them
# rather than to libc.so.6. Two things must hold, and neither is visible without
# checking: each name is defined here, and none is still imported from libc. An
# import means something is allocating outside ValkeyModule_Alloc, whose memory
# Valkey never accounts for and which crashes if it later reaches our free().
#
UNDEF=$(undefined_syms "${MODULE_SO}")
# Names exported with GLOBAL or WEAK binding, i.e. the ones the dynamic linker
# will happily resolve somewhere else.
DYN_GLOBAL=$(exported_syms "${MODULE_SO}")
for sym in ${ALLOCATORS}; do
    if echo "${UNDEF}" | grep -qx "${sym}"; then
        echo "FAIL: ${MODULE_SO} imports ${sym} from libc instead of using its" >&2
        echo "      own definition. Is memory_allocation_c_api.cc still linked" >&2
        echo "      into the module?" >&2
        FAILED=1
    elif ! nm "${MODULE_SO}" 2>/dev/null | grep -qE "^[0-9a-f]+ [Tt] ${sym}$"; then
        echo "FAIL: ${MODULE_SO} does not define ${sym}." >&2
        echo "      memory_allocation_c_api.cc is not being linked in." >&2
        FAILED=1
    elif echo "${DYN_GLOBAL}" | grep -qx "${sym}"; then
        # Defined, but exported with global binding -- which means preemptible.
        # References from inside the module then resolve through the global
        # scope, find libc.so.6's definition first, and this one is silently
        # bypassed: the module keeps running on the system allocator and Valkey
        # accounts for none of it. Nothing crashes, so only this check catches
        # it.
        echo "FAIL: ${MODULE_SO} defines ${sym} but exports it with global" >&2
        echo "      binding, so it is preemptible and will be bypassed in" >&2
        echo "      favour of libc's. Is ${sym} still listed under local: in" >&2
        echo "      vmsdk/versionscript.lds?" >&2
        FAILED=1
    fi
done

#
# Check 3: no C++ heap object can cross a DSO boundary.
#
# The allocator above only covers code linked into this .so. If the module still
# called into libstdc++.so.6, an object allocated there (by std::getline,
# std::filesystem::path, std::locale::name, ...) would be freed here with
# ValkeyModule_Free -- a jemalloc free of a libc malloc pointer. Linking
# libstdc++ statically removes the boundary; this confirms it stayed removed.
#
GLIBCXX_UNDEF=$(nm -D --undefined-only "${MODULE_SO}" 2>/dev/null |
                grep -c "GLIBCXX" || true)
if [ "${GLIBCXX_UNDEF}" -ne 0 ]; then
    echo "FAIL: ${MODULE_SO} has ${GLIBCXX_UNDEF} undefined GLIBCXX symbols," >&2
    echo "      so it is calling into libstdc++.so.6. Objects allocated there" >&2
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
# -Wl,--exclude-libs,ALL is what keeps this list empty. Both sides are compared
# using exported_syms above, which deliberately includes UNIQUE bindings -- the
# facet ids are that class, so excluding it would leave this check unable to see
# the collision it is named for.
#
LIBSTDCXX=$(gcc -print-file-name=libstdc++.so.6 2>/dev/null || true)
if [ -n "${LIBSTDCXX}" ] && [ -f "${LIBSTDCXX}" ]; then
    CLASHES=$(comm -12 <(exported_syms "${MODULE_SO}") \
                       <(exported_syms "${LIBSTDCXX}"))
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

#
# Check 5: no unhandled libc function hands us libc-allocated memory.
#
# The module's allocator is local, so libc.so.6 never sees it and keeps using
# its own. Memory allocated inside libc and freed inside libc is therefore
# self-consistent. The one way a pointer crosses is a libc function that
# allocates a result and returns it to us: we would later release it through our
# free(), handing a libc pointer to ValkeyModule_Free.
#
# memory_allocation_c_api.cc handles every such function the module references
# today -- strdup is reimplemented, realpath and getcwd abort. If a new one
# appears, it must be handled there before this check will pass.
#
# Note that __realpath_chk is absent from this list on purpose: it is the
# fortified form taking a caller-provided buffer, which does not allocate. ICU's
# uprv_tzname uses it legitimately.
#
# Both the public names and the glibc-internal aliases the compiler actually
# emits: <stdio.h> turns getline() into __getdelim(), for instance.
ALLOCATING_LIBC="strndup __strdup __strndup
                 getline getdelim __getdelim
                 asprintf vasprintf __asprintf __vasprintf
                 canonicalize_file_name get_current_dir_name
                 scandir scandir64 tempnam wcsdup __wcsdup open_memstream"
for sym in ${ALLOCATING_LIBC}; do
    if echo "${UNDEF}" | grep -qx "${sym}"; then
        echo "FAIL: ${MODULE_SO} references ${sym}(), which allocates its" >&2
        echo "      result with libc's malloc. Freeing that pointer inside the" >&2
        echo "      module passes it to ValkeyModule_Free and corrupts the" >&2
        echo "      heap. Handle it in vmsdk/src/memory_allocation_c_api.cc" >&2
        echo "      alongside strdup/realpath/getcwd." >&2
        FAILED=1
    fi
done

#
# Check 6: the set of shared libraries the module depends on is pinned.
#
# Every DSO the module links against is another allocator boundary: memory it
# allocates and hands back is allocated by its allocator, not ours, and freeing
# it here would pass it to ValkeyModule_Free. Check 5 covers libc, which is the
# only one whose allocate-and-return functions the module calls directly. The
# rest have been reviewed and are safe:
#
#   libsystemd  Only sd_is_socket_inet, sd_is_socket_sockaddr,
#               sd_is_socket_unix and sd_listen_fds are imported. All return
#               int; nothing crosses.
#   libssl,     The constructors and duplicators imported (BIO_new, SSL_new,
#   libcrypto   EVP_*_CTX_new, X509_NAME_dup, SSL_get1_peer_certificate, ...)
#               are each paired with the matching free function, which is
#               imported too. The raw-buffer cases (ASN1_STRING_to_UTF8, the
#               i2d_* family with a null output pointer) must be released with
#               OPENSSL_free, which is CRYPTO_free inside libcrypto -- so both
#               the allocation and the free happen on the far side of the
#               boundary, as with getaddrinfo/freeaddrinfo.
#   libm,       No allocation.
#   libmvec
#   libgcc_s    Unwinder only.
#   ld-linux    dlopen/dlsym only.
#
# A new entry here means a boundary nobody has looked at, so it fails the build
# until someone does. Note that OpenSSL is deliberately dynamic: linking it
# statically would mean rebuilding the module for every OpenSSL CVE rather than
# picking up a distribution update.
#
# Only additions fail. A dependency disappearing is not a memory-safety problem
# -- and it is how this list last changed, when -static-libstdc++ removed
# libstdc++.so.6.
#
ALLOWED_NEEDED="libc.so.6 libm.so.6 libmvec.so.1 libgcc_s.so.1
                libssl.so.3 libcrypto.so.3 libsystemd.so.0"

# Unquoted, so that the newlines and indentation above collapse to single
# spaces; the match below relies on every entry being space-delimited.
ALLOWED_NEEDED=$(echo ${ALLOWED_NEEDED})

NEEDED=$(readelf -d "${MODULE_SO}" 2>/dev/null |
         sed -n 's/.*(NEEDED).*\[\(.*\)\]/\1/p')
for lib in ${NEEDED}; do
    # The dynamic loader's own name is architecture-specific.
    case "${lib}" in
        ld-linux-*.so.*) continue ;;
    esac
    case " ${ALLOWED_NEEDED} " in
        *" ${lib} "*) continue ;;
    esac
    echo "FAIL: ${MODULE_SO} has a new shared library dependency: ${lib}" >&2
    echo "      Each DSO is another allocator boundary. Check whether it has" >&2
    echo "      functions that allocate memory and hand it to the caller: if" >&2
    echo "      the module frees such a pointer, it goes to ValkeyModule_Free" >&2
    echo "      and corrupts the heap. Record the finding next to check 6 in" >&2
    echo "      this script and add it to ALLOWED_NEEDED." >&2
    FAILED=1
done

if [ "${FAILED}" -ne 0 ]; then
    echo "" >&2
    echo "See vmsdk/src/memory_allocation_c_api.cc for how module memory is" >&2
    echo "expected to be allocated." >&2
    exit 1
fi

echo "check_module_allocators: OK"
