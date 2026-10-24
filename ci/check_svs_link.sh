#!/bin/bash -e
#
# Copyright (c) 2026, valkey-search contributors
# All rights reserved.
# SPDX-License-Identifier: BSD 3-Clause
#
# POST_BUILD check of how the SVS C API (svs_c_api_static) is linked into
# the module: no libgomp in DT_NEEDED (SVS is built without OpenMP), no
# svs_* symbol exported (LINKER:--exclude-libs,ALL keeps it internal), no
# undefined svs:: symbol (every C API source compiled), and
# svs_get_version_string present so the first checks are not vacuous.
#
# Usage: check_svs_link.sh <module.so>
MODULE_SO=$1
[ -f "${MODULE_SO}" ] || { echo "check_svs_link: ${MODULE_SO} not found" >&2; exit 1; }
command -v readelf nm >/dev/null || { echo "check_svs_link: readelf/nm required" >&2; exit 1; }
case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) ;;
    *) echo "check_svs_link: skipped (SVS builds on x86_64 Linux only)"; exit 0 ;;
esac

if readelf -d "${MODULE_SO}" | grep -q 'gomp'; then
    echo "check_svs_link: FAIL: libgomp in DT_NEEDED (SVS was built with OpenMP)" >&2
    readelf -d "${MODULE_SO}" | grep 'gomp' >&2
    exit 1
fi

EXPORTED=$(nm -D "${MODULE_SO}" 2>/dev/null | grep -E ' [TWD] svs_' || true)
if [ -n "${EXPORTED}" ]; then
    echo "check_svs_link: FAIL: svs_* symbols exported from the module:" >&2
    echo "${EXPORTED}" | head -5 >&2
    exit 1
fi

# A C API source missing from SRCS_SVS_C_API still links (a shared library
# may carry undefined symbols) and fails only at loadmodule; catch it here.
# Both the C entry points (svs_*, extern "C") and anything in namespace svs
# (mangled with "3svs": functions, vtables, typeinfo).
UNDEFINED=$(nm -D --undefined-only "${MODULE_SO}" 2>/dev/null | grep -E ' U (svs_|.*3svs)' || true)
if [ -n "${UNDEFINED}" ]; then
    echo "check_svs_link: FAIL: undefined SVS symbols (missing C API source in SRCS_SVS_C_API?):" >&2
    echo "${UNDEFINED}" | head -5 >&2
    exit 1
fi

if ! nm --defined-only "${MODULE_SO}" 2>/dev/null | grep -q 'svs_get_version_string'; then
    echo "check_svs_link: FAIL: SVS C API not linked (svs_get_version_string missing)" >&2
    exit 1
fi

echo "check_svs_link: OK"
