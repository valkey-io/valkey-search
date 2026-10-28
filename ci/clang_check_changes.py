"""Syntax-check changed translation units with clang.

The Linux CI jobs build with GCC. Some diagnostics are clang-only -- narrowing
a non-constant expression inside a braced initializer is the common one, where
GCC warns and clang errors -- so they slip through every Linux job and surface
only on cmake-tests-macos, three quarters of an hour in.

This replays each changed .cc file's own compile command from
compile_commands.json through `clang++ -fsyntax-only` and reports the errors.
It needs a configured build tree (ci/refresh_comp_db.sh, or any build.sh run)
so that the compilation database exists.

Usage:
    python3 ci/clang_check_changes.py [base-ref]      # default: origin/main

Exits non-zero if any changed file fails to compile under clang.
"""

import json
import os
import shlex
import subprocess
import sys

# GCC spellings clang does not accept; dropping them does not affect whether
# the source itself is well-formed.
DROP_EXACT = {
    "-c",
    "-falign-functions=5",
    "-ffat-lto-objects",
    "-flax-vector-conversions",
    "-fno-rounding-math",
    "-mtune=generic",
}
DROP_PREFIX = ("-gz=", "-MD")
DROP_WITH_ARG = ("-MT", "-MF", "-o")


def clang_args(command):
    """The compile command with output/dependency and GCC-only flags removed."""
    args = shlex.split(command)[1:]
    kept = []
    skip = False
    for arg in args:
        if skip:
            skip = False
            continue
        if arg in DROP_WITH_ARG:
            skip = True
            continue
        if arg in DROP_EXACT or arg.startswith(DROP_PREFIX):
            continue
        kept.append(arg)
    return kept


def main():
    base = sys.argv[1] if len(sys.argv) > 1 else "origin/main"
    root = subprocess.run(["git", "rev-parse", "--show-toplevel"],
                          capture_output=True, text=True,
                          check=True).stdout.strip()

    db_path = os.path.join(root, ".build-release", "compile_commands.json")
    if not os.path.exists(db_path):
        db_path = os.path.join(root, "compile_commands.json")
    if not os.path.exists(db_path):
        sys.stderr.write("No compile_commands.json; run ci/refresh_comp_db.sh\n")
        return 2

    changed = subprocess.run(
        ["git", "-C", root, "diff", "--name-only", f"{base}...HEAD"],
        capture_output=True, text=True, check=True).stdout.split()
    changed = {c for c in changed if c.endswith((".cc", ".cpp"))}
    if not changed:
        print(f"no changed C++ sources against {base}")
        return 0

    with open(db_path) as handle:
        entries = json.load(handle)

    todo = [(os.path.relpath(e["file"], root), e) for e in entries
            if os.path.relpath(e["file"], root) in changed]

    print(f"clang -fsyntax-only on {len(todo)} of {len(changed)} changed files")
    failed = []
    for name, entry in todo:
        result = subprocess.run(
            ["clang++", "-fsyntax-only", "-Wno-unknown-warning-option",
             "-Qunused-arguments"] + clang_args(entry["command"]),
            cwd=entry["directory"], capture_output=True, text=True)
        errors = [ln for ln in result.stderr.splitlines() if ": error:" in ln]
        if errors:
            failed.append(name)
            print(f"\n=== {name}")
            for line in errors[:8]:
                print("   ", line)

    if failed:
        print(f"\nclang errors in: {', '.join(failed)}")
        return 1
    print("clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
