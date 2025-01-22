#!/bin/bash
set -e
bazel run @hedron_compile_commands//:refresh_all

git_args=""
for arg in "$@"; do
  git_args+=" \"$arg\""
done
# Get the list of modified or new files
files=$(eval "git diff --name-only --diff-filter=AM $git_args"| grep -E '\.cc$|\.h$')

# Check if there are any files to process
if [ -z "$files" ]; then
  exit 0
fi

for file in $files; do
  #echo "file: $file"
  clang-tidy --quiet  -p compile_commands.json "$file" 2>&1 | tail -n +3
  ci/check_clang_format.sh "$file"
done

