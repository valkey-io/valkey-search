#!/bin/bash
set +e

bazel run @hedron_compile_commands//:refresh_all

use_cache=false
from_branch=
if [[ "$1" == "--cached" ]]; then
  use_cache=true
fi
if [[ "$1" == "--from" ]]; then
  from_branch="$2"
fi

# Get the list of modified or new files
if $use_cache; then
  files=$(git diff --cached --name-only --diff-filter=AM | grep -E '\.cc$|\.h$')
else
  files=$(git diff --name-only --diff-filter=AM $from_branch | grep -E '\.cc$|\.h$')
fi

# Check if there are any files to process
if [ -z "$files" ]; then
  exit 0
fi

execute_command() {
    local command="$1"   # The command to run
    local output         # Variable to store the command output

    # Run the command and capture the output
    output=$(eval "$command" 2>&1)

    # Check if the command was successful
    if [ $? -eq 0 ]; then
        # Print OK in green if the command succeeded
        echo -e "\033[0;32mOK\033[0m"
    else
        # Print Error in red and the command output if it failed
        echo -e "\033[0;31mError\033[0m"
        echo "$output"
    fi
}

for file in $files; do
  echo "Checking $file"
  echo -n "clang-tidy: "
  execute_command "clang-tidy --quiet  -p compile_commands.json $file 2>&1 | tail -n +3"
  echo -n "clang-format: "
  execute_command "ci/check_clang_format.sh $file"
done

