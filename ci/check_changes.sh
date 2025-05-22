#!/bin/bash
set +e

git_args=""
for arg in "$@"; do
  git_args+=" \"$arg\""
done
# Get the list of modified or new files
files=$(eval "git diff --name-only --diff-filter=AM $git_args"| grep -E '\.cc$|\.h$' || echo "")

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

