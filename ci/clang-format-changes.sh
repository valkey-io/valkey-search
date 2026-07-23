#!/bin/bash
set +e

BASE_BRANCH="${BASE_BRANCH:-origin/main}"
BASE=$(git merge-base HEAD "$BASE_BRANCH" 2>/dev/null || echo "HEAD")

# Get the list of modified or new files
files=$(git diff --name-only --diff-filter=AM "$BASE" | grep -E '\.cc$|\.h$' || echo "")

# Check if there are any files to process
if [ -z "$files" ]; then
  echo "No changed C++ files found."
  exit 0
fi

has_inline=false
for arg in "$@"; do
  if [ "$arg" = "-i" ]; then
    has_inline=true
  fi
done

for file in $files; do
  if [ ! -f "$file" ]; then
    continue
  fi
  
  if $has_inline; then
    echo "Formatting $file"
    clang-format "$@" "$file"
  else
    echo -n "Checking $file ... "
    TEMP_FILE=$(mktemp)
    if clang-format "$@" "$file" > "$TEMP_FILE"; then
      if diff -u "$file" "$TEMP_FILE" > /dev/null; then
        echo -e "\033[0;32mOK\033[0m"
      else
        echo -e "\033[0;31mError\033[0m"
        echo "Formatting issues detected in $file:"
        diff -U 0 "$file" "$TEMP_FILE" | tail -n +3
      fi
    else
      echo -e "\033[0;31mError\033[0m"
      echo "Failed to run clang-format on $file"
    fi
    rm -f "$TEMP_FILE"
  fi
done
