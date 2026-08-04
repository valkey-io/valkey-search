#!/usr/bin/env bash
set -e

FIX=false
UPDATE=false
FILE=""

for arg in "$@"; do
  if [ "$arg" = "--fix" ]; then
    FIX=true
  elif [ "$arg" = "--update" ]; then
    UPDATE=true
  else
    FILE="$arg"
  fi
done

if [ "$UPDATE" = true ] && [ -n "$FILE" ]; then
  echo "Error: Cannot specify both --update and a file path." >&2
  exit 1
fi

if [ "$UPDATE" = false ] && [ -z "$FILE" ]; then
  echo "Usage: $0 [--fix] [--update] [<file>]" >&2
  exit 1
fi

# Helper function to find the best compilation database file
get_tidy_db_file() {
  if [ -f ".build-release-container/compile_commands.json" ]; then
    echo ".build-release-container/compile_commands.json"
  elif [ -f ".build-debug-container/compile_commands.json" ]; then
    echo ".build-debug-container/compile_commands.json"
  elif [ -f ".build-release/compile_commands.json" ]; then
    echo ".build-release/compile_commands.json"
  elif [ -f ".build-debug/compile_commands.json" ]; then
    echo ".build-debug/compile_commands.json"
  elif [ -f "compile_commands.json" ]; then
    echo "compile_commands.json"
  else
    echo ""
  fi
}

# Multi-file update mode
if [ "$UPDATE" = true ]; then
  BASE_BRANCH="${BASE_BRANCH:-origin/main}"
  BASE=$(git merge-base HEAD "$BASE_BRANCH" 2>/dev/null || echo "HEAD")
  
  # Get the list of modified or new C++ files
  files=$(git diff --name-only --diff-filter=AM "$BASE" | grep -E '\.cc$|\.h$|\.cpp$|\.hpp$' || echo "")
  
  if [ -z "$files" ]; then
    echo "No changed C++ files found."
    exit 0
  fi
  
  echo "Found changed files to process:"
  for file in $files; do
    echo "  $file"
  done
  echo ""
  
  failed=false
  for file in $files; do
    if [ ! -f "$file" ]; then
      continue
    fi
    
    echo "================================================================================"
    if [ "$FIX" = true ]; then
      echo "Processing (Fix Mode): $file"
      if ! "$0" --fix "$file"; then
        failed=true
      fi
    else
      echo "Processing (Check Mode): $file"
      if ! "$0" "$file"; then
        failed=true
      fi
    fi
  done
  
  echo "================================================================================"
  if [ "$failed" = true ]; then
    echo "Some files failed check/fix." >&2
    exit 1
  else
    echo "All files processed successfully."
    exit 0
  fi
fi


if [ ! -f "$FILE" ]; then
  echo "Error: File '$FILE' does not exist." >&2
  exit 1
fi

# Helper function to generate a precise header filter matching only the target component
get_header_filter() {
  local file=$1
  local base="${file%.*}"
  local escaped_base
  escaped_base=$(echo "$base" | sed 's/[./*?+^$()|[\]\\]/\\&/g')
  echo "${escaped_base}\.(cc|cpp|h|hpp)$"
}

# Helper function to setup the compilation database with adjusted paths (for source files)
setup_tidy_db() {
  local db_file
  db_file=$(get_tidy_db_file)
  TIDY_DB_DIR="."
  TEMP_DB_DIR=""
  
  if [ -z "$db_file" ]; then
    echo "Warning: No compilation database found. clang-tidy checks may fail." >&2
    return
  fi
  
  if [ -f "$db_file" ]; then
    local host_dir
    host_dir=$(grep -m1 '"directory":' "$db_file" | sed -E 's/.*"directory": "([^"]*)".*/\1/')
    local build_dir_name
    build_dir_name=$(basename "$host_dir")
    local host_root
    host_root=$(echo "$host_dir" | sed "s|/$build_dir_name$||")
    local container_root
    container_root=$(pwd)
    
    if [ "$host_root" != "$container_root" ]; then
      echo "Adjusting compile_commands.json paths for container environment..."
      TEMP_DB_DIR=$(mktemp -d)
      sed "s|$host_root|$container_root|g" "$db_file" > "$TEMP_DB_DIR/compile_commands.json"
      TIDY_DB_DIR="$TEMP_DB_DIR"
    else
      TIDY_DB_DIR=$(dirname "$db_file")
    fi
  fi
}

# Fix mode (single file)
if [ "$FIX" = true ]; then
  if [[ "$FILE" =~ \.(cc|cpp|h|hpp)$ ]]; then
    TIDY_ARGS=("-fix" "-checks=-misc-include-cleaner" "--header-filter=$(get_header_filter "$FILE")")
    
    if [[ "$FILE" =~ \.(h|hpp)$ ]]; then
      CC_FILE="${FILE%.*}.cc"
      if [ ! -f "$CC_FILE" ]; then
        CC_FILE="${FILE%.*}.cpp"
      fi
      
      if [ -f "$CC_FILE" ]; then
        echo "Header file detected. Fixing clang-tidy issues using $CC_FILE context..."
        DB_FILE=$(get_tidy_db_file)
        if [ -n "$DB_FILE" ] && [ -f "$DB_FILE" ]; then
          FLAGS=$(python3 ci/extract_flags.py "$DB_FILE" "$CC_FILE")
          echo "Running clang-tidy with fixes on $FILE..."
          clang-tidy "${TIDY_ARGS[@]}" "$FILE" -- -x c++ $FLAGS || echo "clang-tidy reported errors during fixing."
        else
          echo "Warning: No compilation database found. Skipping clang-tidy fixes for header."
        fi
      else
        echo "Warning: No matching source file found for header $FILE. Skipping clang-tidy fixes."
      fi
    else
      # Source file
      setup_tidy_db
      echo "Running clang-tidy with fixes on $FILE..."
      clang-tidy -p "$TIDY_DB_DIR" "${TIDY_ARGS[@]}" "$FILE" || echo "clang-tidy reported errors during fixing."
      if [ -n "$TEMP_DB_DIR" ]; then rm -rf "$TEMP_DB_DIR"; fi
    fi
  fi
  
  echo "Formatting $FILE in-place..."
  clang-format -i "$FILE"
  echo "Fixes applied successfully."
  exit 0
fi

# Check mode (single file)
# 1. Run clang-format check
echo "Checking formatting for $FILE..."
TEMP_FILE=$(mktemp)
clang-format "$FILE" > "$TEMP_FILE"

if ! diff -u "$FILE" "$TEMP_FILE" > /dev/null; then
  echo "Formatting issues detected in $FILE:" >&2
  diff -U 0 "$FILE" "$TEMP_FILE" | tail -n +3 >&2
  rm -f "$TEMP_FILE"
  exit 1
fi
rm -f "$TEMP_FILE"
echo "Formatting is OK."

# 2. Run clang-tidy check (only for C++ files)
if [[ "$FILE" =~ \.(cc|cpp|h|hpp)$ ]]; then
  TIDY_ARGS=("--header-filter=$(get_header_filter "$FILE")")
  
  if [[ "$FILE" =~ \.(h|hpp)$ ]]; then
    CC_FILE="${FILE%.*}.cc"
    if [ ! -f "$CC_FILE" ]; then
      CC_FILE="${FILE%.*}.cpp"
    fi
    
    if [ -f "$CC_FILE" ]; then
      echo "Header file detected. Running clang-tidy on $FILE using $CC_FILE context..."
      DB_FILE=$(get_tidy_db_file)
      if [ -n "$DB_FILE" ] && [ -f "$DB_FILE" ]; then
        FLAGS=$(python3 ci/extract_flags.py "$DB_FILE" "$CC_FILE")
        if ! clang-tidy "${TIDY_ARGS[@]}" -warnings-as-errors='*' "$FILE" -- -x c++ $FLAGS; then
          echo "clang-tidy failed for $FILE" >&2
          exit 1
        fi
      else
        echo "Warning: No compilation database found. Skipping clang-tidy for header."
      fi
    else
      echo "Warning: No matching source file found for header $FILE. Skipping clang-tidy."
      exit 0
    fi
  else
    # Source file
    setup_tidy_db
    echo "Running clang-tidy on $FILE..."
    if ! clang-tidy -p "$TIDY_DB_DIR" "${TIDY_ARGS[@]}" -warnings-as-errors='*' "$FILE"; then
      echo "clang-tidy failed for $FILE" >&2
      if [ -n "$TEMP_DB_DIR" ]; then rm -rf "$TEMP_DB_DIR"; fi
      exit 1
    fi
    if [ -n "$TEMP_DB_DIR" ]; then rm -rf "$TEMP_DB_DIR"; fi
  fi
  echo "clang-tidy check passed."
fi

exit 0
