#!/bin/bash

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <file>" >&2
  exit 1
fi

FILE=$1

if [ ! -f "$FILE" ]; then
  echo "Error: File '$FILE' does not exist." >&2
  exit 1
fi

# Create a temporary file for formatted output
TEMP_FILE=$(mktemp)

# Run clang-format and save output to the temp file
clang-format "$FILE" > "$TEMP_FILE"

# Perform a diff between the original file and the formatted file
if diff -u "$FILE" "$TEMP_FILE" > /dev/null; then
  rm -f "$TEMP_FILE"
  exit 0
else
  echo "Formatting issues detected in $FILE:" >&2
  diff -U 0 "$FILE" "$TEMP_FILE"  | tail -n +3 >&2
  rm -f "$TEMP_FILE"
  exit 1
fi
