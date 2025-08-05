#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMP_DIR=$(mktemp -d)

# Function to get the latest stable version
get_latest_stable_version() {
    git ls-remote --tags https://github.com/snowballstem/snowball.git | \
    grep 'refs/tags/v[0-9]' | \
    grep -v '\^{}' | \
    sed 's/.*refs\/tags\///' | \
    sort -V | \
    tail -1
}

# Parse command line arguments
VERSION=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --version|-v)
            VERSION="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--version|-v VERSION]"
            echo "  If version is not specified, updates to latest stable version"
            exit 1
            ;;
    esac
done

# Determine version to use
if [ -z "$VERSION" ]; then
    echo "Getting latest stable version..."
    VERSION=$(get_latest_stable_version)
    if [ -z "$VERSION" ]; then
        echo "Error: Could not determine latest stable version"
        exit 1
    fi
    echo "Latest stable version: $VERSION"
    
    # Update VERSION file
    echo "$VERSION" > "$SCRIPT_DIR/VERSION"
    echo "Updated VERSION file to $VERSION"
else
    echo "Using specified version: $VERSION"
    # Update VERSION file with specified version
    echo "$VERSION" > "$SCRIPT_DIR/VERSION"
    echo "Updated VERSION file to $VERSION"
fi

echo "Updating existing Snowball language binaries to version $VERSION..."

# Get list of currently supported languages
CURRENT_LANGS=()
if ls "$SCRIPT_DIR/src_c/stem_UTF_8_"*.h >/dev/null 2>&1; then
    for f in "$SCRIPT_DIR/src_c/stem_UTF_8_"*.h; do
        lang=$(basename "$f" .h | sed 's/stem_UTF_8_//')
        CURRENT_LANGS+=("$lang")
    done
fi

if [ ${#CURRENT_LANGS[@]} -eq 0 ]; then
    echo "No languages currently installed. Use add_language.sh to add languages first."
    exit 1
fi

echo "Found languages: ${CURRENT_LANGS[*]}"

# Setup Snowball compiler from specific version
cd "$TEMP_DIR"
echo "Downloading Snowball compiler version $VERSION..."
git clone https://github.com/snowballstem/snowball.git >/dev/null 2>&1
cd snowball
git checkout "$VERSION" >/dev/null 2>&1
echo "Checked out version: $(git describe --tags)"
make snowball >/dev/null 2>&1

# Update each language
echo "Updating language files..."
for lang in "${CURRENT_LANGS[@]}"; do
    if [ ! -f "algorithms/${lang}.sbl" ]; then
        echo "Warning: Language '$lang' not found in Snowball version $VERSION, skipping"
        continue
    fi
    
    echo "  - Updating $lang"
    ./snowball "algorithms/${lang}.sbl" -o "stem_UTF_8_${lang}" -eprefix "${lang}_UTF_8_" -r runtime -u
    cp "stem_UTF_8_${lang}.c" "$SCRIPT_DIR/src_c/"
    cp "stem_UTF_8_${lang}.h" "$SCRIPT_DIR/src_c/"
done

# Update CMakeLists.txt to ensure consistency (language list unchanged)
echo "Updating CMakeLists.txt..."
{
    echo "# Snowball stemming library"
    echo "# Build the libstemmer C library"
    echo "# Version: $VERSION"
    echo
    echo "set(SNOWBALL_SOURCE_DIR \${CMAKE_CURRENT_SOURCE_DIR})"
    echo
    echo "# Source files for libstemmer"
    echo "set(LIBSTEMMER_SOURCES"
    echo "  \${SNOWBALL_SOURCE_DIR}/libstemmer/libstemmer.c"
    echo "  \${SNOWBALL_SOURCE_DIR}/runtime/api.c"
    echo "  \${SNOWBALL_SOURCE_DIR}/runtime/utilities.c"
    echo ")"
    echo
    echo "# Generated stemmer sources (UTF-8 versions for supported languages)"
    echo "set(STEMMER_SOURCES"
    for f in "$SCRIPT_DIR/src_c/stem_UTF_8_"*.c; do
        if [ -f "$f" ]; then
            lang=$(basename "$f" .c | sed 's/stem_UTF_8_//')
            echo "  \${SNOWBALL_SOURCE_DIR}/src_c/stem_UTF_8_${lang}.c"
        fi
    done
    echo ")"
    echo
    echo "# Create the snowball library"
    echo "add_library(snowball STATIC \${LIBSTEMMER_SOURCES} \${STEMMER_SOURCES})"
    echo
    echo "# Set include directories"
    echo "target_include_directories(snowball PUBLIC" 
    echo "  \${SNOWBALL_SOURCE_DIR}/include"
    echo "  \${SNOWBALL_SOURCE_DIR}"
    echo ")"
    echo
    echo "# Set compile flags to match the original build"
    echo "target_compile_options(snowball PRIVATE -w) # Suppress warnings from third-party code"
    echo
    echo "# Export the target"
    echo "set_target_properties(snowball PROPERTIES"
    echo "  POSITION_INDEPENDENT_CODE ON"
    echo "  CXX_STANDARD 20"
    echo ")"
} > "$SCRIPT_DIR/CMakeLists.txt"

rm -rf "$TEMP_DIR"
echo "Successfully updated all language binaries to Snowball version $VERSION"
echo "Languages updated: ${CURRENT_LANGS[*]}"
echo "Note: modules.h was not modified. Language list and aliases remain unchanged."
echo "CMakeLists.txt was updated for consistency."
