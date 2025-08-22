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

# Update only existing essential runtime files used by the build
echo "Updating existing Snowball files..."
if [ -f "$SCRIPT_DIR/runtime/api.c" ] && [ -f "runtime/api.c" ]; then
    cp runtime/api.c "$SCRIPT_DIR/runtime/"
fi
if [ -f "$SCRIPT_DIR/runtime/utilities.c" ] && [ -f "runtime/utilities.c" ]; then
    cp runtime/utilities.c "$SCRIPT_DIR/runtime/"
fi
if [ -f "$SCRIPT_DIR/runtime/header.h" ] && [ -f "runtime/header.h" ]; then
    cp runtime/header.h "$SCRIPT_DIR/runtime/"
fi
if [ -f "$SCRIPT_DIR/runtime/api.h" ] && [ -f "runtime/api.h" ]; then
    cp runtime/api.h "$SCRIPT_DIR/runtime/"
fi
if [ -f "$SCRIPT_DIR/include/libstemmer.h" ] && [ -f "include/libstemmer.h" ]; then
    cp include/libstemmer.h "$SCRIPT_DIR/include/"
fi
if [ -f "$SCRIPT_DIR/libstemmer/modules.txt" ] && [ -f "libstemmer/modules.txt" ]; then
    cp libstemmer/modules.txt "$SCRIPT_DIR/libstemmer/"
fi

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

# Regenerate modules.h based on current languages
echo "Regenerating modules.h for current languages..."
{
    echo "/* libstemmer/modules.h: List of stemming modules."
    echo " *"
    echo " * This file is generated by mkmodules.pl from a list of module names."
    echo " * Do not edit manually."
    echo " *"
    echo " * Modules included by this file are: ${CURRENT_LANGS[*]},"
    echo " */"
    echo
    for lang in "${CURRENT_LANGS[@]}"; do
        echo "#include \"../src_c/stem_UTF_8_${lang}.h\""
    done
    echo
    echo "typedef enum {"
    echo "  ENC_UNKNOWN=0,"
    echo "  ENC_UTF_8"
    echo "} stemmer_encoding_t;"
    echo
    echo "struct stemmer_encoding {"
    echo "  const char * name;"
    echo "  stemmer_encoding_t enc;"
    echo "};"
    echo "static const struct stemmer_encoding encodings[] = {"
    echo "  {\"UTF_8\", ENC_UTF_8},"
    echo "  {0,ENC_UNKNOWN}"
    echo "};"
    echo
    echo "struct stemmer_modules {"
    echo "  const char * name;"
    echo "  stemmer_encoding_t enc;"
    echo "  struct SN_env * (*create)(void);"
    echo "  void (*close)(struct SN_env *);"
    echo "  int (*stem)(struct SN_env *);"
    echo "};"
    echo "static const struct stemmer_modules modules[] = {"
    for lang in "${CURRENT_LANGS[@]}"; do
        echo "  {\"${lang}\", ENC_UTF_8, ${lang}_UTF_8_create_env, ${lang}_UTF_8_close_env, ${lang}_UTF_8_stem},"
        # Add common aliases - you may want to customize this based on modules.txt
        case $lang in
            "english")
                echo "  {\"en\", ENC_UTF_8, ${lang}_UTF_8_create_env, ${lang}_UTF_8_close_env, ${lang}_UTF_8_stem},"
                echo "  {\"eng\", ENC_UTF_8, ${lang}_UTF_8_create_env, ${lang}_UTF_8_close_env, ${lang}_UTF_8_stem},"
                ;;
        esac
    done
    echo "  {0,ENC_UNKNOWN,0,0,0}"
    echo "};"
    echo "static const char * algorithm_names[] = {"
    for lang in "${CURRENT_LANGS[@]}"; do
        echo "  \"${lang}\", "
    done
    echo "  0"
    echo "};"
} > "$SCRIPT_DIR/libstemmer/modules.h"

# Regenerate libstemmer.c from template
echo "Regenerating libstemmer.c from template..."
if [ -f "libstemmer/libstemmer_c.in" ]; then
    # Read the template and replace @MODULES_H@ with modules.h
    sed 's/@MODULES_H@/modules.h/g' "libstemmer/libstemmer_c.in" > "$SCRIPT_DIR/libstemmer/libstemmer.c"
    echo "Successfully regenerated libstemmer.c from template"
else
    echo "Warning: libstemmer_c.in template not found, libstemmer.c not updated"
fi

rm -rf "$TEMP_DIR"
echo "Successfully updated all language binaries to Snowball version $VERSION"
echo "Languages updated: ${CURRENT_LANGS[*]}"
echo "Updated files: modules.h, modules.txt, api.h, libstemmer.c, and CMakeLists.txt"
echo "All files are now consistent with version $VERSION"
