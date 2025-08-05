#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMP_DIR=$(mktemp -d)

echo "Updating existing Snowball language binaries..."

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

# Setup Snowball compiler
cd "$TEMP_DIR"
echo "Downloading latest Snowball compiler..."
git clone --depth=1 https://github.com/snowballstem/snowball.git >/dev/null 2>&1
cd snowball
make snowball >/dev/null 2>&1

# Update each language
echo "Updating language files..."
for lang in "${CURRENT_LANGS[@]}"; do
    if [ ! -f "algorithms/${lang}.sbl" ]; then
        echo "Warning: Language '$lang' not found in current Snowball repository, skipping"
        continue
    fi
    
    echo "  - Updating $lang"
    ./snowball "algorithms/${lang}.sbl" -o "stem_UTF_8_${lang}" -eprefix "${lang}_UTF_8_" -r runtime -u
    cp "stem_UTF_8_${lang}.c" "$SCRIPT_DIR/src_c/"
    cp "stem_UTF_8_${lang}.h" "$SCRIPT_DIR/src_c/"
done

rm -rf "$TEMP_DIR"
echo "Successfully updated all language binaries to latest Snowball versions"
echo "Languages updated: ${CURRENT_LANGS[*]}"
echo "Note: modules.h was not modified. Language list and aliases remain unchanged."
