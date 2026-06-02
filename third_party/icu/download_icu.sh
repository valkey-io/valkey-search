#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ICU_VERSION=$(cat "${SCRIPT_DIR}/VERSION")
ICU_VERSION_DASH="${ICU_VERSION//./-}"
ICU_VERSION_UNDERSCORE="${ICU_VERSION//./_}"
ICU_URL="https://github.com/unicode-org/icu/releases/download/release-${ICU_VERSION_DASH}/icu4c-${ICU_VERSION_UNDERSCORE}-src.tgz"
ICU_ARCHIVE="icu4c-${ICU_VERSION_UNDERSCORE}-src.tgz"

cd "${SCRIPT_DIR}"
[ -d "source" ] && exit 0

curl -L -o "${ICU_ARCHIVE}" "${ICU_URL}"
tar xzf "${ICU_ARCHIVE}"
mv icu/source ./

echo "Optimizing ICU source - removing files from disabled components..."

for dir in samples test extra io layoutex; do
    if [ -d "source/$dir" ]; then
        echo "Optimizing $dir/ (keeping build system files)"
        # Keep only essential build files: *Makefile*, *.in, configure*, *.py, *.mk
        find "source/$dir" -type f ! -name "*Makefile*" ! -name "*.in" ! -name "configure*" ! -name "*.py" ! -name "*.mk" -delete 2>/dev/null || true
    fi
done

echo "ICU source optimized successfully"

rm -rf icu "${ICU_ARCHIVE}"
