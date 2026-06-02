# ICU (International Components for Unicode)

## What is ICU?
ICU is a mature library providing robust Unicode and locale support for multi-language text processing. It handles character properties, normalization, case folding, and word boundary detection for 300+ languages including Chinese, Japanese, Korean, Arabic, Hebrew, and European languages.

## Version Management
- **Current Version**: 76.1 (Unicode 16.0)
- **Version File**: `VERSION` - Contains the current ICU version being used

The ICU source is committed to the repository for reliable, reproducible builds. Version upgrades use the download script with the specific version defined in the `VERSION` file.

## How We Use It
We integrate ICU with static data packaging (~45MB) containing embedded Unicode data. The `valkey_search::indexes::text::UnicodeNormalizer` class provides a C++ wrapper around ICU APIs for text processing in search indexes.

**Currently Implemented:**
- Basic case folding for case-insensitive search
- ICU initialization and data integrity verification

**Foundation Ready For:**
- Unicode normalization (NFC, NFKC, NFD, NFKD)
- Word boundary detection for CJK languages
- Locale-aware case folding
- Script detection for mixed-script text

## Integration Details
- **Static linking** with `--with-data-packaging=static`
- **Out-of-tree builds** in `.build-release/icu/`
- **Libraries**: `libicudata.a`, `libicui18n.a`, `libicuuc.a`
- **Text integration**: `target_link_libraries(text PUBLIC icu)`

## Usage

```cpp
#include "src/indexes/text/unicode_normalizer.h"

// Case folding for case-insensitive search
std::string folded = UnicodeNormalizer::CaseFold("Hello World");
std::string german = UnicodeNormalizer::CaseFold("Straße");  // Handles ß
std::string turkish = UnicodeNormalizer::CaseFoldLocale("İstanbul");  // Handles İ/ı
```

## Version Upgrade

```bash
# Update to new ICU version
echo "77.1" > third_party/icu/VERSION
rm -rf third_party/icu/source
bash third_party/icu/download_icu.sh
# Commit updated source to repository
```

## Build Requirements
- **Compiler**: GCC 12+ (for main project - ICU builds with older GCC)  
- **Build tools**: autotools (configure, make, ar, ranlib)
- **Dependencies**: None (self-contained with embedded data)

## License
Unicode License (MIT-compatible) - Compatible with this project.
