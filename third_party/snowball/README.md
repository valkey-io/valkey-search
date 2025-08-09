# Snowball Stemming Library

## What is Snowball?
Snowball is a algorithmic stemming library that reduces words to their morphological root form (e.g., "running" → "run"). It uses language-specific algorithms compiled from high-level `.sbl` rule definitions into efficient C code.

## Version Management
- **Current Version**: v3.0.1 (stable release)
- **Version File**: `VERSION` - Contains the current Snowball version being used

All language binaries are generated from the specific version defined in the `VERSION` file to ensure consistency and reproducibility.

## How We Use It
We integrate only the minimal Snowball runtime (128KB) with UTF-8 language support. The `valkey_search::indexes::Stemmer` class provides a clean C++ wrapper around the Snowball C API for text preprocessing in search indexes.

## Commands

### Add Language Support
```bash
./add_language.sh french german spanish
```
Adds new stemming languages using the version specified in the `VERSION` file. Downloads the Snowball compiler for that specific version, generates language-specific C code, and updates both modules.h and CMakeLists.txt with the new languages.

### Remove Language Support  
```bash
./remove_language.sh french german
```
Removes specified languages and regenerates both modules.h and CMakeLists.txt to exclude removed languages.

### Update Existing Languages
```bash
# Update to latest stable version
./update_languages.sh

# Update to specific version
./update_languages.sh --version v2.2.0
```
Updates all currently installed language binaries. If no version is specified, updates to the latest stable version and updates the `VERSION` file accordingly. If a specific version is provided, uses that version and updates the `VERSION` file. This is useful for getting algorithm improvements and bug fixes without changing the language list.

### List Available Languages
**Currently supported**: english (add more via scripts)

**Available for addition**: arabic, armenian, basque, catalan, danish, dutch, english, finnish, french, german, greek, hindi, hungarian, indonesian, irish, italian, lithuanian, nepali, norwegian, portuguese, romanian, russian, serbian, spanish, swedish, tamil, turkish, yiddish

**Aliases supported**: Each language has multiple aliases (e.g., english/en/eng, french/fr/fre/fra)

## License
BSD 3-Clause (see COPYING) - Compatible with this project. We comply by retaining copyright notices and not using the Snowball name for endorsement.
