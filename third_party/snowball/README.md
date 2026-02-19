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
Snowball is licensed under the 3-clause BSD License by its authors (see COPYING for full license text and copyright holders). Source: snowballstem.org/license.html as of v3.0.1.

```
Except where explicitly noted, all the software given out on this Snowball site is covered by the 3-clause BSD License:

Copyright (c) 2001, Dr Martin Porter,
Copyright (c) 2002, Richard Boulton.
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

Essentially, all this means is that you can do what you like with the code, except claim another Copyright for it, or claim that it is issued under a different license. The software is also issued without warranties, which means that if anyone suffers through its use, they cannot come back and sue you. You also have to alert anyone to whom you give the Snowball software to the fact that it is covered by the BSD license. 
```