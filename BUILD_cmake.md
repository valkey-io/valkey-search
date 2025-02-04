# Building with `CMake`

If you wish to work with `Bazel` instead of `CMake`, follow this [guide instead][1]

Note: the below was tested on Ubuntu Linux, it might not work on other Linux distros


## Install basic tools

```bash
sudo apt update
sudo apt install -y clangd          \
                    build-essential \
                    g++             \
                    cmake           \
                    libgtest-dev    \
                    libssl-dev
```


## Build the module

```bash
git clone git@github.com:valkey-io/valkey-search.git
cd valkey-search
git submodule update --remote --init --recursive --depth 1
mkdir build-debug
cd $_
cmake ..
make -j$(nproc)
```

## Integration with IDE

During the `CMake` stage of the build, `CMake` generates a JSON file named `compile_commands.json` and places it under the
build folder. This file is used by many IDEs and text editors for providing code completion (via `clangd`).

A small caveat is that these tools will look for `compile_commands.json` under the workspace root folder.
A common workaround is to create a symbolic link to it:

```bash
cd /path/to/valkey/
# We assume here that your build folder is `build-debug`
ln -sf $(pwd)/build-debug/compile_commands.json $(pwd)/compile_commands.json
```

Another option, is to instruct `clangd` where to search for the `compile_commands.json` by using the `--compile-commands-dir` option:

```bash
clangd --compile-commands-dir=/path/to/compile_commands.json
```

Restart your IDE and voila

## Loading the module

After a successfull build, the module is placed under `build-debug/lib/valkeysearch.so` (assuming your build folder is `build-debug`),
to load it into `valkey-server`, you can use this command:

```bash
valkey-server --loadmodule build-debug/lib/valkeysearch.so
```


[1]: https://github.com/valkey-io/valkey-search/blob/main/DEVELOPER.md
