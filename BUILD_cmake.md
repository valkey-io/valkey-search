# Building with `CMake`

If you wish to work with `Bazel` instead of `CMake`, follow this [guide instead][1]

Note: the below was tested on `Ubuntu Linux`, it might not work on other Linux distros

## Install basic tools

**NOTE**: building `valkey-search` requires GCC version 12 and later or Clang version 16 and later.

### `Ubuntu 24.04`

```bash
sudo apt update
sudo apt install -y clangd          \
                    build-essential \
                    g++             \
                    cmake           \
                    libgtest-dev    \
                    libssl-dev
```

### `Ubuntu 22.04`

`Ubuntu` 22.04 comes with gcc 11 by default - which does not meet the minimum required, to fix this, we install `gcc-12` & `g++-12` and we override the defaults using `update-alternatives`:

```bash
sudo apt update
sudo apt install -y clangd          \
                    build-essential \
                    g++             \
                    gcc-12          \
                    g++-12          \
                    cmake           \
                    libgtest-dev    \
                    libssl-dev
```

Update the `gcc` & `g++` to point to version 12:

```bash
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-12 90
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-12 90
```

## Build the module

For your convenience, we provide a `build.sh` script. First, clone the code:

```bash
git clone https://github.com/valkey-io/valkey-search.git
cd valkey-search
```

Next, build & test the module for the `release` configuration by typing this into your terminal:

```bash
./build.sh --run-tests
```

Once the build the completed, all build generated output can be found in `.build-release/` folder.

Tip: use `./build.sh --help` to see more build options

## Loading the module

After a successful build, the module is placed under `.build-release/valkeysearch.so`.
to load it into `valkey-server`, use the following command:

```bash
valkey-server --loadmodule .build-release/valkeysearch.so
```


[1]: https://github.com/valkey-io/valkey-search/blob/main/DEVELOPER.md
