# ValkeySearch

**ValkeySearch** (BSD-3-Clause), provided as a Valkey module, is a high-performance Vector Similarity Search engine optimized for AI-driven workloads. It delivers single-digit millisecond latency and high QPS, capable of handling billions of vectors with over 99% recall.

ValkeySearch allows users to create indexes and perform similarity searches, incorporating complex filters. It supports Approximate Nearest Neighbor (ANN) search with HNSW and exact matching using K-Nearest Neighbors (KNN). Users can index data using either **Valkey Hash** or **[Valkey-JSON](https://github.com/valkey-io/valkey-json)** data types.

While ValkeySearch currently focuses on Vector Search, its goal is to extend Valkey into a full-fledged search engine, supporting Full Text Search and additional indexing options.

## Supported Commands

```plaintext
FT.CREATE
FT.DROPINDEX
FT.INFO
FT._LIST
FT.SEARCH
```

For a detailed description of the supported commands and configuration options, see the [Command Reference](https://github.com/valkey-io/valkey-search/blob/main/COMMANDS.md).

For comprehensive examples, refer to the [Quick Start Guide](https://github.com/valkey-io/valkey-search/blob/main/QUICK_START.md).

## Scaling

ValkeySearch supports both **Standalone** and **Cluster** modes. Query processing and ingestion scale linearly with CPU cores in both modes. For large storage requirements, users can leverage Cluster mode for horizontal scaling of the keyspace.

If replica lag is acceptable, users can achieve horizontal query scaling by directing clients to read from replicas.

## Performance

ValkeySearch achieves high performance by storing vectors in-memory and applying optimizations throughout the stack to efficiently utilize the host resources, such as:

- **Parallelism:**  Threading model that enables lock-free execution in the read path.
- **CPU Cache Efficiency:** Designed to promote efficient use of CPU cache.
- **SIMD Optimizations:** Leveraging SIMD (Single Instruction, Multiple Data) for enhanced vector processing.

## Hybrid Queries

ValkeySearch supports hybrid queries, combining Vector Similarity Search with filtering on indexed fields, such as **Numeric** and **Tag indexes**.

There are two primary approaches to hybrid queries:

- **Pre-filtering:** Begin by filtering the dataset and then perform an exact similarity search. This works well when the filtered result set is small but can be costly with larger datasets.
- **Post-filtering:** Perform the similarity search first, then filter the results. This is suitable when the filter-qualified result set is large but may lead to empty or lower than expected amount of results.

ValkeySearch uses a **hybrid approach** with a query planner that selects the most efficient query execution path between:

- **Pre-filtering**
- **Inline-filtering:** Filters results during the similarity search process.

## Build Instructions

### Install basic tools

#### Ubuntu / Debian

```sh
sudo apt update
sudo apt install -y clangd          \
                    build-essential \
                    g++             \
                    cmake           \
                    libgtest-dev    \
                    ninja-build     \
                    libssl-dev      \
                    clang-tidy      \
                    clang-format    \
                    libsystemd-dev
```


**IMPORTANT**: building ValkeySearch requires GCC version 12 or higher, or Clang version 16 or higher. For Debian/Ubuntu, in case a lower version of GCC is installed, you may upgrade to gcc/g++ 12 with:

```sh
sudo apt update
sudo apt install -y gcc-12 g++-12
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-12 1000
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-12 1000
```

#### RedHat / CentOS / Amazon Linux

```sh
sudo yum update
sudo yum install -y gcc             \
                    gcc-c++         \
                    cmake           \
                    gtest           \
                    gtest-devel     \
                    ninja-build     \
                    openssl-devel   \
                    clang-tidy      \
                    clang-format    \
                    systemd-devel
```

### Build the module

ValkeySearch uses **CMake** for its build system. To simplify, a build script is provided. To build the module, run:

```sh
./build.sh
```

To view the available arguments, use:

```sh
./build.sh --help
```

Run unit tests with:

```sh
./build.sh --run-tests
```

#### Integration Tests

Install required dependencies (Ubuntu / Debian):

```sh
sudo apt update
sudo apt install -y lsb-release      \
                     curl            \
                     coreutils       \
                     libsystemd-dev  \
                     python3-pip     \
                     python3.12-venv \
                     locales-all     \
                     locales         \
                     gpg

curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/redis.list
sudo apt update
sudo apt-get install -y memtier-benchmark
```

Run the integration tests with:

```sh
./build.sh --run-integration-tests
```

## Load the Module

To start Valkey with the module, use the `--loadmodule` option:

```sh
valkey-server --loadmodule /path/to/libsearch.so
```

To enable JSON support, load the JSON module as well:

```sh
valkey-server --loadmodule /path/to/libsearch.so --loadmodule /path/to/libjson.so
```

For optimal performance, ValkeySearch will match the number of worker threads to the number of CPU cores on the host. You can override this with:

```sh
valkey-server "--loadmodule /path/to/libsearch.so --reader-threads 64 --writer-threads 64"
```

## Development Environment

For development purposes, it is recommended to use <b>VSCode</b>, which is already configured to run within a Docker container and is integrated with clang-tidy and clang-format. Follow these steps to set up your environment:

1. <b>Install VSCode Extensions:</b>
    - Install the `Dev Containers` extension by Microsoft in VSCode.
    - Note: Building the code may take some time, and it is important to use a host with decent CPU capacity. If you prefer, you can use a remote host. In that case, also install the following extensions:
      - `Remote - SSH` by Microsoft
      - `Remote Explorer` by Microsoft

2. <b>Run the dev container setup script</b>
    - Issue the following command from the cloned repo root directory:
        ```sh
        .devcontainer/setup.sh
        ```

3. <b>Open the Repository in VSCode:</b>
    - On your local machine, open the root directory of the cloned ValkeySearch repository in VSCode.
    - If the repository is located on a remote host:
      1. Press Ctrl+Shift+P (Windows/Linux) or Cmd+Shift+P (macOS) to open the Command Palette.
      2. Type Remote-SSH: Connect to Host and select it.
      3. Choose the appropriate host and provide any necessary authentication details.

       Once connected, VSCode will open the repository in the context of the remote host.

