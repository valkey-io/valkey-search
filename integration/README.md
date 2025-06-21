## Integration Testing Framework for `valkey-search`

This testing framework extends the [valkey test framework][1]

[1]: https://github.com/valkey-io/valkey-test-framework

## Standard Test Execution

To execute the integration tests:

```bash
# Set up workspace
mkdir -p $HOME/src && cd $_
git clone https://github.com/valkey-io/valkey-search.git
cd valkey-search

# Build and test
./build.sh
integration/run.sh
```

This process will:
1. Create a release build of the module
2. Execute the test suite against the built module

## Testing with Address Sanitizer (ASan)

### Step 1: Prepare valkey-server with ASan

```bash
# Set up workspace
mkdir -p $HOME/src && cd $_
git clone https://github.com/valkey-io/valkey.git
cd valkey

# Build with ASan
mkdir .build-debug-asan && cd $_
cmake .. -DBUILD_SANITIZER=address \
         -DCMAKE_BUILD_TYPE=Debug \
         -DCMAKE_INSTALL_PREFIX=$HOME/valkey-install-asan
make -j$(nproc) install
```

The ASan-enabled server will be available at: `$HOME/valkey-install-asan/bin/valkey-server`

### Step 2: Build Module with ASan

```bash
cd $HOME/src/valkey-search
./build.sh --asan
```

### Step 3: Execute Tests

```bash
cd $HOME/src/valkey-search
VALKEY_SERVER_PATH=$HOME/valkey-install-asan/bin/valkey-server \
MODULE_PATH=$(pwd)/.build-release-asan/libsearch.so \
integration/run.sh --asan
```

### Environment Variables

The test runner accepts these configuration variables:
- `VALKEY_SERVER_PATH`: Location of `valkey-server` executable (defaults to PATH lookup)
- `MODULE_PATH`: Location of `libsearch.so` (auto-detected based on build type if not specified)

## Filtering tests

In order to run a filtered list of tests, use the `TEST_PATTERN` environment variable like this:

```bash
TEST_PATTERN=TestVSSBasic integration/run.sh
```

## Adding new tests

1. Create a new file under `integration/` folder `test_my_test.py`
2. Paste the following content:

```python
import time
from valkeytestframework.util.waiters import *
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

# The class name must start with `Test`
class TestMyTest(ValkeySearchTestCaseBase):

    def tests_something(self):
        # Create a new connection
        client: Valkey = self.server.get_new_client()
        # Check something ...
        pass
```
