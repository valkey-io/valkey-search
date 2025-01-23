#!/bin/bash
set -e
bazel build //...
bazel run @hedron_compile_commands//:refresh_all