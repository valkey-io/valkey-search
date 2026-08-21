/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include <unistd.h>

#include <vector>

#include "fuzzing/common/fuzz_common.h"
#include "src/commands/ft_create_parser.h"

void TestFTCreateParser(const char *data, size_t size) {
  auto args = SplitToValkeyStringVector(data, size);
  if (args.empty()) {
    return;
  }
  auto result =
      valkey_search::ParseFTCreateArgs(nullptr, args.data(), args.size());
  (void)result;
  FreeValkeyStringVector(args);
}

int main() {
  InitFuzzEnvironment();

  if (!g_index_schema) {
    return 1;
  }

#ifdef __AFL_HAVE_MANUAL_CONTROL
  __AFL_INIT();
#endif

  char buf[4096];

#ifdef __AFL_LOOP
  while (__AFL_LOOP(1000)) {
#endif
    ssize_t len = read(STDIN_FILENO, buf, sizeof(buf) - 1);
    if (len > 0) {
      buf[len] = '\0';
      TestFTCreateParser(buf, len);
    }
#ifdef __AFL_LOOP
  }
#endif

  return 0;
}
