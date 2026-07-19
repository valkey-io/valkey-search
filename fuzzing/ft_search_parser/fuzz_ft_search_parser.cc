/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include <unistd.h>

#include <vector>

#include "fuzzing/common/fuzz_common.h"
#include "src/commands/ft_search_parser.h"
#include "vmsdk/src/command_parser.h"

void TestFTSearchParser(const char *data, size_t size) {
  auto args = SplitToValkeyStringVector(data, size);

  valkey_search::SearchCommand cmd(0);
  cmd.index_schema = g_index_schema;
  cmd.timeout_ms = 10000;
  cmd.parse_vars.query_string = "*";

  vmsdk::ArgsIterator itr(args.data(), args.size());
  auto result = cmd.ParseCommand(itr);
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
      TestFTSearchParser(buf, len);
    }
#ifdef __AFL_LOOP
  }
#endif

  return 0;
}
