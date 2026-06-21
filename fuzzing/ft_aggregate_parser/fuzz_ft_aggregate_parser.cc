/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include <unistd.h>

#include <vector>

#include "fuzzing/common/fuzz_common.h"
#include "src/commands/ft_aggregate_parser.h"
#include "vmsdk/src/command_parser.h"

void TestFTAggregateParser(const char *data, size_t size) {
  auto args = SplitToValkeyStringVector(data, size);

  valkey_search::aggregate::AggregateParameters params(0);
  params.timeout_ms = 10000;
  params.index_schema = g_index_schema;

  FuzzIndexInterface iface(g_index_schema);
  params.parse_vars_.index_interface_ = &iface;

  auto parser = valkey_search::aggregate::CreateAggregateParser();
  vmsdk::ArgsIterator itr(args.data(), args.size());
  auto result = parser.Parse(params, itr);
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
      TestFTAggregateParser(buf, len);
    }
#ifdef __AFL_LOOP
  }
#endif

  return 0;
}
