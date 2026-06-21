/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include <unistd.h>

#include <string>

#include "fuzzing/common/fuzz_common.h"

void TestQueryParser(const char *data, size_t size) {
  std::string query(data, size);

  FuzzSearchParameters params;
  params.index_schema = g_index_schema;
  params.parse_vars.query_string = query;

  auto result = params.PreParseQueryString();
  (void)result;
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
      TestQueryParser(buf, len);
    }
#ifdef __AFL_LOOP
  }
#endif

  return 0;
}
