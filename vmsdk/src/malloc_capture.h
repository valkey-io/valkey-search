/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef __VMSDK_MALLOC_CAPTURE_H__
#define __VMSDK_MALLOC_CAPTURE_H__

#include <map>

#include "vmsdk/src/utils.h"

namespace vmsdk {

namespace malloc_capture {

struct MarkStack {
  MarkStack();
  ~MarkStack();
};

void Control(bool enable);

std::multimap<size_t, Backtrace> GetCaptures();

}  // namespace malloc_capture

}  // namespace vmsdk

#endif