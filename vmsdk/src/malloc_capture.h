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

// Make a stack allocation of this to enable malloc capture.
struct Enable {
  Enable();
  ~Enable();

 private:
  bool previous_;
};

// Exempt these calls from malloc capture
struct Disable {
  Disable();
  ~Disable();

 private:
  bool previous_;
};

void Control(bool enable);

std::multimap<size_t, Backtrace> GetCaptures();

}  // namespace malloc_capture

}  // namespace vmsdk

#endif