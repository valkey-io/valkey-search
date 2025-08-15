/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_DEBUG_H_
#define VMSDK_SRC_DEBUG_H_

#include <absl/status/statusor.h>
#include <absl/strings/string_view.h>

#include <source_location>

#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
namespace debug {

//
// PausePoints are a tool to help with debugging of background processes.
//

//
// This function is put into your code at points that you want to pause the
// current thread. A unique label is provided to distinguish this pause point
// with others.
//
#define PAUSEPOINT(name) \
  vmsdk::debug::PausePoint(name, std::source_location::current())
void PausePoint(absl::string_view point, std::source_location location);

//
// This function is used by the control machinery (FT.DEBUG) to enable/disable
// and test PausePoints.
//
void PausePointControl(absl::string_view point, bool enable);

//
// This function is used to determine how many threads are waiting at the
// PausePoint
//
absl::StatusOr<size_t> PausePointWaiters(absl::string_view point);

//
// List current state as a reply
//
void PausePointList(ValkeyModuleCtx *ctx);

}  // namespace debug
}  // namespace vmsdk

#endif