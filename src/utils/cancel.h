/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_UTILS_CANCEL_H_
#define VALKEYSEARCH_SRC_UTILS_CANCEL_H_

#include <memory>

#include "grpcpp/server_context.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
namespace cancel {

//
// Long running query operations need to be cancellable.
// Every query object is given a shared_ptr to a Token object
// The query should periodically check if the operation has been cancelled,
// and if so, it should stop processing as soon as possible.
//
// There are different concrete implementations of Token,
// depending on the context of the query operation.
//

struct Base {
  virtual ~Base() = default;
  virtual bool IsCancelled() = 0;
  virtual void Cancel() = 0;
};

using Token = std::shared_ptr<Base>;

//
// Make a Cancellation Token based on a timeout and optionally a rGPC server
// context
//
Token Make(long long timeout_ms, grpc::CallbackServerContext *context);

}  // namespace cancel
}  // namespace valkey_search

#endif
