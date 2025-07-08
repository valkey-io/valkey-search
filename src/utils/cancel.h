/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_UTILS_CANCEL_H_
#define VALKEYSEARCH_SRC_UTILS_CANCEL_H_

#include <memory>
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
// A Concrete implementation of Token that can be used to cancel  
// operations based on a timeout.
//
struct OnTime : public Base {
  static Token Make(long long timeout_ms);
 private:
  OnTime(long long timeout_ms);
  bool IsCancelled() override;
  void Cancel() override;
  bool is_cancelled_{false}; // Once cancelled, stay cancelled
  long long deadline_ms_;
  int count_{0};
};

} // namespace cancel
} // namespace valkey_search



#endif
