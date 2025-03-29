/*
 * Copyright (c) 2025, ValkeySearch contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef VALKEYSEARCH_SRC_COORDINATOR_UTIL_H_
#define VALKEYSEARCH_SRC_COORDINATOR_UTIL_H_

#include <string>

#include "absl/status/status.h"
#include "grpcpp/support/status.h"
#include "src/coordinator/coordinator.pb.h"

namespace valkey_search {
inline grpc::Status ToGrpcStatus(const absl::Status& status) {
  if (status.ok()) {
    return grpc::Status::OK;
  }
  return {static_cast<grpc::StatusCode>(status.code()),
          std::string(status.message())};
}
namespace coordinator {
// This offset results in 26673 for Redis default port 6379 - which is COORD
// on a telephone keypad.
static constexpr int kCoordinatorPortOffset = 20294;

inline int GetCoordinatorPort(int redis_port) {
  // TODO Make handling of TLS more robust
  if (redis_port == 6378) {
    return redis_port + kCoordinatorPortOffset + 1;
  }
  return redis_port + kCoordinatorPortOffset;
}
}  // namespace coordinator

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_COORDINATOR_UTIL_H_
