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

#ifndef VALKEYSEARCH_SRC_INDEX_PERSISTENCE_H_
#define VALKEYSEARCH_SRC_INDEX_PERSISTENCE_H_

#include <cstddef>
#include <cstdlib>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "third_party/hnswlib/iostream.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

class RDBInputStream : public hnswlib::InputStream {
 public:
  explicit RDBInputStream(RedisModuleIO *rdb) : rdb_(rdb) {}

  absl::Status LoadSizeT(size_t &val) override {
    val = RedisModule_LoadUnsigned(rdb_);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_LoadUnsigned failed");
    }
    return absl::OkStatus();
  }

  absl::Status LoadUnsigned(unsigned int &val) override {
    val = RedisModule_LoadUnsigned(rdb_);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_LoadUnsigned failed");
    }
    return absl::OkStatus();
  }

  absl::Status LoadSigned(int &val) override {
    val = RedisModule_LoadSigned(rdb_);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_LoadSigned failed");
    }
    return absl::OkStatus();
  }

  absl::Status LoadDouble(double &val) override {
    val = RedisModule_LoadDouble(rdb_);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_LoadDouble failed");
    }
    return absl::OkStatus();
  }

  absl::StatusOr<hnswlib::StringBufferUniquePtr> LoadStringBuffer(
      const size_t len) override {
    if (len == 0) {
      return absl::InvalidArgumentError("len must be > 0");
    }
    size_t len_read;
    auto str = hnswlib::StringBufferUniquePtr(
        RedisModule_LoadStringBuffer(rdb_, &len_read));
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_LoadStringBuffer failed");
    }
    if (len_read != len) {
      return absl::InternalError(
          "Inconsistency between the expected and the actual string length");
    }
    return str;
  }

  virtual absl::StatusOr<vmsdk::UniqueRedisString> LoadString() {
    auto str = vmsdk::UniqueRedisString(RedisModule_LoadString(rdb_));
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_LoadString failed");
    }
    return str;
  }

 protected:
  RDBInputStream() = default;

 private:
  RedisModuleIO *rdb_;
};

class RDBOutputStream : public hnswlib::OutputStream {
 public:
  explicit RDBOutputStream(RedisModuleIO *rdb) : rdb_(rdb) {}

  absl::Status SaveSizeT(size_t val) override {
    RedisModule_SaveUnsigned(rdb_, val);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_SaveUnsigned failed");
    }
    return absl::OkStatus();
  }

  absl::Status SaveUnsigned(unsigned int val) override {
    RedisModule_SaveUnsigned(rdb_, val);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_SaveUnsigned failed");
    }
    return absl::OkStatus();
  }

  absl::Status SaveSigned(int val) override {
    RedisModule_SaveSigned(rdb_, val);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_SaveSigned failed");
    }
    return absl::OkStatus();
  }

  absl::Status SaveDouble(double val) override {
    RedisModule_SaveDouble(rdb_, val);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_SaveDouble failed");
    }
    return absl::OkStatus();
  }

  absl::Status SaveStringBuffer(const char *str, size_t len) override {
    if (len == 0) {
      return absl::InvalidArgumentError("len must be > 0");
    }
    RedisModule_SaveStringBuffer(rdb_, str, len);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_SaveStringBuffer failed");
    }
    return absl::OkStatus();
  }

 protected:
  RDBOutputStream() = default;

 private:
  RedisModuleIO *rdb_;
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_INDEX_PERSISTENCE_H_
