/*
 * Copyright (c) 2025, valkey-search contributors
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

#ifndef VALKEYSEARCH_SRC_RDB_SERIALIZATION_H_
#define VALKEYSEARCH_SRC_RDB_SERIALIZATION_H_

#include <cstddef>
#include <cstdlib>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "src/rdb_section.pb.h"
#include "third_party/hnswlib/iostream.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

constexpr uint32_t kCurrentEncVer = 1;
// Format is 0xMMmmpp (M=major, m=minor, p=patch)
constexpr uint64_t kCurrentSemanticVersion = 0x010000;
constexpr absl::string_view kValkeySearchModuleTypeName{"Vk-Search"};

class SafeRDB;
class SupplementalContentChunkIter;
class SupplementalContentIter;
class RDBSectionIter;

// Callback to load a single RDBSection from the RDB. Return value is an error
// if the load fails. Supplemental content iterator must be fully iterated (and
// any chunks within it iterated as well) or loading will not succeed.
using RDBSectionLoadCallback = absl::AnyInvocable<absl::Status(
    RedisModuleCtx *ctx, std::unique_ptr<data_model::RDBSection> section,
    SupplementalContentIter &&supplemental_iter)>;

// Callback to save an arbitrary count of RDBSections to RDB on aux save events.
// Return value is an error status or the count of writted RDBSections.
using RDBSectionSaveCallback = absl::AnyInvocable<absl::Status(
    RedisModuleCtx *ctx, SafeRDB *rdb, int when)>;

using RDBSectionCountCallback =
    absl::AnyInvocable<int(RedisModuleCtx *ctx, int when)>;

using RDBSectionMinSemVerCallback =
    absl::AnyInvocable<int(RedisModuleCtx *ctx, int when)>;

using RDBSectionCallbacks = struct RDBSectionCallbacks {
  RDBSectionLoadCallback load;
  RDBSectionSaveCallback save;
  RDBSectionCountCallback section_count;
  RDBSectionMinSemVerCallback minimum_semantic_version;
};

// Static mapping from section type to callback.
extern absl::flat_hash_map<data_model::RDBSectionType, RDBSectionCallbacks>
    kRegisteredRDBSectionCallbacks;

std::string HumanReadableSemanticVersion(uint64_t semantic_version) {
  return absl::StrFormat("%d.%d.%d", (semantic_version >> 16) & 0xFF,
                         (semantic_version >> 8) & 0xFF,
                         semantic_version & 0xFF);
}

/* SafeRDB wraps a RedisModuleIO object and performs IO error checking,
 * returning absl::StatusOr to force error handling on the caller side. */
class SafeRDB {
 public:
  explicit SafeRDB(RedisModuleIO *rdb) : rdb_(rdb) {}

  virtual absl::StatusOr<size_t> LoadSizeT() {
    auto val = RedisModule_LoadUnsigned(rdb_);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_LoadUnsigned failed");
    }
    return val;
  }

  virtual absl::StatusOr<unsigned int> LoadUnsigned() {
    auto val = RedisModule_LoadUnsigned(rdb_);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_LoadUnsigned failed");
    }
    return val;
  }

  virtual absl::StatusOr<int> LoadSigned() {
    auto val = RedisModule_LoadSigned(rdb_);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_LoadSigned failed");
    }
    return val;
  }

  virtual absl::StatusOr<double> LoadDouble() {
    auto val = RedisModule_LoadDouble(rdb_);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_LoadDouble failed");
    }
    return val;
  }

  virtual absl::StatusOr<vmsdk::UniqueRedisString> LoadString() {
    auto str = vmsdk::UniqueRedisString(RedisModule_LoadString(rdb_));
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_LoadString failed");
    }
    return str;
  }
  bool operator==(const SafeRDB &other) const { return rdb_ == other.rdb_; }

  virtual absl::Status SaveSizeT(size_t val) {
    RedisModule_SaveUnsigned(rdb_, val);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_SaveUnsigned failed");
    }
    return absl::OkStatus();
  }

  virtual absl::Status SaveUnsigned(unsigned int val) {
    RedisModule_SaveUnsigned(rdb_, val);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_SaveUnsigned failed");
    }
    return absl::OkStatus();
  }

  virtual absl::Status SaveSigned(int val) {
    RedisModule_SaveSigned(rdb_, val);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_SaveSigned failed");
    }
    return absl::OkStatus();
  }

  virtual absl::Status SaveDouble(double val) {
    RedisModule_SaveDouble(rdb_, val);
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_SaveDouble failed");
    }
    return absl::OkStatus();
  }

  virtual absl::Status SaveStringBuffer(absl::string_view buf) {
    RedisModule_SaveStringBuffer(rdb_, buf.data(), buf.size());
    if (RedisModule_IsIOError(rdb_)) {
      return absl::InternalError("RedisModule_SaveStringBuffer failed");
    }
    return absl::OkStatus();
  }

 protected:
  SafeRDB() = default;

 private:
  RedisModuleIO *rdb_;
};

/* SupplementalContentChunkIter is an iterator over chunks of a supplemental
 * content section in the RDB. */
class SupplementalContentChunkIter {
 public:
  SupplementalContentChunkIter() = delete;
  SupplementalContentChunkIter(SafeRDB *rdb) : rdb_(rdb) {
    // Buffer one chunk ahead so that done_ is correctly reflected.
    ReadNextChunk();
  }
  SupplementalContentChunkIter(SupplementalContentChunkIter &&other) noexcept
      : rdb_(other.rdb_),
        curr_chunk_(std::move(other.curr_chunk_)),
        done_(other.done_) {
    other.curr_chunk_ = absl::InternalError("Use after move");
    other.done_ = true;
  };
  SupplementalContentChunkIter(SupplementalContentChunkIter &other) = delete;
  SupplementalContentChunkIter &operator=(
      SupplementalContentChunkIter &&other) noexcept {
    rdb_ = other.rdb_;
    done_ = other.done_;
    curr_chunk_ = std::move(other.curr_chunk_);
    other.curr_chunk_ = absl::InternalError("Use after move");
    other.done_ = true;
    return *this;
  }
  SupplementalContentChunkIter &operator=(SupplementalContentChunkIter &other) =
      delete;
  absl::StatusOr<std::unique_ptr<data_model::SupplementalContentChunk>> Next();
  bool HasNext() const { return !done_; }
  ~SupplementalContentChunkIter() {
    if (!done_) {
      VMSDK_LOG(WARNING, nullptr)
          << "SupplementalContentChunkIter was not fully iterated";
      DCHECK(done_);
    }
  }

 private:
  void ReadNextChunk();

  SafeRDB *rdb_;
  absl::StatusOr<std::unique_ptr<data_model::SupplementalContentChunk>>
      curr_chunk_;
  bool done_ = false;
};

/* SupplementalContentIter implements an iterator over the SupplementalContent
 * of an RDBSection. After each iteration via Next(), IterateChunks() must be
 * called and iterated or RDB loading will not succeed. */
class SupplementalContentIter {
 public:
  SupplementalContentIter() = delete;
  SupplementalContentIter(SafeRDB *rdb, size_t remaining)
      : rdb_(rdb), remaining_(remaining) {}
  SupplementalContentIter(SupplementalContentIter &&other) noexcept
      : rdb_(std::move(other.rdb_)), remaining_(other.remaining_) {
    other.remaining_ = 0;
  };
  SupplementalContentIter(SupplementalContentIter &other) = delete;
  SupplementalContentIter &operator=(SupplementalContentIter &&other) noexcept {
    rdb_ = std::move(other.rdb_);
    remaining_ = other.remaining_;
    other.remaining_ = 0;
    return *this;
  }
  ~SupplementalContentIter() {
    if (remaining_ > 0) {
      VMSDK_LOG(WARNING, nullptr)
          << "SupplementalContentIter was not fully iterated. remaining_ == "
          << remaining_;
      DCHECK(remaining_ == 0);
    }
  }
  SupplementalContentIter &operator=(SupplementalContentIter &other) = delete;
  absl::StatusOr<std::unique_ptr<data_model::SupplementalContentHeader>> Next();
  bool HasNext() { return remaining_ > 0; }
  SupplementalContentChunkIter IterateChunks() { return {rdb_}; }

 private:
  SafeRDB *rdb_;
  size_t remaining_;
};

/* RDBSectionIter implements an iterator over the RDBSections contained within
 * the RDB. After each iteration via Next(), IterateSupplementalContent() must
 * be called and iterated or RDB loading will not succeed. */
class RDBSectionIter {
 public:
  RDBSectionIter() = delete;
  RDBSectionIter(SafeRDB *rdb, size_t remaining)
      : rdb_(rdb), remaining_(remaining) {}

  RDBSectionIter(RDBSectionIter &&other) noexcept
      : rdb_(std::move(other.rdb_)),
        remaining_(other.remaining_),
        curr_supplemental_count_(other.curr_supplemental_count_) {
    other.remaining_ = 0;
  };
  RDBSectionIter(RDBSectionIter &other) = delete;
  RDBSectionIter &operator=(RDBSectionIter &&other) noexcept {
    rdb_ = std::move(other.rdb_);
    remaining_ = other.remaining_;
    curr_supplemental_count_ = other.curr_supplemental_count_;
    other.remaining_ = 0;
    other.curr_supplemental_count_ = 0;
    return *this;
  }
  RDBSectionIter &operator=(RDBSectionIter &other) = delete;
  SupplementalContentIter IterateSupplementalContent() {
    return {rdb_, curr_supplemental_count_};
  }
  absl::StatusOr<std::unique_ptr<data_model::RDBSection>> Next();
  bool HasNext() { return remaining_ > 0; }
  ~RDBSectionIter() {
    if (remaining_ > 0) {
      VMSDK_LOG(WARNING, nullptr)
          << "RDBSectionIter was not fully iterated. remaining_ == "
          << remaining_;
      DCHECK(remaining_ == 0);
    }
  }

 private:
  SafeRDB *rdb_;
  size_t remaining_;
  size_t curr_supplemental_count_ = 0;
};

class RDBChunkInputStream : public hnswlib::InputStream {
 public:
  explicit RDBChunkInputStream(SupplementalContentChunkIter &&iter)
      : iter_(std::move(iter)) {}
  RDBChunkInputStream(RDBChunkInputStream &&other) noexcept
      : iter_(std::move(other.iter_)) {}
  RDBChunkInputStream(RDBChunkInputStream &other) = delete;
  RDBChunkInputStream &operator=(RDBChunkInputStream &&other) noexcept {
    iter_ = std::move(other.iter_);
    return *this;
  }
  RDBChunkInputStream &operator=(RDBChunkInputStream &other) = delete;
  absl::StatusOr<std::unique_ptr<std::string>> LoadChunk() override;

 private:
  SupplementalContentChunkIter iter_;
};

class RDBChunkOutputStream : public hnswlib::OutputStream {
 public:
  explicit RDBChunkOutputStream(SafeRDB *rdb) : rdb_(rdb) {}
  RDBChunkOutputStream(RDBChunkOutputStream &&other) noexcept
      : rdb_(other.rdb_), closed_(other.closed_) {
    other.closed_ = true;
  }
  RDBChunkOutputStream(RDBChunkOutputStream &other) = delete;
  RDBChunkOutputStream &operator=(RDBChunkOutputStream &&other) noexcept {
    rdb_ = other.rdb_;
    closed_ = other.closed_;
    other.closed_ = true;
    return *this;
  }
  RDBChunkOutputStream &operator=(RDBChunkOutputStream &other) = delete;
  ~RDBChunkOutputStream() override {
    if (!closed_) {
      auto status = Close();
      if (!status.ok()) {
        VMSDK_LOG(WARNING, nullptr)
            << "Failed to write final chunk on closing output stream: "
            << status.message();
      }
    }
  }
  absl::Status SaveChunk(const char *data, size_t len) override;
  absl::Status Close();

 private:
  SafeRDB *rdb_;
  bool closed_ = false;
};

// Register for an RDB callback on RDB load and save, based on the RDBSection
// type. For load callbacks, a callback is made for each matching RDBSection in
// the loaded RDB. For save callbacks, a single callback is always given.
// Callbacks are made from the main thread, so thread safety should be
// guaranteed by the callback implementation.
void RegisterRDBCallback(data_model::RDBSectionType type,
                         RDBSectionCallbacks callbacks);
void ClearRDBCallbacks();
absl::Status PerformRDBLoad(RedisModuleCtx *ctx, SafeRDB *rdb, int encver);
int AuxLoadCallback(RedisModuleIO *rdb, int encver, int when);
absl::Status PerformRDBSave(RedisModuleCtx *ctx, SafeRDB *rdb, int when);
void AuxSaveCallback(RedisModuleIO *rdb, int when);
absl::Status RegisterModuleType(RedisModuleCtx *ctx);

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_RDB_SERIALIZATION_H_
