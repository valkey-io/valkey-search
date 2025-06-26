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
    ValkeyModuleCtx *ctx, std::unique_ptr<data_model::RDBSection> section,
    SupplementalContentIter &&supplemental_iter)>;

// Callback to save an arbitrary count of RDBSections to RDB on aux save events.
// Return value is an error status or the count of written RDBSections.
using RDBSectionSaveCallback = absl::AnyInvocable<absl::Status(
    ValkeyModuleCtx *ctx, SafeRDB *rdb, int when)>;

using RDBSectionCountCallback =
    absl::AnyInvocable<int(ValkeyModuleCtx *ctx, int when)>;

using RDBSectionMinSemVerCallback =
    absl::AnyInvocable<int(ValkeyModuleCtx *ctx, int when)>;

using RDBSectionCallbacks = struct RDBSectionCallbacks {
  RDBSectionLoadCallback load;
  RDBSectionSaveCallback save;
  RDBSectionCountCallback section_count;
  RDBSectionMinSemVerCallback minimum_semantic_version;
};

// Static mapping from section type to callback.
extern absl::flat_hash_map<data_model::RDBSectionType, RDBSectionCallbacks>
    kRegisteredRDBSectionCallbacks;

inline std::string HumanReadableSemanticVersion(uint64_t semantic_version) {
  return absl::StrFormat("%d.%d.%d", (semantic_version >> 16) & 0xFF,
                         (semantic_version >> 8) & 0xFF,
                         semantic_version & 0xFF);
}

/* SafeRDB wraps a ValkeyModuleIO object and performs IO error checking,
 * returning absl::StatusOr to force error handling on the caller side. */
class SafeRDB {
 public:
  explicit SafeRDB(ValkeyModuleIO *rdb) : rdb_(rdb) {}

  virtual absl::StatusOr<size_t> LoadSizeT() {
    auto val = ValkeyModule_LoadUnsigned(rdb_);
    if (ValkeyModule_IsIOError(rdb_)) {
      return absl::InternalError("ValkeyModule_LoadUnsigned failed");
    }
    return val;
  }

  virtual absl::StatusOr<unsigned int> LoadUnsigned() {
    auto val = ValkeyModule_LoadUnsigned(rdb_);
    if (ValkeyModule_IsIOError(rdb_)) {
      return absl::InternalError("ValkeyModule_LoadUnsigned failed");
    }
    return val;
  }

  virtual absl::StatusOr<int> LoadSigned() {
    auto val = ValkeyModule_LoadSigned(rdb_);
    if (ValkeyModule_IsIOError(rdb_)) {
      return absl::InternalError("ValkeyModule_LoadSigned failed");
    }
    return val;
  }

  virtual absl::StatusOr<double> LoadDouble() {
    auto val = ValkeyModule_LoadDouble(rdb_);
    if (ValkeyModule_IsIOError(rdb_)) {
      return absl::InternalError("ValkeyModule_LoadDouble failed");
    }
    return val;
  }

  virtual absl::StatusOr<vmsdk::UniqueValkeyString> LoadString() {
    auto str = vmsdk::UniqueValkeyString(ValkeyModule_LoadString(rdb_));
    if (ValkeyModule_IsIOError(rdb_)) {
      return absl::InternalError("ValkeyModule_LoadString failed");
    }
    return str;
  }
  bool operator==(const SafeRDB &other) const { return rdb_ == other.rdb_; }

  virtual absl::Status SaveSizeT(size_t val) {
    ValkeyModule_SaveUnsigned(rdb_, val);
    if (ValkeyModule_IsIOError(rdb_)) {
      return absl::InternalError("ValkeyModule_SaveUnsigned failed");
    }
    return absl::OkStatus();
  }

  virtual absl::Status SaveUnsigned(unsigned int val) {
    ValkeyModule_SaveUnsigned(rdb_, val);
    if (ValkeyModule_IsIOError(rdb_)) {
      return absl::InternalError("ValkeyModule_SaveUnsigned failed");
    }
    return absl::OkStatus();
  }

  virtual absl::Status SaveSigned(int val) {
    ValkeyModule_SaveSigned(rdb_, val);
    if (ValkeyModule_IsIOError(rdb_)) {
      return absl::InternalError("ValkeyModule_SaveSigned failed");
    }
    return absl::OkStatus();
  }

  virtual absl::Status SaveDouble(double val) {
    ValkeyModule_SaveDouble(rdb_, val);
    if (ValkeyModule_IsIOError(rdb_)) {
      return absl::InternalError("ValkeyModule_SaveDouble failed");
    }
    return absl::OkStatus();
  }

  virtual absl::Status SaveStringBuffer(absl::string_view buf) {
    ValkeyModule_SaveStringBuffer(rdb_, buf.data(), buf.size());
    if (ValkeyModule_IsIOError(rdb_)) {
      return absl::InternalError("ValkeyModule_SaveStringBuffer failed");
    }
    return absl::OkStatus();
  }

 protected:
  SafeRDB() = default;

 private:
  ValkeyModuleIO *rdb_;
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
absl::Status PerformRDBLoad(ValkeyModuleCtx *ctx, SafeRDB *rdb, int encver);
int AuxLoadCallback(ValkeyModuleIO *rdb, int encver, int when);
absl::Status PerformRDBSave(ValkeyModuleCtx *ctx, SafeRDB *rdb, int when);
void AuxSaveCallback(ValkeyModuleIO *rdb, int when);
absl::Status RegisterModuleType(ValkeyModuleCtx *ctx);

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_RDB_SERIALIZATION_H_
