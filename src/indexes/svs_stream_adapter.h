/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */
#ifndef VALKEYSEARCH_SRC_INDEXES_SVS_STREAM_ADAPTER_H_
#define VALKEYSEARCH_SRC_INDEXES_SVS_STREAM_ADAPTER_H_

#include <cstddef>
#include <cstring>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "src/rdb_serialization.h"
#include "svs/c/svs_c.h"

namespace valkey_search::indexes {

// Bridges an RDBChunkOutputStream to svs_stream_interface for
// svs_index_save_stream. Forwards every write 1:1: SVS's own std::streambuf
// already coalesces at 64 KiB, so re-buffering here would only add a memcpy
// and extra dirty pages in the fork child.
class SvsRdbOutputStream {
 public:
  explicit SvsRdbOutputStream(RDBChunkOutputStream &out) : out_(out) {}
  SvsRdbOutputStream(const SvsRdbOutputStream &) = delete;
  SvsRdbOutputStream &operator=(const SvsRdbOutputStream &) = delete;

  svs_stream_interface AsInterface() {
    static svs_stream_interface_ops ops = SVS_INIT_STREAM_OPS(nullptr, &Write);
    return svs_stream_interface{&ops, this};
  }

  // Set only on failure; discard SVS's own error and surface this instead,
  // since it avoids a message that round-tripped through a C string.
  const absl::Status &status() const { return status_; }

 private:
  static bool Write(void *self, const void *buf, size_t n,
                    svs_error_h out_err) {
    return static_cast<SvsRdbOutputStream *>(self)->WriteImpl(buf, n, out_err);
  }

  bool WriteImpl(const void *buf, size_t n, svs_error_h out_err) {
    status_ = out_.SaveChunk(static_cast<const char *>(buf), n);
    if (!status_.ok()) {
      svs_error_set(out_err, SVS_ERROR_GENERIC,
                    std::string(status_.message()).c_str());
      return false;
    }
    return true;
  }

  RDBChunkOutputStream &out_;
  absl::Status status_ = absl::OkStatus();
};

// Bridges an RDBChunkInputStream to svs_stream_interface for
// svs_index_load_stream_dynamic. Hands back at most one RDB chunk per read
// call and keeps a residual offset when a chunk outlives a request; SVS's
// underflow() accepts a short read and calls again, so the adapter never
// needs to know the total payload size.
class SvsRdbInputStream {
 public:
  explicit SvsRdbInputStream(RDBChunkInputStream &in) : in_(in) {}
  SvsRdbInputStream(const SvsRdbInputStream &) = delete;
  SvsRdbInputStream &operator=(const SvsRdbInputStream &) = delete;

  svs_stream_interface AsInterface() {
    static svs_stream_interface_ops ops = SVS_INIT_STREAM_OPS(&Read, nullptr);
    return svs_stream_interface{&ops, this};
  }

  const absl::Status &status() const { return status_; }

  // True once a read observed the underlying stream already exhausted
  // (RDBChunkInputStream::AtEnd()), i.e. this adapter itself hit the
  // sentinel rather than SVS stopping before ever asking for it. The
  // caller uses this together with AtEnd() to tell the terminator
  // invariant's two valid post-load states apart.
  bool consumed_sentinel() const { return consumed_sentinel_; }

 private:
  static size_t Read(void *self, void *buf, size_t n, svs_error_h out_err) {
    return static_cast<SvsRdbInputStream *>(self)->ReadImpl(buf, n, out_err);
  }

  size_t ReadImpl(void *buf, size_t n, svs_error_h out_err) {
    // Zero is reserved for indicating EOF
    if (n == 0) {
      status_ = absl::InternalError(
          "SVS_VAMANA RDB load: SVS requested a 0-length read");
      svs_error_set(out_err, SVS_ERROR_GENERIC,
                    std::string(status_.message()).c_str());
      return 0;
    }
    // Loops, not branches: an empty-but-present chunk is not the sentinel, and
    // returning 0 for one would signal a false clean EOF and truncate the load.
    while (residual_offset_ >= residual_.size()) {
      auto chunk = in_.LoadChunk();
      if (!chunk.ok()) {
        if (absl::IsNotFound(chunk.status())) {
          // No error set: this is clean end of stream, not a failure.
          consumed_sentinel_ = true;
          return 0;
        }
        status_ = chunk.status();
        svs_error_set(out_err, SVS_ERROR_GENERIC,
                      std::string(status_.message()).c_str());
        return 0;
      }
      residual_ = std::move(**chunk);
      residual_offset_ = 0;
    }
    size_t avail = residual_.size() - residual_offset_;
    size_t to_copy = n < avail ? n : avail;
    std::memcpy(buf, residual_.data() + residual_offset_, to_copy);
    residual_offset_ += to_copy;
    return to_copy;
  }

  RDBChunkInputStream &in_;
  std::string residual_;
  size_t residual_offset_ = 0;
  absl::Status status_ = absl::OkStatus();
  bool consumed_sentinel_ = false;
};

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_SVS_STREAM_ADAPTER_H_
