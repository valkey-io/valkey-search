/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/svs_stream_adapter.h"

#include <cstddef>
#include <cstring>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "gtest/gtest.h"
#include "src/rdb_serialization.h"
#include "svs/c/svs_c.h"
#include "testing/common.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search::indexes {
namespace {

// Owns an svs_error_h so tests don't leak it; mirrors ScopedSvsError in
// vector_svs.cc, which this file must not edit.
class ScopedSvsError {
 public:
  ScopedSvsError() : err_(svs_error_create()) {}
  ~ScopedSvsError() {
    if (err_ != nullptr) svs_error_free(err_);
  }
  ScopedSvsError(const ScopedSvsError &) = delete;
  ScopedSvsError &operator=(const ScopedSvsError &) = delete;
  svs_error_h get() const { return err_; }

 private:
  svs_error_h err_;
};

// A SafeRDB whose SaveStringBuffer always fails, for injecting a write
// failure without going through the real ValkeyModuleIO plumbing.
class FailingSaveRDB : public SafeRDB {
 public:
  FailingSaveRDB() : SafeRDB(nullptr) {}
  absl::Status SaveStringBuffer(absl::string_view buf) override {
    return absl::InternalError("injected save failure");
  }
};

// A SafeRDB whose LoadString always fails, for injecting a read failure.
class FailingLoadRDB : public SafeRDB {
 public:
  FailingLoadRDB() : SafeRDB(nullptr) {}
  absl::StatusOr<vmsdk::UniqueValkeyString> LoadString() override {
    return absl::InternalError("injected load failure");
  }
};

class SvsStreamAdapterTest : public vmsdk::ValkeyTest {};

// -- Write path ------------------------------------------------------------

TEST_F(SvsStreamAdapterTest, WriteForwardsOneToOneWithoutCoalescing) {
  FakeSafeRDB fake_rdb;
  RDBChunkOutputStream out(&fake_rdb);
  SvsRdbOutputStream adapter(out);
  svs_stream_interface iface = adapter.AsInterface();

  ScopedSvsError err;
  std::string first(3, 'a');
  std::string second(5, 'b');
  EXPECT_TRUE(
      iface.ops->write(iface.self, first.data(), first.size(), err.get()));
  EXPECT_TRUE(
      iface.ops->write(iface.self, second.data(), second.size(), err.get()));
  EXPECT_TRUE(svs_error_ok(err.get()));
  VMSDK_EXPECT_OK(out.Close());

  // Each write() call must have produced its own chunk: two chunks of the
  // exact sizes written, not one coalesced chunk of 8 bytes.
  SupplementalContentChunkIter iter(&fake_rdb);
  auto chunk1 = iter.Next();
  ASSERT_TRUE(chunk1.ok());
  EXPECT_EQ((*chunk1)->binary_content(), first);
  auto chunk2 = iter.Next();
  ASSERT_TRUE(chunk2.ok());
  EXPECT_EQ((*chunk2)->binary_content(), second);
  auto sentinel = iter.Next();
  ASSERT_TRUE(sentinel.ok());
  EXPECT_FALSE((*sentinel)->has_binary_content());
  EXPECT_FALSE(iter.HasNext());
}

TEST_F(SvsStreamAdapterTest, WriteFailurePropagatesBothWays) {
  FailingSaveRDB failing_rdb;
  RDBChunkOutputStream out(&failing_rdb);
  SvsRdbOutputStream adapter(out);
  svs_stream_interface iface = adapter.AsInterface();

  ScopedSvsError err;
  std::string data(4, 'x');
  EXPECT_FALSE(
      iface.ops->write(iface.self, data.data(), data.size(), err.get()));

  // Both halves: the stashed absl::Status and the SVS-visible error.
  EXPECT_EQ(adapter.status().code(), absl::StatusCode::kInternal);
  EXPECT_EQ(adapter.status().message(), "injected save failure");
  EXPECT_FALSE(svs_error_ok(err.get()));
}

// -- Read path ---------------------------------------------------------------

// Writes each of `contents` as its own RDB chunk, followed by the sentinel,
// into `rdb`.
void WriteChunks(FakeSafeRDB &rdb, const std::vector<std::string> &contents) {
  RDBChunkOutputStream out(&rdb);
  for (const auto &content : contents) {
    VMSDK_EXPECT_OK(out.SaveChunk(content.data(), content.size()));
  }
  VMSDK_EXPECT_OK(out.Close());
}

TEST_F(SvsStreamAdapterTest, ReadRequestLargerThanOneChunkStopsAtChunkEnd) {
  FakeSafeRDB fake_rdb;
  WriteChunks(fake_rdb, {"abc", "defgh"});
  RDBChunkInputStream in{SupplementalContentChunkIter(&fake_rdb)};
  SvsRdbInputStream adapter(in);
  svs_stream_interface iface = adapter.AsInterface();
  ScopedSvsError err;

  // A request bigger than the first chunk gets only that chunk's bytes;
  // the adapter never spans a chunk boundary within one call.
  char buf[16] = {};
  size_t n = iface.ops->read(iface.self, buf, sizeof(buf), err.get());
  EXPECT_EQ(n, 3u);
  EXPECT_EQ(std::string(buf, n), "abc");
  EXPECT_TRUE(svs_error_ok(err.get()));

  n = iface.ops->read(iface.self, buf, sizeof(buf), err.get());
  EXPECT_EQ(n, 5u);
  EXPECT_EQ(std::string(buf, n), "defgh");
}

TEST_F(SvsStreamAdapterTest, ReadChunkOutlivesRequestKeepsResidualOffset) {
  FakeSafeRDB fake_rdb;
  WriteChunks(fake_rdb, {"abcdefgh"});
  RDBChunkInputStream in{SupplementalContentChunkIter(&fake_rdb)};
  SvsRdbInputStream adapter(in);
  svs_stream_interface iface = adapter.AsInterface();
  ScopedSvsError err;

  char buf[3] = {};
  size_t n = iface.ops->read(iface.self, buf, sizeof(buf), err.get());
  EXPECT_EQ(n, 3u);
  EXPECT_EQ(std::string(buf, n), "abc");

  n = iface.ops->read(iface.self, buf, sizeof(buf), err.get());
  EXPECT_EQ(n, 3u);
  EXPECT_EQ(std::string(buf, n), "def");

  n = iface.ops->read(iface.self, buf, sizeof(buf), err.get());
  EXPECT_EQ(n, 2u);
  EXPECT_EQ(std::string(buf, n), "gh");
}

TEST_F(SvsStreamAdapterTest, ReadEofReturnsZeroWithoutErrorSet) {
  FakeSafeRDB fake_rdb;
  WriteChunks(fake_rdb, {"abc"});
  RDBChunkInputStream in{SupplementalContentChunkIter(&fake_rdb)};
  SvsRdbInputStream adapter(in);
  svs_stream_interface iface = adapter.AsInterface();
  ScopedSvsError err;

  char buf[8] = {};
  size_t n = iface.ops->read(iface.self, buf, sizeof(buf), err.get());
  EXPECT_EQ(n, 3u);

  // Adapter itself hits the sentinel: state 2 of the terminator invariant.
  n = iface.ops->read(iface.self, buf, sizeof(buf), err.get());
  EXPECT_EQ(n, 0u);
  EXPECT_TRUE(svs_error_ok(err.get()));
  EXPECT_TRUE(adapter.consumed_sentinel());
  EXPECT_TRUE(in.AtEnd());
}

TEST_F(SvsStreamAdapterTest, ReadZeroLengthRequestIsAnError) {
  FakeSafeRDB fake_rdb;
  WriteChunks(fake_rdb, {"abc"});
  RDBChunkInputStream in{SupplementalContentChunkIter(&fake_rdb)};
  SvsRdbInputStream adapter(in);
  svs_stream_interface iface = adapter.AsInterface();

  // Zero is indistinguishable from end of stream, so a 0-length request is
  // refused outright and the stream is left untouched.
  char buf[8] = {};
  {
    ScopedSvsError err;
    EXPECT_EQ(iface.ops->read(iface.self, buf, 0, err.get()), 0u);
    EXPECT_FALSE(svs_error_ok(err.get()));
    EXPECT_EQ(adapter.status().code(), absl::StatusCode::kInternal);
    EXPECT_FALSE(adapter.consumed_sentinel());
  }

  // No chunk was consumed: a real read still gets the first chunk.
  ScopedSvsError err;
  size_t n = iface.ops->read(iface.self, buf, sizeof(buf), err.get());
  EXPECT_EQ(n, 3u);
  EXPECT_EQ(std::string(buf, n), "abc");
  EXPECT_TRUE(svs_error_ok(err.get()));
}

TEST_F(SvsStreamAdapterTest, ReadSkipsEmptyButPresentChunk) {
  FakeSafeRDB fake_rdb;
  WriteChunks(fake_rdb, {"abc", "", "def"});
  RDBChunkInputStream in{SupplementalContentChunkIter(&fake_rdb)};
  SvsRdbInputStream adapter(in);
  svs_stream_interface iface = adapter.AsInterface();
  ScopedSvsError err;

  char buf[8] = {};
  EXPECT_EQ(iface.ops->read(iface.self, buf, sizeof(buf), err.get()), 3u);

  // An empty chunk still has binary_content, so it is not the sentinel;
  // returning 0 for it would report a false clean EOF and truncate the load.
  size_t n = iface.ops->read(iface.self, buf, sizeof(buf), err.get());
  EXPECT_EQ(n, 3u);
  EXPECT_EQ(std::string(buf, n), "def");
  EXPECT_TRUE(svs_error_ok(err.get()));
  EXPECT_FALSE(adapter.consumed_sentinel());
}

TEST_F(SvsStreamAdapterTest, ReadFailurePropagatesBothWaysAndSetsError) {
  FailingLoadRDB failing_rdb;
  RDBChunkInputStream in{SupplementalContentChunkIter(&failing_rdb)};
  SvsRdbInputStream adapter(in);
  svs_stream_interface iface = adapter.AsInterface();
  ScopedSvsError err;

  char buf[8] = {};
  size_t n = iface.ops->read(iface.self, buf, sizeof(buf), err.get());
  EXPECT_EQ(n, 0u);

  // A read failure must set the SVS error (0 without one means clean EOF)
  // and stash the local status for the caller to prefer.
  EXPECT_FALSE(svs_error_ok(err.get()));
  EXPECT_EQ(adapter.status().code(), absl::StatusCode::kInternal);
  // SupplementalContentChunkIter replaces the underlying SafeRDB error with
  // its own wording, so the adapter stashes that rather than "injected".
  EXPECT_EQ(adapter.status().message(),
            "IO error while reading serialized SupplementalContentChunk from "
            "RDB");
  EXPECT_FALSE(adapter.consumed_sentinel());
}

// -- Terminator invariant: the two valid post-load states ------------------

TEST_F(SvsStreamAdapterTest, TerminatorStateOneSvsStopsWithoutReadingSentinel) {
  FakeSafeRDB fake_rdb;
  WriteChunks(fake_rdb, {"abc"});
  RDBChunkInputStream in{SupplementalContentChunkIter(&fake_rdb)};
  SvsRdbInputStream adapter(in);
  svs_stream_interface iface = adapter.AsInterface();
  ScopedSvsError err;

  // SVS reads exactly the payload and never asks again.
  char buf[3] = {};
  size_t n = iface.ops->read(iface.self, buf, sizeof(buf), err.get());
  EXPECT_EQ(n, 3u);

  // The adapter's own bool never observed the sentinel...
  EXPECT_FALSE(adapter.consumed_sentinel());
  // ...but the stream is already drained: SupplementalContentChunkIter reads
  // one chunk ahead, so it buffered the sentinel while delivering "abc".
  EXPECT_TRUE(in.AtEnd());
}

TEST_F(SvsStreamAdapterTest, TerminatorStateTwoSvsAsksAgainAndHitsEof) {
  FakeSafeRDB fake_rdb;
  WriteChunks(fake_rdb, {"abc"});
  RDBChunkInputStream in{SupplementalContentChunkIter(&fake_rdb)};
  SvsRdbInputStream adapter(in);
  svs_stream_interface iface = adapter.AsInterface();
  ScopedSvsError err;

  char buf[3] = {};
  ASSERT_EQ(iface.ops->read(iface.self, buf, sizeof(buf), err.get()), 3u);
  // SVS asks once more, past the payload end.
  size_t n = iface.ops->read(iface.self, buf, sizeof(buf), err.get());
  EXPECT_EQ(n, 0u);
  EXPECT_TRUE(svs_error_ok(err.get()));

  EXPECT_TRUE(adapter.consumed_sentinel());
  EXPECT_TRUE(in.AtEnd());
}

}  // namespace
}  // namespace valkey_search::indexes
