/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <memory>
#include <string>
#include <vector>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/time/clock.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "src/commands/commands.h"
#include "src/cursor.h"
#include "src/index_schema.pb.h"
#include "src/schema_manager.h"
#include "testing/common.h"
#include "vmsdk/src/testing_infra/module.h"

namespace valkey_search {

namespace {

// A cursor over the rows 0..num_rows-1, replying `[n, row...]`.
class FakeCursor : public Cursor {
 public:
  FakeCursor(size_t num_rows, uint32_t db_num = 0,
             std::string index_name = "idx",
             std::weak_ptr<IndexSchema> index_schema = {},
             absl::Duration max_idle = absl::Seconds(10),
             bool *destroyed = nullptr)
      : Cursor(db_num, std::move(index_name), std::move(index_schema),
               max_idle),
        num_rows_(num_rows),
        destroyed_(destroyed) {}
  ~FakeCursor() override {
    if (destroyed_) {
      *destroyed_ = true;
    }
  }
  size_t RemainingRows() const override { return num_rows_ - next_; }
  void ReplyRows(ValkeyModuleCtx *ctx, const std::shared_ptr<IndexSchema> &,
                 size_t count) override {
    size_t n = std::min(count, RemainingRows());
    ValkeyModule_ReplyWithArray(ctx, n + 1);
    ValkeyModule_ReplyWithLongLong(ctx, n);
    for (size_t i = 0; i < n; ++i) {
      ValkeyModule_ReplyWithLongLong(ctx, next_++);
    }
  }

 private:
  size_t num_rows_;
  size_t next_{0};
  bool *destroyed_;
};

class FTCursorTest : public ValkeySearchTest {
 protected:
  void SetUp() override {
    ValkeySearchTest::SetUp();
    CursorTable::InitInstance(std::make_unique<CursorTable>(0x12345678, 0));
    CreateIndex("idx", 0);
  }
  void TearDown() override {
    CursorTable::InitInstance(nullptr);
    ValkeySearchTest::TearDown();
  }

  void CreateIndex(absl::string_view name, int db_num) {
    data_model::IndexSchema proto;
    ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(absl::StrCat(R"(
          subscribed_key_prefixes: "prefix:"
          attribute_data_type: ATTRIBUTE_DATA_TYPE_HASH
          attributes: {
            alias: "tag"
            identifier: "tag"
            index: { tag_index: { separator: "," } }
          })"),
                                                              &proto));
    proto.set_name(name);
    proto.set_db_num(db_num);
    VMSDK_EXPECT_OK(
        SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, proto));
  }

  std::shared_ptr<IndexSchema> GetIndex(absl::string_view name,
                                        int db_num = 0) {
    return SchemaManager::Instance().GetIndexSchema(db_num, name).value();
  }

  uint64_t InsertCursor(size_t num_rows, bool *destroyed = nullptr) {
    return CursorTable::Instance().Insert(
        std::make_unique<FakeCursor>(num_rows, 0, "idx", GetIndex("idx"),
                                     absl::Seconds(10), destroyed),
        absl::Now());
  }

  absl::Status Run(absl::string_view command) {
    fake_ctx_.reply_capture.ClearReply();
    std::vector<ValkeyModuleString *> argv;
    for (auto word : absl::StrSplit(command, ' ')) {
      argv.push_back(
          ValkeyModule_CreateString(&fake_ctx_, word.data(), word.size()));
    }
    auto status = FTCursorCmd(&fake_ctx_, argv.data(), argv.size());
    for (auto *arg : argv) {
      ValkeyModule_FreeString(&fake_ctx_, arg);
    }
    return status;
  }
};

TEST_F(FTCursorTest, BadSyntax) {
  struct {
    std::string command;
    std::string error;
  } test_cases[] = {
      {"FT.CURSOR", "wrong number of arguments"},
      {"FT.CURSOR READ idx", "wrong number of arguments"},
      {"FT.CURSOR FOO idx 1", "Unknown subcommand: FOO"},
      {"FT.CURSOR READ idx abc", "Bad cursor id"},
      {"FT.CURSOR READ idx -1", "Bad cursor id"},
      {"FT.CURSOR READ idx 1 COUNT", "Missing argument"},
      {"FT.CURSOR READ idx 1 COUNT x", "Bad COUNT value"},
      {"FT.CURSOR READ idx 1 COUNT 0", "COUNT must be between 1 and 100000"},
      {"FT.CURSOR READ idx 1 COUNT 100001",
       "COUNT must be between 1 and 100000"},
      {"FT.CURSOR READ idx 1 extra", "Unexpected argument `extra`"},
      {"FT.CURSOR DEL idx 1 COUNT 5", "Unexpected argument `COUNT`"},
      {"FT.CURSOR READ nosuch 1", "Index with name 'nosuch' not found"},
      {"FT.CURSOR READ idx 1", "Cursor not found, id: 1"},
      {"FT.CURSOR DEL idx 1", "Cursor does not exist"},
  };
  for (auto &tc : test_cases) {
    auto status = Run(tc.command);
    EXPECT_FALSE(status.ok()) << tc.command;
    EXPECT_THAT(status.message(), testing::HasSubstr(tc.error)) << tc.command;
  }
}

TEST_F(FTCursorTest, ReadToExhaustion) {
  auto id = InsertCursor(5);
  auto id_str = absl::StrCat(":", id, "\r\n");
  VMSDK_EXPECT_OK(Run(absl::StrCat("FT.CURSOR READ idx ", id, " COUNT 2")));
  EXPECT_EQ(fake_ctx_.reply_capture.GetReply(),
            "*2\r\n*3\r\n:2\r\n:0\r\n:1\r\n" + id_str);
  VMSDK_EXPECT_OK(Run(absl::StrCat("FT.CURSOR read idx ", id, " count 2")));
  EXPECT_EQ(fake_ctx_.reply_capture.GetReply(),
            "*2\r\n*3\r\n:2\r\n:2\r\n:3\r\n" + id_str);
  VMSDK_EXPECT_OK(Run(absl::StrCat("FT.CURSOR READ idx ", id)));
  EXPECT_EQ(fake_ctx_.reply_capture.GetReply(),
            "*2\r\n*2\r\n:1\r\n:4\r\n:0\r\n");
  EXPECT_EQ(CursorTable::Instance().Size(), 0);
  EXPECT_THAT(Run(absl::StrCat("FT.CURSOR READ idx ", id)).message(),
              testing::HasSubstr("Cursor not found"));
}

TEST_F(FTCursorTest, Del) {
  bool destroyed = false;
  auto id = InsertCursor(5, &destroyed);
  VMSDK_EXPECT_OK(Run(absl::StrCat("FT.CURSOR DEL idx ", id)));
  EXPECT_EQ(fake_ctx_.reply_capture.GetReply(), "+OK\r\n");
  EXPECT_TRUE(destroyed);
  EXPECT_EQ(CursorTable::Instance().Size(), 0);
}

TEST_F(FTCursorTest, OtherIndexName) {
  // As in Redis, any existing index may be named.
  CreateIndex("other", 0);
  auto id = InsertCursor(5);
  VMSDK_EXPECT_OK(Run(absl::StrCat("FT.CURSOR READ other ", id, " COUNT 2")));
  EXPECT_EQ(fake_ctx_.reply_capture.GetReply(),
            absl::StrCat("*2\r\n*3\r\n:2\r\n:0\r\n:1\r\n:", id, "\r\n"));
  VMSDK_EXPECT_OK(Run(absl::StrCat("FT.CURSOR DEL other ", id)));
  EXPECT_EQ(CursorTable::Instance().Size(), 0);
}

TEST_F(FTCursorTest, WrongDb) {
  CreateIndex("idx", 1);
  auto id = InsertCursor(5);
  ON_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
      .WillByDefault(testing::Return(1));
  EXPECT_THAT(Run(absl::StrCat("FT.CURSOR READ idx ", id)).message(),
              testing::HasSubstr("Cursor not found"));
  EXPECT_THAT(Run(absl::StrCat("FT.CURSOR DEL idx ", id)).message(),
              testing::HasSubstr("Cursor does not exist"));
  EXPECT_EQ(CursorTable::Instance().Size(), 1);
}

TEST_F(FTCursorTest, IndexDroppedAndRecreated) {
  auto id = InsertCursor(5);
  VMSDK_EXPECT_OK(SchemaManager::Instance().RemoveIndexSchema(0, "idx"));
  EXPECT_THAT(Run(absl::StrCat("FT.CURSOR READ idx ", id)).message(),
              testing::HasSubstr("not found"));
  EXPECT_EQ(CursorTable::Instance().Size(), 1);
  CreateIndex("idx", 0);
  EXPECT_THAT(Run(absl::StrCat("FT.CURSOR READ idx ", id)).message(),
              testing::HasSubstr("The index was dropped while the cursor"));
  EXPECT_EQ(CursorTable::Instance().Size(), 0);

  id = InsertCursor(5);
  VMSDK_EXPECT_OK(SchemaManager::Instance().RemoveIndexSchema(0, "idx"));
  CreateIndex("idx", 0);
  VMSDK_EXPECT_OK(Run(absl::StrCat("FT.CURSOR DEL idx ", id)));
  EXPECT_EQ(CursorTable::Instance().Size(), 0);

  // Reading through another index still detects the drop.
  CreateIndex("other", 0);
  id = InsertCursor(5);
  VMSDK_EXPECT_OK(SchemaManager::Instance().RemoveIndexSchema(0, "idx"));
  EXPECT_THAT(Run(absl::StrCat("FT.CURSOR READ other ", id)).message(),
              testing::HasSubstr("The index was dropped while the cursor"));
  EXPECT_EQ(CursorTable::Instance().Size(), 0);
}

TEST_F(FTCursorTest, ExpireIdle) {
  auto &table = CursorTable::Instance();
  auto now = absl::Now();
  auto add = [&](absl::Duration max_idle) {
    return table.Insert(
        std::make_unique<FakeCursor>(1, 0, "idx", std::weak_ptr<IndexSchema>{},
                                     max_idle),
        now);
  };
  add(absl::Seconds(1));
  add(absl::Seconds(2));
  add(absl::Seconds(3));
  EXPECT_EQ(table.ExpireIdle(now), 0);
  EXPECT_EQ(table.ExpireIdle(now + absl::Seconds(1)), 1);
  EXPECT_EQ(table.Size(), 2);
  EXPECT_EQ(table.ExpireIdle(now + absl::Seconds(1)), 0);
  EXPECT_EQ(table.ExpireIdle(now + absl::Seconds(3)), 2);
  EXPECT_EQ(table.Size(), 0);
}

TEST_F(FTCursorTest, TouchPostponesExpiration) {
  auto &table = CursorTable::Instance();
  auto now = absl::Now();
  auto make = [] {
    return std::make_unique<FakeCursor>(
        1, 0, "idx", std::weak_ptr<IndexSchema>{}, absl::Seconds(10));
  };
  auto first = table.Insert(make(), now);
  auto second = table.Insert(make(), now);
  table.Touch(first, now + absl::Seconds(5));
  // Inserting and erasing other entries must not disturb first's position.
  auto third = table.Insert(make(), now);
  table.Erase(second);
  EXPECT_EQ(table.ExpireIdle(now + absl::Seconds(10)), 1);
  EXPECT_EQ(table.Lookup(third), nullptr);
  ASSERT_NE(table.Lookup(first), nullptr);
  EXPECT_EQ(table.ExpireIdle(now + absl::Seconds(15)), 1);
  EXPECT_EQ(table.Size(), 0);
}

TEST_F(FTCursorTest, IdGeneration) {
  auto now = absl::Now();
  auto make = [] {
    return std::make_unique<FakeCursor>(
        1, 0, "idx", std::weak_ptr<IndexSchema>{}, absl::Seconds(10));
  };
  // The 31 bit counter is the upper half, the CRC the lower half.
  CursorTable table(0x9abcdef0, 0);
  EXPECT_EQ(table.Insert(make(), now), uint64_t{1} << 32 | 0x9abcdef0);
  EXPECT_EQ(table.Insert(make(), now), uint64_t{2} << 32 | 0x9abcdef0);

  // With a zero CRC the counter wraps to 0 at 2^31, and the zero id is
  // skipped.
  CursorTable zero_crc(0, 0x7FFFFFFE);
  EXPECT_EQ(zero_crc.Insert(make(), now), uint64_t{0x7FFFFFFF} << 32);
  EXPECT_EQ(zero_crc.Insert(make(), now), uint64_t{1} << 32);

  // With a non-zero CRC a zero counter still yields a non-zero id.
  CursorTable wrapped(7, 0x7FFFFFFF);
  EXPECT_EQ(wrapped.Insert(make(), now), 7);
}

TEST_F(FTCursorTest, ShowCursors) {
  auto id = InsertCursor(5);
  fake_ctx_.reply_capture.ClearReply();
  VMSDK_EXPECT_OK(ShowCursorsCmd(&fake_ctx_));
  auto reply = fake_ctx_.reply_capture.GetReply();
  EXPECT_THAT(reply, testing::StartsWith(absl::StrCat("*1\r\n*2\r\n:", id)));
}

}  // namespace

}  // namespace valkey_search
