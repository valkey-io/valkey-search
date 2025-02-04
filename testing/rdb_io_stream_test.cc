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

#include "src/rdb_io_stream.h"

#include <cstddef>
#include <cstdint>

#include "absl/status/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "testing/common.h"
#include "third_party/hnswlib/iostream.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {

class RdbIoStreamTest : public ValkeySearchTest {
 protected:
  void SetUp() override { TestRedisModule_Init(); }
  RedisModuleIO* fake_redis_module_io_ = (RedisModuleIO*)0xBADF00D1;
};

TEST_F(RdbIoStreamTest, LoadSizeTSuccess) {
  size_t actual_value;
  size_t expected_value = 34;
  EXPECT_CALL(*kMockRedisModule, LoadUnsigned(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.LoadSizeT(actual_value));
  EXPECT_EQ(actual_value, expected_value);
}

TEST_F(RdbIoStreamTest, LoadSizeTFailure) {
  size_t actual_value;
  size_t expected_value = 34;
  EXPECT_CALL(*kMockRedisModule, LoadUnsigned(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.LoadSizeT(actual_value).code(),
            absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, LoadUnsignedSuccess) {
  unsigned int actual_value;
  unsigned int expected_value = 34;
  EXPECT_CALL(*kMockRedisModule, LoadUnsigned(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.LoadUnsigned(actual_value));
  EXPECT_EQ(actual_value, expected_value);
}

TEST_F(RdbIoStreamTest, LoadUnsignedFailure) {
  unsigned int actual_value;
  unsigned int expected_value = 34;
  EXPECT_CALL(*kMockRedisModule, LoadUnsigned(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.LoadUnsigned(actual_value).code(),
            absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, LoadSignedSuccess) {
  int actual_value;
  int expected_value = 34;
  EXPECT_CALL(*kMockRedisModule, LoadSigned(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.LoadSigned(actual_value));
  EXPECT_EQ(actual_value, expected_value);
}

TEST_F(RdbIoStreamTest, LoadSignedFailure) {
  int actual_value;
  int expected_value = 34;
  EXPECT_CALL(*kMockRedisModule, LoadSigned(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.LoadSigned(actual_value).code(),
            absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, LoadDoubleSuccess) {
  double actual_value;
  double expected_value = 34.5;
  EXPECT_CALL(*kMockRedisModule, LoadDouble(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.LoadDouble(actual_value));
  EXPECT_EQ(actual_value, expected_value);
}

TEST_F(RdbIoStreamTest, LoadDoubleFailure) {
  double actual_value;
  double expected_value = 34.5;
  EXPECT_CALL(*kMockRedisModule, LoadDouble(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.LoadDouble(actual_value).code(),
            absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, LoadStringBufferSuccess) {
  size_t len = 5;
  auto expected_value = hnswlib::MakeStringBufferUniquePtr(len);
  auto expected_value_ptr = expected_value.get();
  EXPECT_CALL(*kMockRedisModule,
              LoadStringBuffer(fake_redis_module_io_, testing::_))
      .WillOnce(testing::DoAll(testing::SetArgPointee<1>(len),
                               testing::Return(expected_value.release())));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  auto actual_value_or = rdb_stream.LoadStringBuffer(len);
  VMSDK_EXPECT_OK(actual_value_or.status());
  EXPECT_EQ(actual_value_or.value().get(), expected_value_ptr);
}

TEST_F(RdbIoStreamTest, LoadStringBufferFailureUnexpectedLength) {
  size_t len = 5;
  auto expected_value = hnswlib::MakeStringBufferUniquePtr(len);

  EXPECT_CALL(*kMockRedisModule,
              LoadStringBuffer(fake_redis_module_io_, testing::_))
      .WillOnce(testing::DoAll(testing::SetArgPointee<1>(len + 1),
                               testing::Return(expected_value.release())));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.LoadStringBuffer(len).status().code(),
            absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, LoadStringBufferFailureZeroLength) {
  size_t len = 0;
  RDBInputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.LoadStringBuffer(len).status().code(),
            absl::StatusCode::kInvalidArgument);
}

TEST_F(RdbIoStreamTest, LoadStringBufferFailureRedisIOError) {
  size_t len = 5;
  auto expected_value = hnswlib::MakeStringBufferUniquePtr(len);
  EXPECT_CALL(*kMockRedisModule,
              LoadStringBuffer(fake_redis_module_io_, testing::_))
      .WillOnce(testing::DoAll(testing::SetArgPointee<1>(len),
                               testing::Return(expected_value.release())));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.LoadStringBuffer(len).status().code(),
            absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, LoadStringSuccess) {
  RedisModuleString* expected_value =
      TestRedisModule_CreateStringPrintf(nullptr, "test");
  EXPECT_CALL(*kMockRedisModule, LoadString(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  auto actual_value_or = rdb_stream.LoadString();
  VMSDK_EXPECT_OK(actual_value_or.status());
  EXPECT_EQ(actual_value_or.value().get(), expected_value);
}

TEST_F(RdbIoStreamTest, LoadStringFailure) {
  RedisModuleString* expected_value =
      TestRedisModule_CreateStringPrintf(nullptr, "test");
  EXPECT_CALL(*kMockRedisModule, LoadString(fake_redis_module_io_))
      .WillOnce(testing::Return(expected_value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBInputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.LoadString().status().code(),
            absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, SaveSizeTSuccess) {
  size_t value = 34;
  EXPECT_CALL(*kMockRedisModule, SaveUnsigned(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.SaveSizeT(value));
}

TEST_F(RdbIoStreamTest, SaveSizeTFailure) {
  size_t value = 34;
  EXPECT_CALL(*kMockRedisModule, SaveUnsigned(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.SaveSizeT(value).code(), absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, SaveUnsignedSuccess) {
  unsigned int value = 34;
  EXPECT_CALL(*kMockRedisModule, SaveUnsigned(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.SaveUnsigned(value));
}

TEST_F(RdbIoStreamTest, SaveUnsignedFailure) {
  unsigned int value = 34;
  EXPECT_CALL(*kMockRedisModule, SaveUnsigned(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.SaveUnsigned(value).code(), absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, SaveSignedSuccess) {
  int64_t value = 34;
  EXPECT_CALL(*kMockRedisModule, SaveSigned(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.SaveSigned(value));
}

TEST_F(RdbIoStreamTest, SaveSignedFailure) {
  int64_t value = 34;
  EXPECT_CALL(*kMockRedisModule, SaveSigned(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.SaveSigned(value).code(), absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, SaveDoubleSuccess) {
  double value = 34.5;
  EXPECT_CALL(*kMockRedisModule, SaveDouble(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.SaveDouble(value));
}

TEST_F(RdbIoStreamTest, SaveDoubleFailure) {
  double value = 34.5;
  EXPECT_CALL(*kMockRedisModule, SaveDouble(fake_redis_module_io_, value));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.SaveDouble(value).code(), absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, SaveStringBufferSuccess) {
  const char* value = "test";
  const size_t len = 5;
  EXPECT_CALL(*kMockRedisModule,
              SaveStringBuffer(fake_redis_module_io_, value, len));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(0));
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  VMSDK_EXPECT_OK(rdb_stream.SaveStringBuffer(value, len));
}

TEST_F(RdbIoStreamTest, SaveStringBufferFailureIOError) {
  const char* value = "test";
  const size_t len = 5;
  EXPECT_CALL(*kMockRedisModule,
              SaveStringBuffer(fake_redis_module_io_, value, len));
  EXPECT_CALL(*kMockRedisModule, IsIOError(fake_redis_module_io_))
      .WillOnce(testing::Return(1));
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.SaveStringBuffer(value, len).code(),
            absl::StatusCode::kInternal);
}

TEST_F(RdbIoStreamTest, SaveStringBufferFailureZeroLength) {
  const char* value = "test";
  RDBOutputStream rdb_stream(fake_redis_module_io_);
  EXPECT_EQ(rdb_stream.SaveStringBuffer(value, 0).code(),
            absl::StatusCode::kInvalidArgument);
}

}  // namespace

}  // namespace valkey_search
