/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */
#include "src/vector_externalizer.h"

#include "gtest/gtest.h"
#include "testing/common.h"
#include "valkey_search_options.h"

namespace valkey_search {

namespace {
class VectorExternalizerTest : public vmsdk::ValkeyTest {
 public:
  VectorExternalizerTest()
      : allocator(CREATE_UNIQUE_PTR(FixedSizeAllocator, 100 * sizeof(float) + 1,
                                    true)) {}
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    EXPECT_EQ(options::GetEnableVectorSharing().SetValue(true),
              absl::OkStatus());
    vectors = DeterministicallyGenerateVectors(vector_size, 100, 10.0);
    vec = StringInternStore::Intern(
        absl::string_view((const char *)vectors[0].data(),
                          vectors.size() * sizeof(float)),
        vector_allocator.get());
    EXPECT_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(&fake_ctx))
        .WillRepeatedly(testing::Return(&ctx));
  }
  void TearDown() override {
    VectorExternalizer::Instance().Reset();
    vmsdk::ValkeyTest::TearDown();
  }
  std::vector<std::vector<float>> vectors;
  ValkeyModuleCtx fake_ctx;
  ValkeyModuleCtx ctx;
  UniqueFixedSizeAllocatorPtr allocator;
  const size_t vector_size = 10;
#ifdef SAN_BUILD
  UniqueFixedSizeAllocatorPtr vector_allocator{nullptr, nullptr};
#else
  UniqueFixedSizeAllocatorPtr vector_allocator{CREATE_UNIQUE_PTR(
      FixedSizeAllocator, vector_size * sizeof(float) + 1, true)};
#endif  // !SAN_BUILD
  const InternedStringPtr key = StringInternStore::Intern("my_key");
  const std::string attribute_identifier = "my_attr";
  InternedStringPtr vec;
  ValkeyModuleKey *key_obj = new ValkeyModuleKey{&ctx, key->Str().data()};
};

TEST_F(VectorExternalizerTest, VectorExternalizerSupported) {
  EXPECT_CALL(*kMockValkeyModule,
              GetApi(testing::StrEq("ValkeyModule_HashSetStringRef"),
                     (void **)&ValkeyModule_HashSetStringRef))
      .WillOnce(testing::Return(VALKEYMODULE_OK));
  EXPECT_CALL(*kMockValkeyModule,
              GetApi(testing::StrEq("ValkeyModule_HashExternalize"),
                     (void **)&ValkeyModule_HashExternalize))
      .WillOnce(testing::Return(VALKEYMODULE_OK));

  VectorExternalizer::Instance().Init(&fake_ctx);
  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(&ctx, vmsdk::ValkeyModuleStringValueEq(key->Str().data()),
                      VALKEYMODULE_WRITE))
      .WillOnce(testing::Return(key_obj));
  EXPECT_CALL(*kMockValkeyModule,
              HashHasStringRef(key_obj, vmsdk::ValkeyModuleStringValueEq(
                                            attribute_identifier.data())))
      .WillOnce(testing::Return(VALKEYMODULE_OK));
  EXPECT_CALL(*kMockValkeyModule,
              HashSetStringRef(
                  key_obj,
                  vmsdk::ValkeyModuleStringValueEq(attribute_identifier.data()),
                  vec->Str().data(), vec->Str().size()))
      .WillOnce(testing::Return(VALKEYMODULE_OK));

  EXPECT_TRUE(VectorExternalizer::Instance().Externalize(
      key, attribute_identifier,
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, vec,
      vec->Str().size()));
}

TEST_F(VectorExternalizerTest, VectorExternalizerAlreadyExternalized) {
  EXPECT_CALL(*kMockValkeyModule,
              GetApi(testing::StrEq("ValkeyModule_HashSetStringRef"),
                     (void **)&ValkeyModule_HashSetStringRef))
      .WillOnce(testing::Return(VALKEYMODULE_OK));
  EXPECT_CALL(*kMockValkeyModule,
              GetApi(testing::StrEq("ValkeyModule_HashExternalize"),
                     (void **)&ValkeyModule_HashExternalize))
      .WillOnce(testing::Return(VALKEYMODULE_OK));
  VectorExternalizer::Instance().Init(&fake_ctx);

  EXPECT_CALL(*kMockValkeyModule,
              OpenKey(&ctx, vmsdk::ValkeyModuleStringValueEq(key->Str().data()),
                      VALKEYMODULE_WRITE))
      .WillOnce(testing::Return(key_obj));
  EXPECT_CALL(*kMockValkeyModule,
              HashHasStringRef(key_obj, vmsdk::ValkeyModuleStringValueEq(
                                            attribute_identifier.data())))
      .WillOnce(testing::Return(VALKEYMODULE_ERR));
  EXPECT_CALL(*kMockValkeyModule,
              HashSetStringRef(testing::_, testing::_, testing::_, testing::_))
      .Times(0);
  EXPECT_FALSE(VectorExternalizer::Instance().Externalize(
      key, attribute_identifier,
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, vec,
      vec->Str().size()));
}

TEST_F(VectorExternalizerTest, VectorExternalizerNotSupported) {
  EXPECT_CALL(*kMockValkeyModule,
              GetApi(testing::StrEq("ValkeyModule_HashSetStringRef"),
                     (void **)&ValkeyModule_HashSetStringRef))
      .WillOnce(testing::Return(VALKEYMODULE_ERR));
  VectorExternalizer::Instance().Init(&fake_ctx);
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .Times(0);
  EXPECT_CALL(*kMockValkeyModule,
              HashSetStringRef(testing::_, testing::_, testing::_, testing::_))
      .Times(0);
  EXPECT_FALSE(VectorExternalizer::Instance().Externalize(
      key, attribute_identifier,
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, vec,
      vec->Str().size()));
}

TEST_F(VectorExternalizerTest, VectorExternalizerJson) {
  EXPECT_CALL(*kMockValkeyModule,
              GetApi(testing::StrEq("ValkeyModule_HashSetStringRef"),
                     (void **)&ValkeyModule_HashSetStringRef))
      .WillOnce(testing::Return(VALKEYMODULE_OK));
  EXPECT_CALL(*kMockValkeyModule,
              GetApi(testing::StrEq("ValkeyModule_HashExternalize"),
                     (void **)&ValkeyModule_HashExternalize))
      .WillOnce(testing::Return(VALKEYMODULE_OK));
  VectorExternalizer::Instance().Init(&fake_ctx);
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .Times(0);
  EXPECT_CALL(*kMockValkeyModule,
              HashSetStringRef(testing::_, testing::_, testing::_, testing::_))
      .Times(0);
  EXPECT_FALSE(VectorExternalizer::Instance().Externalize(
      key, attribute_identifier,
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON, vec,
      vec->Str().size()));
}

TEST_F(VectorExternalizerTest, VectorExternalizerDisabled) {
  VMSDK_EXPECT_OK(options::GetEnableVectorSharing().SetValue(false));

  EXPECT_CALL(*kMockValkeyModule,
              GetApi(testing::StrEq("ValkeyModule_HashSetStringRef"),
                     (void **)&ValkeyModule_HashSetStringRef))
      .WillOnce(testing::Return(VALKEYMODULE_OK));
  EXPECT_CALL(*kMockValkeyModule,
              GetApi(testing::StrEq("ValkeyModule_HashExternalize"),
                     (void **)&ValkeyModule_HashExternalize))
      .WillOnce(testing::Return(VALKEYMODULE_OK));
  VectorExternalizer::Instance().Init(&fake_ctx);
  EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(testing::_, testing::_))
      .Times(0);
  EXPECT_CALL(*kMockValkeyModule,
              HashSetStringRef(testing::_, testing::_, testing::_, testing::_))
      .Times(0);
  EXPECT_FALSE(VectorExternalizer::Instance().Externalize(
      key, attribute_identifier,
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, vec,
      vec->Str().size()));
}
}  // namespace

}  // namespace valkey_search
