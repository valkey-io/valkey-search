import re

with open("testing/vector_registry_test.cc", "r") as f:
    content = f.read()

to_remove = [
    """  auto match_valkey_vec = vmsdk::MakeUniqueValkeyString(vec_str);
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(testing::_, VALKEYMODULE_HASH_NONE, testing::_,
                      testing::An<ValkeyModuleString **>(),
                      testing::TypedEq<void *>(nullptr)))
      .WillRepeatedly(
          testing::DoAll(testing::SetArgPointee<3>(match_valkey_vec.release()),
                         testing::Return(VALKEYMODULE_OK)));""",
                         
    """  auto diff_valkey_vec = vmsdk::MakeUniqueValkeyString(diff_str);
  EXPECT_CALL(*kMockValkeyModule,
              HashGet(testing::_, VALKEYMODULE_HASH_NONE, testing::_,
                      testing::An<ValkeyModuleString **>(),
                      testing::TypedEq<void *>(nullptr)))
      .WillRepeatedly(
          testing::DoAll(testing::SetArgPointee<3>(diff_valkey_vec.release()),
                         testing::Return(VALKEYMODULE_OK)));"""
]

for block in to_remove:
    content = content.replace(block, "")

with open("testing/vector_registry_test.cc", "w") as f:
    f.write(content)

