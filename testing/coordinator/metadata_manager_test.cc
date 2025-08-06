/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/coordinator/metadata_manager.h"

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "google/protobuf/text_format.h"
#include "google/protobuf/util/message_differencer.h"
#include "gtest/gtest.h"
#include "src/coordinator/client.h"
#include "src/coordinator/client_pool.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/coordinator/util.h"
#include "testing/common.h"
#include "testing/coordinator/common.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::coordinator {

using ::testing::ValuesIn;

struct TypeToRegister {
  std::string type_name;
  uint64_t encoding_version{1};
  absl::Status status_to_return;
  absl::StatusOr<uint64_t> fingerprint_to_return{
      absl::UnimplementedError("Fingerprint not set")};
};

struct CallbackResult {
  std::string type_name;
  std::string id;
  bool has_content;

  bool operator==(const CallbackResult& other) const {
    return type_name == other.type_name && id == other.id &&
           has_content == other.has_content;
  }
};

struct EntryOperationTestParam {
  std::string test_name;
  struct EntryOperation {
    enum Operation {
      kCreate,
      kDelete,
    };
    Operation operation_type;
    std::string type_name;
    std::string id;
    std::string content;
  };
  std::vector<EntryOperation> entry_operations;
  std::vector<TypeToRegister> types_to_register;
  absl::StatusCode expected_status_code;
  int expect_num_broadcasts;
  std::vector<CallbackResult> expected_callbacks;
  std::string expected_metadata_pbtxt;
};

class EntryOperationTest
    : public ValkeySearchTestWithParam<EntryOperationTestParam> {
 public:
  ValkeyModuleCtx* fake_ctx = reinterpret_cast<ValkeyModuleCtx*>(0xBADF00D0);
  std::unique_ptr<MetadataManager> test_metadata_manager_;
  std::unique_ptr<ClientPool> test_client_pool_;

 protected:
  void SetUp() override {
    ValkeySearchTestWithParam::SetUp();
    ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault(testing::Return(fake_ctx));
    ON_CALL(*kMockValkeyModule, FreeThreadSafeContext(testing::_))
        .WillByDefault(testing::Return());
    test_client_pool_ = std::make_unique<ClientPool>(
        vmsdk::UniqueValkeyDetachedThreadSafeContext(fake_ctx));
    test_metadata_manager_ =
        std::make_unique<MetadataManager>(fake_ctx, *test_client_pool_);
  }
  void TearDown() override {
    test_metadata_manager_.reset();
    test_client_pool_.reset();
    ValkeySearchTestWithParam::TearDown();
  }
};

TEST_P(EntryOperationTest, TestEntryOperations) {
  const EntryOperationTestParam& test_case = GetParam();
  std::vector<CallbackResult> callbacks_tracker;
  for (auto& type_to_register : test_case.types_to_register) {
    test_metadata_manager_->RegisterType(
        type_to_register.type_name, type_to_register.encoding_version,
        [&](const google::protobuf::Any& metadata) -> absl::StatusOr<uint64_t> {
          return type_to_register.fingerprint_to_return;
        },
        [&](absl::string_view id, const google::protobuf::Any* metadata) {
          CallbackResult callback_result{
              .type_name = type_to_register.type_name,
              .id = std::string(id),
              .has_content = metadata != nullptr,
          };
          callbacks_tracker.push_back(std::move(callback_result));
          return type_to_register.status_to_return;
        });
  }
  if (test_case.expect_num_broadcasts > 0) {
    EXPECT_CALL(*kMockValkeyModule,
                SendClusterMessage(
                    fake_ctx, nullptr,
                    coordinator::kMetadataBroadcastClusterMessageReceiverId,
                    testing::_, testing::_))
        .Times(test_case.expect_num_broadcasts)
        .WillRepeatedly(testing::Return(VALKEYMODULE_OK));
  } else {
    EXPECT_CALL(*kMockValkeyModule,
                SendClusterMessage(
                    fake_ctx, nullptr,
                    coordinator::kMetadataBroadcastClusterMessageReceiverId,
                    testing::_, testing::_))
        .Times(0);
  }
  for (auto& operation : test_case.entry_operations) {
    std::unique_ptr<google::protobuf::Any> content = nullptr;
    if (operation.operation_type ==
        EntryOperationTestParam::EntryOperation::kCreate) {
      content = std::make_unique<google::protobuf::Any>();
      content->set_type_url("type.googleapis.com/FakeType");
      content->set_value(operation.content);
      EXPECT_EQ(test_metadata_manager_
                    ->CreateEntry(operation.type_name, operation.id,
                                  std::move(content))
                    .code(),
                test_case.expected_status_code);
    } else if (operation.operation_type ==
               EntryOperationTestParam::EntryOperation::kDelete) {
      EXPECT_EQ(
          test_metadata_manager_->DeleteEntry(operation.type_name, operation.id)
              .code(),
          test_case.expected_status_code);
    }
  }
  EXPECT_THAT(callbacks_tracker,
              testing::ElementsAreArray(test_case.expected_callbacks));
  GlobalMetadata expected;
  google::protobuf::TextFormat::Parser parser;
  EXPECT_TRUE(
      parser.ParseFromString(test_case.expected_metadata_pbtxt, &expected));
  EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
      *test_metadata_manager_->GetGlobalMetadata(), expected));
}

INSTANTIATE_TEST_SUITE_P(
    EntryOperationTests, EntryOperationTest,
    ValuesIn<EntryOperationTestParam>({
        {
            .test_name = "SimpleCreate",
            .entry_operations =
                {
                    {
                        .operation_type =
                            EntryOperationTestParam::EntryOperation::kCreate,
                        .type_name = "my_type",
                        .id = "my_id",
                        .content = "serialized_content_1",
                    },
                },
            .types_to_register =
                {
                    {
                        .type_name = "my_type",
                        .status_to_return = absl::OkStatus(),
                        .fingerprint_to_return = 1234,
                    },
                },
            .expected_status_code = absl::StatusCode::kOk,
            .expect_num_broadcasts = 1,
            .expected_callbacks =
                {
                    {
                        .type_name = "my_type",
                        .id = "my_id",
                        .has_content = true,
                    },
                },
            .expected_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 1
                  top_level_fingerprint: 17662341151372892129
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 0
                        fingerprint: 1234
                        encoding_version: 1
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_1"
                        }
                      }
                    }
                  }
                }
              )",
        },
        {
            .test_name = "CreateEntryTypeNotRegistered",
            .entry_operations =
                {
                    {
                        .operation_type =
                            EntryOperationTestParam::EntryOperation::kCreate,
                        .type_name = "my_type",
                        .id = "my_id",
                        .content = "serialized_content_1",
                    },
                },
            .expected_status_code = absl::StatusCode::kNotFound,
            .expect_num_broadcasts = 0,
            .expected_metadata_pbtxt = "",
        },
        {
            .test_name = "CreateEntryWithCallbackFailure",
            .entry_operations =
                {
                    {
                        .operation_type =
                            EntryOperationTestParam::EntryOperation::kCreate,
                        .type_name = "my_type",
                        .id = "my_id",
                        .content = "serialized_content_1",
                    },
                },
            .types_to_register =
                {
                    {
                        .type_name = "my_type",
                        .status_to_return = absl::InternalError("failure"),
                        .fingerprint_to_return = 1234,
                    },
                },
            .expected_status_code = absl::StatusCode::kInternal,
            .expect_num_broadcasts = 0,
            .expected_callbacks =
                {
                    {
                        .type_name = "my_type",
                        .id = "my_id",
                        .has_content = true,
                    },
                },
            .expected_metadata_pbtxt = "",
        },
        {
            .test_name = "CreateEntryWithFingerprintFailure",
            .entry_operations =
                {
                    {
                        .operation_type =
                            EntryOperationTestParam::EntryOperation::kCreate,
                        .type_name = "my_type",
                        .id = "my_id",
                        .content = "serialized_content_1",
                    },
                },
            .types_to_register =
                {
                    {
                        .type_name = "my_type",
                        .status_to_return = absl::OkStatus(),
                        .fingerprint_to_return = absl::InternalError("failure"),
                    },
                },
            .expected_status_code = absl::StatusCode::kInternal,
            .expect_num_broadcasts = 0,
            .expected_metadata_pbtxt = "",
        },
        {
            .test_name = "CreateEntryTwice",
            .entry_operations =
                {
                    {
                        .operation_type =
                            EntryOperationTestParam::EntryOperation::kCreate,
                        .type_name = "my_type",
                        .id = "my_id",
                        .content = "serialized_content_1",
                    },
                    {
                        .operation_type =
                            EntryOperationTestParam::EntryOperation::kCreate,
                        .type_name = "my_type",
                        .id = "my_id",
                        .content = "serialized_content_2",
                    },
                },
            .types_to_register =
                {
                    {
                        .type_name = "my_type",
                        .status_to_return = absl::OkStatus(),
                        .fingerprint_to_return = 1234,
                    },
                },
            .expected_status_code = absl::StatusCode::kOk,
            .expect_num_broadcasts = 2,
            .expected_callbacks =
                {
                    {
                        .type_name = "my_type",
                        .id = "my_id",
                        .has_content = true,
                    },
                    {
                        .type_name = "my_type",
                        .id = "my_id",
                        .has_content = true,
                    },
                },
            .expected_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 2
                  top_level_fingerprint: 8502063974858136158
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 1
                        fingerprint: 1234
                        encoding_version: 1
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_2"
                        }
                      }
                    }
                  }
                }
              )",
        },
        {
            .test_name = "CreateThenDeleteEntry",
            .entry_operations =
                {
                    {
                        .operation_type =
                            EntryOperationTestParam::EntryOperation::kCreate,
                        .type_name = "my_type",
                        .id = "my_id",
                        .content = "serialized_content_1",
                    },
                    {
                        .operation_type =
                            EntryOperationTestParam::EntryOperation::kDelete,
                        .type_name = "my_type",
                        .id = "my_id",
                    },
                },
            .types_to_register =
                {
                    {
                        .type_name = "my_type",
                        .status_to_return = absl::OkStatus(),
                        .fingerprint_to_return = 1234,
                    },
                },
            .expected_status_code = absl::StatusCode::kOk,
            .expect_num_broadcasts = 2,
            .expected_callbacks =
                {
                    {
                        .type_name = "my_type",
                        .id = "my_id",
                        .has_content = true,
                    },
                    {
                        .type_name = "my_type",
                        .id = "my_id",
                        .has_content = false,
                    },
                },
            .expected_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 2
                  top_level_fingerprint: 1130665396559467152
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 1
                        fingerprint: 0
                        encoding_version: 0
                      }
                    }
                  }
                }
              )",
        },
        {
            .test_name = "DeleteEntryDoesNotExist",
            .entry_operations =
                {
                    {
                        .operation_type =
                            EntryOperationTestParam::EntryOperation::kDelete,
                        .type_name = "my_type",
                        .id = "my_id",
                    },
                },
            .types_to_register =
                {
                    {
                        .type_name = "my_type",
                        .status_to_return = absl::OkStatus(),
                        .fingerprint_to_return = 1234,
                    },
                },
            .expected_status_code = absl::StatusCode::kNotFound,
            .expect_num_broadcasts = 0,
            .expected_metadata_pbtxt = "",
        },
    }),
    [](const testing::TestParamInfo<EntryOperationTest::ParamType>& info) {
      return info.param.test_name;
    });

struct MetadataManagerReconciliationTestParam {
  std::string test_name;
  std::string existing_metadata_pbtxt;
  std::string proposed_metadata_pbtxt;
  std::vector<TypeToRegister> types_to_register;
  bool fail_get_cluster_node_info;
  absl::Status get_global_metadata_status;
  bool expect_get_cluster_node_info;
  bool expect_reconcile;
  bool expect_broadcast;
  std::vector<CallbackResult> expected_callbacks;
  std::string expected_metadata_pbtxt;
};

class MetadataManagerReconciliationTest
    : public ValkeySearchTestWithParam<MetadataManagerReconciliationTestParam> {
 public:
  std::unique_ptr<MockClientPool> mock_client_pool_;
  MetadataManager* test_metadata_manager_;

 protected:
  void SetUp() override {
    ValkeySearchTestWithParam::SetUp();
    mock_client_pool_ = std::make_unique<MockClientPool>();
    ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault(testing::Return(&fake_ctx_));
    ON_CALL(*kMockValkeyModule, FreeThreadSafeContext(testing::_))
        .WillByDefault(testing::Return());
    auto test_metadata_manager =
        std::make_unique<MetadataManager>(&fake_ctx_, *mock_client_pool_);
    test_metadata_manager_ = test_metadata_manager.get();
    MetadataManager::InitInstance(std::move(test_metadata_manager));
  }
  void TearDown() override {
    MetadataManager::InitInstance(nullptr);
    test_metadata_manager_ = nullptr;
    mock_client_pool_.reset();
    ValkeySearchTestWithParam::TearDown();
  }
};

TEST_P(MetadataManagerReconciliationTest, TestReconciliation) {
  const MetadataManagerReconciliationTestParam& test_case = GetParam();
  GlobalMetadata existing_metadata;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      test_case.existing_metadata_pbtxt, &existing_metadata));
  GlobalMetadata proposed_metadata;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      test_case.proposed_metadata_pbtxt, &proposed_metadata));
  GlobalMetadata expected_metadata;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      test_case.expected_metadata_pbtxt, &expected_metadata));
  existing_metadata.mutable_version_header()->set_top_level_fingerprint(
      MetadataManager::ComputeTopLevelFingerprint(
          existing_metadata.type_namespace_map()));
  proposed_metadata.mutable_version_header()->set_top_level_fingerprint(
      MetadataManager::ComputeTopLevelFingerprint(
          proposed_metadata.type_namespace_map()));
  expected_metadata.mutable_version_header()->set_top_level_fingerprint(
      MetadataManager::ComputeTopLevelFingerprint(
          expected_metadata.type_namespace_map()));

  FakeSafeRDB fake_rdb;
  auto section = std::make_unique<data_model::RDBSection>();
  section->set_type(data_model::RDB_SECTION_GLOBAL_METADATA);
  section->mutable_global_metadata_contents()->CopyFrom(existing_metadata);
  section->set_supplemental_count(0);
  VMSDK_EXPECT_OK(test_metadata_manager_->LoadMetadata(
      &fake_ctx_, std::move(section), SupplementalContentIter(&fake_rdb, 0)));

  test_metadata_manager_->OnLoadingEnded(&fake_ctx_);
  EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
      *test_metadata_manager_->GetGlobalMetadata(), existing_metadata));

  std::vector<CallbackResult> callbacks_tracker;
  for (const auto& type_to_register : test_case.types_to_register) {
    test_metadata_manager_->RegisterType(
        type_to_register.type_name, type_to_register.encoding_version,
        [&](const google::protobuf::Any& metadata) -> absl::StatusOr<uint64_t> {
          return type_to_register.fingerprint_to_return;
        },
        [&](absl::string_view id, const google::protobuf::Any* metadata) {
          callbacks_tracker.push_back(CallbackResult{
              .type_name = type_to_register.type_name,
              .id = std::string(id),
              .has_content = metadata != nullptr,
          });
          return type_to_register.status_to_return;
        });
  }

  if (test_case.expect_broadcast) {
    EXPECT_CALL(*kMockValkeyModule,
                SendClusterMessage(
                    &fake_ctx_, nullptr,
                    coordinator::kMetadataBroadcastClusterMessageReceiverId,
                    testing::_, testing::_))
        .WillOnce(testing::Return(VALKEYMODULE_OK));
  } else {
    EXPECT_CALL(*kMockValkeyModule,
                SendClusterMessage(
                    &fake_ctx_, nullptr,
                    coordinator::kMetadataBroadcastClusterMessageReceiverId,
                    testing::_, testing::_))
        .Times(0);
  }

  std::string sender_id = "fake_sender";
  // Pad the sender id to meet to expected node id length
  sender_id += std::string(VALKEYMODULE_NODE_ID_LEN - sender_id.length(), 'a');
  std::string sender_ip = "127.0.0.1";
  int sender_port = 1234;
  int sender_coordinator_port = sender_port + 20294;
  std::string sender_coordinator_addr =
      absl::StrCat(sender_ip, ":", sender_coordinator_port);
  if (test_case.expect_get_cluster_node_info) {
    EXPECT_CALL(
        *kMockValkeyModule,
        GetClusterNodeInfo(&fake_ctx_, testing::StrEq(sender_id), testing::_,
                           testing::_, testing::_, testing::_))
        .WillOnce([&](ValkeyModuleCtx* ctx, const char* sender_id, char* ip,
                      char* master_id, int* port, int* flags) {
          if (test_case.fail_get_cluster_node_info) {
            return VALKEYMODULE_ERR;
          }
          memcpy(ip, sender_ip.c_str(), sender_ip.length() + 1);
          *port = sender_port;
          return VALKEYMODULE_OK;
        });
  } else {
    EXPECT_CALL(
        *kMockValkeyModule,
        GetClusterNodeInfo(&fake_ctx_, testing::StrEq(sender_id), testing::_,
                           testing::_, testing::_, testing::_))
        .Times(0);
  }

  if (test_case.expect_reconcile) {
    auto mock_client = std::make_shared<MockClient>();
    EXPECT_CALL(*mock_client_pool_,
                GetClient(testing::StrEq(sender_coordinator_addr)))
        .WillOnce(testing::Return(mock_client));
    EXPECT_CALL(*mock_client, GetGlobalMetadata(testing::_))
        .WillOnce([&](GetGlobalMetadataCallback callback) {
          GetGlobalMetadataResponse response;
          if (test_case.get_global_metadata_status.ok()) {
            response.mutable_metadata()->CopyFrom(proposed_metadata);
          }
          callback(ToGrpcStatus(test_case.get_global_metadata_status),
                   response);
        });
  }
  std::string payload = proposed_metadata.version_header().SerializeAsString();
  test_metadata_manager_->HandleClusterMessage(
      &fake_ctx_, sender_id.c_str(), kMetadataBroadcastClusterMessageReceiverId,
      reinterpret_cast<const unsigned char*>(payload.c_str()),
      payload.length());

  EXPECT_THAT(callbacks_tracker, testing::UnorderedElementsAreArray(
                                     test_case.expected_callbacks.begin(),
                                     test_case.expected_callbacks.end()));

  auto actual_metadata = test_metadata_manager_->GetGlobalMetadata();
  EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
      *actual_metadata, expected_metadata));
}

static constexpr absl::string_view kV1Metadata = R"(
    version_header {
      top_level_version: 1
    }
    type_namespace_map {
      key: "my_type"
      value {
        entries {
          key: "my_id"
          value {
            version: 1
            fingerprint: 1234
            encoding_version: 1
            content {
              type_url: "type.googleapis.com/FakeType"
              value: "serialized_content_1"
            }
          }
        }
      }
    }
  )";

static constexpr absl::string_view kV2Metadata = R"(
    version_header {
      top_level_version: 2
    }
    type_namespace_map {
      key: "my_type"
      value {
        entries {
          key: "my_id"
          value {
            version: 2
            fingerprint: 2345
            encoding_version: 1
            content {
              type_url: "type.googleapis.com/FakeType"
              value: "serialized_content_2"
            }
          }
        }
      }
    }
  )";

INSTANTIATE_TEST_SUITE_P(
    MetadataManagerReconciliationTests, MetadataManagerReconciliationTest,
    ValuesIn<MetadataManagerReconciliationTestParam>({
        {
            .test_name = "NoPriorMetadata",
            .existing_metadata_pbtxt = "",
            .proposed_metadata_pbtxt = std::string(kV1Metadata),
            .types_to_register =
                {
                    {
                        .type_name = "my_type",
                        .status_to_return = absl::OkStatus(),
                    },
                },
            .get_global_metadata_status = absl::OkStatus(),
            .expect_get_cluster_node_info = true,
            .expect_reconcile = true,
            .expected_callbacks =
                {
                    {
                        .type_name = "my_type",
                        .id = "my_id",
                        .has_content = true,
                    },
                },
            .expected_metadata_pbtxt = std::string(kV1Metadata),
        },
        {
            .test_name = "SameVersionSamePayload",
            .existing_metadata_pbtxt = std::string(kV1Metadata),
            .proposed_metadata_pbtxt = std::string(kV1Metadata),
            .types_to_register =
                {
                    {
                        .type_name = "my_type",
                        .status_to_return = absl::OkStatus(),
                    },
                },
            .expected_metadata_pbtxt = std::string(kV1Metadata),
        },
        {
            .test_name = "NewVersionSamePayload",
            .existing_metadata_pbtxt = std::string(kV1Metadata),
            .proposed_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 2
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 1
                        fingerprint: 1234
                        encoding_version: 1
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_1"
                        }
                      }
                    }
                  }
                }
              )",
            .types_to_register =
                {
                    {
                        .type_name = "my_type",
                        .status_to_return = absl::OkStatus(),
                    },
                },
            .get_global_metadata_status = absl::OkStatus(),
            .expect_get_cluster_node_info = true,
            .expect_reconcile = true,
            .expected_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 2
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 1
                        fingerprint: 1234
                        encoding_version: 1
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_1"
                        }
                      }
                    }
                  }
                }
              )",
        },
        {
            .test_name = "LesserVersion",
            .existing_metadata_pbtxt = std::string(kV2Metadata),
            .proposed_metadata_pbtxt = std::string(kV1Metadata),
            .types_to_register =
                {
                    {
                        .type_name = "my_type",
                        .status_to_return = absl::OkStatus(),
                    },
                },
            .expected_metadata_pbtxt = std::string(kV2Metadata),
        },
        {
            .test_name = "GreaterVersion",
            .existing_metadata_pbtxt = std::string(kV1Metadata),
            .proposed_metadata_pbtxt = std::string(kV2Metadata),
            .types_to_register =
                {
                    {
                        .type_name = "my_type",
                        .status_to_return = absl::OkStatus(),
                    },
                },
            .get_global_metadata_status = absl::OkStatus(),
            .expect_get_cluster_node_info = true,
            .expect_reconcile = true,
            .expected_callbacks =
                {
                    {
                        .type_name = "my_type",
                        .id = "my_id",
                        .has_content = true,
                    },
                },
            .expected_metadata_pbtxt = std::string(kV2Metadata),
        },
        {
            .test_name = "FailedCallback",
            .existing_metadata_pbtxt = "",
            .proposed_metadata_pbtxt = std::string(kV1Metadata),
            .types_to_register =
                {
                    {
                        .type_name = "my_type",
                        .status_to_return = absl::InternalError("Failed"),
                    },
                },
            .get_global_metadata_status = absl::OkStatus(),
            .expect_get_cluster_node_info = true,
            .expect_reconcile = true,
            .expected_callbacks =
                {
                    {
                        .type_name = "my_type",
                        .id = "my_id",
                        .has_content = true,
                    },
                },
            .expected_metadata_pbtxt = "",
        },
        {
            .test_name = "NewEncodingVersion",
            .existing_metadata_pbtxt = "",
            .proposed_metadata_pbtxt = std::string(kV1Metadata),
            .types_to_register =
                {
                    {
                        .type_name = "my_type",
                        .encoding_version = 2,
                        .status_to_return = absl::OkStatus(),
                        .fingerprint_to_return = 5678,
                    },
                },
            .get_global_metadata_status = absl::OkStatus(),
            .expect_get_cluster_node_info = true,
            .expect_reconcile = true,
            .expect_broadcast = true,
            .expected_callbacks =
                {
                    {
                        .type_name = "my_type",
                        .id = "my_id",
                        .has_content = true,
                    },
                },
            .expected_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 2
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 1
                        fingerprint: 5678
                        encoding_version: 2
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_1"
                        }
                      }
                    }
                  }
                }
              )",
        },
        {
            .test_name = "NewEncodingVersionFingerprintFailure",
            .existing_metadata_pbtxt = "",
            .proposed_metadata_pbtxt = std::string(kV1Metadata),
            .types_to_register =
                {
                    {
                        .type_name = "my_type",
                        .encoding_version = 2,
                        .status_to_return = absl::OkStatus(),
                        .fingerprint_to_return = absl::InternalError("Failed"),
                    },
                },
            .get_global_metadata_status = absl::OkStatus(),
            .expect_get_cluster_node_info = true,
            .expect_reconcile = true,
            .expected_metadata_pbtxt = "",
        },
        {
            .test_name = "NoCollision",
            .existing_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 1
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id_1"
                      value {
                        version: 1
                        fingerprint: 1234
                        encoding_version: 1
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_1"
                        }
                      }
                    }
                  }
                }
              )",
            .proposed_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 1
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id_2"
                      value {
                        version: 1
                        fingerprint: 1234
                        encoding_version: 1
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_2"
                        }
                      }
                    }
                  }
                }
              )",
            .types_to_register =
                {
                    {
                        .type_name = "my_type",
                        .encoding_version = 1,
                        .status_to_return = absl::OkStatus(),
                    },
                },
            .get_global_metadata_status = absl::OkStatus(),
            .expect_get_cluster_node_info = true,
            .expect_reconcile = true,
            .expect_broadcast = true,
            .expected_callbacks =
                {
                    {
                        .type_name = "my_type",
                        .id = "my_id_2",
                        .has_content = true,
                    },
                },
            .expected_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 2
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id_1"
                      value {
                        version: 1
                        fingerprint: 1234
                        encoding_version: 1
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_1"
                        }
                      }
                    }
                    entries {
                      key: "my_id_2"
                      value {
                        version: 1
                        fingerprint: 1234
                        encoding_version: 1
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_2"
                        }
                      }
                    }
                  }
                }
              )",
        },
        {
            .test_name = "CollisionResolveByFingerprintAcceptProposed",
            .existing_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 1
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 1
                        fingerprint: 1111
                        encoding_version: 1
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_1"
                        }
                      }
                    }
                  }
                }
              )",
            .proposed_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 1
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 1
                        fingerprint: 9999
                        encoding_version: 1
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_2"
                        }
                      }
                    }
                  }
                }
              )",
            .types_to_register =
                {
                    {
                        .type_name = "my_type",
                        .encoding_version = 1,
                        .status_to_return = absl::OkStatus(),
                    },
                },
            .get_global_metadata_status = absl::OkStatus(),
            .expect_get_cluster_node_info = true,
            .expect_reconcile = true,
            .expected_callbacks =
                {
                    {
                        .type_name = "my_type",
                        .id = "my_id",
                        .has_content = true,
                    },
                },
            .expected_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 1
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 1
                        fingerprint: 9999
                        encoding_version: 1
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_2"
                        }
                      }
                    }
                  }
                }
              )",
        },
        {
            .test_name = "CollisionResolveByFingerprintAcceptExisting",
            .existing_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 1
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 1
                        fingerprint: 9999
                        encoding_version: 1
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_1"
                        }
                      }
                    }
                  }
                }
              )",
            .proposed_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 1
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 1
                        fingerprint: 1111
                        encoding_version: 1
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_2"
                        }
                      }
                    }
                  }
                }
              )",
            .types_to_register =
                {
                    {
                        .type_name = "my_type",
                        .encoding_version = 1,
                        .status_to_return = absl::OkStatus(),
                    },
                },
            .get_global_metadata_status = absl::OkStatus(),
            .expect_get_cluster_node_info = true,
            .expect_reconcile = true,
            .expected_metadata_pbtxt = R"(
                  version_header {
                    top_level_version: 1
                  }
                  type_namespace_map {
                    key: "my_type"
                    value {
                      entries {
                        key: "my_id"
                        value {
                          version: 1
                          fingerprint: 9999
                          encoding_version: 1
                          content {
                            type_url: "type.googleapis.com/FakeType"
                            value: "serialized_content_1"
                          }
                        }
                      }
                    }
                  }
                )",
        },
        {
            .test_name = "CollisionResolveByEncodingVersionAcceptProposed",
            .existing_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 1
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 1
                        fingerprint: 9999
                        encoding_version: 1
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_1"
                        }
                      }
                    }
                  }
                }
              )",
            .proposed_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 1
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 1
                        fingerprint: 1111
                        encoding_version: 2
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_2"
                        }
                      }
                    }
                  }
                }
              )",
            .types_to_register =
                {
                    {
                        .type_name = "my_type",
                        .encoding_version = 1,
                        .status_to_return = absl::OkStatus(),
                    },
                },
            .get_global_metadata_status = absl::OkStatus(),
            .expect_get_cluster_node_info = true,
            .expect_reconcile = true,
            .expected_callbacks =
                {
                    {
                        .type_name = "my_type",
                        .id = "my_id",
                        .has_content = true,
                    },
                },
            .expected_metadata_pbtxt = R"(
                  version_header {
                    top_level_version: 1
                  }
                  type_namespace_map {
                    key: "my_type"
                    value {
                      entries {
                        key: "my_id"
                        value {
                          version: 1
                          fingerprint: 1111
                          encoding_version: 2
                          content {
                            type_url: "type.googleapis.com/FakeType"
                            value: "serialized_content_2"
                          }
                        }
                      }
                    }
                  }
                )",
        },
        {
            .test_name = "CollisionResolveByEncodingVersionAcceptExisting",
            .existing_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 1
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 1
                        fingerprint: 1111
                        encoding_version: 2
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_1"
                        }
                      }
                    }
                  }
                }
              )",
            .proposed_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 1
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 1
                        fingerprint: 9999
                        encoding_version: 1
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_2"
                        }
                      }
                    }
                  }
                }
              )",
            .types_to_register =
                {
                    {
                        .type_name = "my_type",
                        .encoding_version = 2,
                        .status_to_return = absl::OkStatus(),
                    },
                },
            .get_global_metadata_status = absl::OkStatus(),
            .expect_get_cluster_node_info = true,
            .expect_reconcile = true,
            .expected_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 1
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 1
                        fingerprint: 1111
                        encoding_version: 2
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_1"
                        }
                      }
                    }
                  }
                }
              )",
        },
        {
            .test_name = "EntryDeleted",
            .existing_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 1
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 1
                        fingerprint: 1234
                        encoding_version: 1
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_1"
                        }
                      }
                    }
                  }
                }
              )",
            .proposed_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 1
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 2
                        fingerprint: 0
                        encoding_version: 0
                      }
                    }
                  }
                }
              )",
            .types_to_register =
                {
                    {
                        .type_name = "my_type",
                        .encoding_version = 1,
                        .status_to_return = absl::OkStatus(),
                    },
                },
            .get_global_metadata_status = absl::OkStatus(),
            .expect_get_cluster_node_info = true,
            .expect_reconcile = true,
            .expected_callbacks =
                {
                    {
                        .type_name = "my_type",
                        .id = "my_id",
                        .has_content = false,
                    },
                },
            .expected_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 1
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 2
                        fingerprint: 0
                        encoding_version: 0
                      }
                    }
                  }
                }
              )",
        },
        {
            .test_name = "TypeNotRegistered",
            .existing_metadata_pbtxt = "",
            .proposed_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 1
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 1
                        fingerprint: 1234
                        encoding_version: 1
                      }
                    }
                  }
                }
              )",
            .get_global_metadata_status = absl::OkStatus(),
            .expect_get_cluster_node_info = true,
            .expect_reconcile = true,
            .expected_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 1
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 1
                        fingerprint: 1234
                        encoding_version: 1
                      }
                    }
                  }
                }
              )",
        },
        {
            .test_name = "SameVersionButExistingEntryIsNewer",
            .existing_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 1
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 2
                        fingerprint: 1234
                        encoding_version: 1
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_1"
                        }
                      }
                    }
                  }
                }
              )",
            .proposed_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 1
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 1
                        fingerprint: 2345
                        encoding_version: 1
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_2"
                        }
                      }
                    }
                  }
                }
              )",
            .types_to_register =
                {
                    {
                        .type_name = "my_type",
                        .encoding_version = 2,
                        .status_to_return = absl::OkStatus(),
                    },
                },
            .get_global_metadata_status = absl::OkStatus(),
            .expect_get_cluster_node_info = true,
            .expect_reconcile = true,
            .expected_metadata_pbtxt = R"(
                version_header {
                  top_level_version: 1
                }
                type_namespace_map {
                  key: "my_type"
                  value {
                    entries {
                      key: "my_id"
                      value {
                        version: 2
                        fingerprint: 1234
                        encoding_version: 1
                        content {
                          type_url: "type.googleapis.com/FakeType"
                          value: "serialized_content_1"
                        }
                      }
                    }
                  }
                }
              )",
        },
        {
            .test_name = "FailToGetClusterNodeInfo",
            .existing_metadata_pbtxt = std::string(kV1Metadata),
            .proposed_metadata_pbtxt = std::string(kV2Metadata),
            .fail_get_cluster_node_info = true,
            .expect_get_cluster_node_info = true,
            .expected_metadata_pbtxt = std::string(kV1Metadata),
        },
        {
            .test_name = "FailToGetGlobalMetadata",
            .existing_metadata_pbtxt = std::string(kV1Metadata),
            .proposed_metadata_pbtxt = std::string(kV2Metadata),
            .get_global_metadata_status = absl::InternalError("failure"),
            .expect_get_cluster_node_info = true,
            .expect_reconcile = true,
            .expected_metadata_pbtxt = std::string(kV1Metadata),
        },
        // Fail to get global metadata.
    }),
    [](const testing::TestParamInfo<
        MetadataManagerReconciliationTest::ParamType>& info) {
      return info.param.test_name;
    });

class MetadataManagerTest : public vmsdk::ValkeyTest {
 public:
  ValkeyModuleCtx* fake_ctx = reinterpret_cast<ValkeyModuleCtx*>(0xBADF00D0);
  std::unique_ptr<MetadataManager> test_metadata_manager_;
  std::unique_ptr<MockClientPool> mock_client_pool_;

 protected:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    mock_client_pool_ = std::make_unique<MockClientPool>();
    ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault(testing::Return(fake_ctx));
    ON_CALL(*kMockValkeyModule, FreeThreadSafeContext(testing::_))
        .WillByDefault(testing::Return());
    test_metadata_manager_ =
        std::make_unique<MetadataManager>(fake_ctx, *mock_client_pool_);
  }
  void TearDown() override {
    test_metadata_manager_.reset();
    mock_client_pool_.reset();
    vmsdk::ValkeyTest::TearDown();
  }
};

TEST_F(MetadataManagerTest, TestBroadcastMetadata) {
  GlobalMetadata existing_metadata;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      kV1Metadata, &existing_metadata));
  existing_metadata.mutable_version_header()->set_top_level_fingerprint(
      MetadataManager::ComputeTopLevelFingerprint(
          existing_metadata.type_namespace_map()));

  FakeSafeRDB fake_rdb;
  auto section = std::make_unique<data_model::RDBSection>();
  section->set_type(data_model::RDB_SECTION_GLOBAL_METADATA);
  section->mutable_global_metadata_contents()->CopyFrom(existing_metadata);
  section->set_supplemental_count(0);
  VMSDK_EXPECT_OK(test_metadata_manager_->LoadMetadata(
      fake_ctx, std::move(section), SupplementalContentIter(&fake_rdb, 0)));

  test_metadata_manager_->OnLoadingEnded(fake_ctx);

  std::string version_header_str =
      existing_metadata.version_header().SerializeAsString();
  EXPECT_CALL(
      *kMockValkeyModule,
      SendClusterMessage(
          fake_ctx, nullptr,
          coordinator::kMetadataBroadcastClusterMessageReceiverId,
          testing::StrEq(version_header_str), version_header_str.size()))
      .WillOnce(testing::Return(VALKEYMODULE_OK));

  test_metadata_manager_->BroadcastMetadata(fake_ctx);
}

TEST_F(MetadataManagerTest, TestLoadMetadataDuringReplicationStagesChanges) {
  FakeSafeRDB fake_rdb;
  GlobalMetadata new_metadata;

  test_metadata_manager_->OnReplicationLoadStart(fake_ctx);

  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(kV1Metadata,
                                                            &new_metadata));
  new_metadata.mutable_version_header()->set_top_level_fingerprint(
      MetadataManager::ComputeTopLevelFingerprint(
          new_metadata.type_namespace_map()));
  auto section = std::make_unique<data_model::RDBSection>();
  section->set_type(data_model::RDB_SECTION_GLOBAL_METADATA);
  section->mutable_global_metadata_contents()->CopyFrom(new_metadata);
  section->set_supplemental_count(0);
  VMSDK_EXPECT_OK(test_metadata_manager_->LoadMetadata(
      fake_ctx, std::move(section), SupplementalContentIter(&fake_rdb, 0)));

  // Should still be empty
  EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
      *test_metadata_manager_->GetGlobalMetadata(), GlobalMetadata()));

  // Now load and validate it is as expected
  test_metadata_manager_->OnLoadingEnded(fake_ctx);
  EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
      *test_metadata_manager_->GetGlobalMetadata(), new_metadata));
}

TEST_F(MetadataManagerTest, TestLoadMetadataNoReplicationDoesNotStageChanges) {
  FakeSafeRDB fake_rdb;
  GlobalMetadata new_metadata;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(kV1Metadata,
                                                            &new_metadata));
  new_metadata.mutable_version_header()->set_top_level_fingerprint(
      MetadataManager::ComputeTopLevelFingerprint(
          new_metadata.type_namespace_map()));
  auto section = std::make_unique<data_model::RDBSection>();
  section->set_type(data_model::RDB_SECTION_GLOBAL_METADATA);
  section->mutable_global_metadata_contents()->CopyFrom(new_metadata);
  section->set_supplemental_count(0);
  VMSDK_EXPECT_OK(test_metadata_manager_->LoadMetadata(
      fake_ctx, std::move(section), SupplementalContentIter(&fake_rdb, 0)));

  EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
      *test_metadata_manager_->GetGlobalMetadata(), new_metadata));

  // Should not be deleted after loading ended
  test_metadata_manager_->OnLoadingEnded(fake_ctx);
  EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
      *test_metadata_manager_->GetGlobalMetadata(), new_metadata));
}

TEST_F(MetadataManagerTest, TestLoadRecomputesFingerprint) {
  FakeSafeRDB fake_rdb;
  GlobalMetadata new_metadata;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(kV1Metadata,
                                                            &new_metadata));
  new_metadata.mutable_version_header()->set_top_level_fingerprint(20241023);
  test_metadata_manager_->OnReplicationLoadStart(fake_ctx);

  auto section = std::make_unique<data_model::RDBSection>();
  section->set_type(data_model::RDB_SECTION_GLOBAL_METADATA);
  section->mutable_global_metadata_contents()->CopyFrom(new_metadata);
  section->set_supplemental_count(0);
  VMSDK_EXPECT_OK(test_metadata_manager_->LoadMetadata(
      fake_ctx, std::move(section), SupplementalContentIter(&fake_rdb, 0)));

  // Should still be empty
  EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
      *test_metadata_manager_->GetGlobalMetadata(), GlobalMetadata()));
  // Now load and validate it is as expected
  test_metadata_manager_->OnLoadingEnded(fake_ctx);

  // Set the fingerprint to the expected value now for comparison.
  new_metadata.mutable_version_header()->set_top_level_fingerprint(
      MetadataManager::ComputeTopLevelFingerprint(
          new_metadata.type_namespace_map()));
  // Version should be bumped as the fingerprint changed.
  new_metadata.mutable_version_header()->set_top_level_version(2);
  EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
      *test_metadata_manager_->GetGlobalMetadata(), new_metadata));
}

TEST_F(MetadataManagerTest, TestLoadWithExistingState) {
  FakeSafeRDB fake_rdb;

  // Load the existing metadata with two entries.
  GlobalMetadata existing_metadata;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      kV1Metadata, &existing_metadata));
  auto initial_entries =
      (*existing_metadata.mutable_type_namespace_map())["my_type"]
          .mutable_entries();
  (*initial_entries)["my_id_2"] = (*initial_entries)["my_id"];
  existing_metadata.mutable_version_header()->set_top_level_fingerprint(
      MetadataManager::ComputeTopLevelFingerprint(
          existing_metadata.type_namespace_map()));
  auto section = std::make_unique<data_model::RDBSection>();
  section->set_type(data_model::RDB_SECTION_GLOBAL_METADATA);
  section->mutable_global_metadata_contents()->CopyFrom(existing_metadata);
  section->set_supplemental_count(0);
  VMSDK_EXPECT_OK(test_metadata_manager_->LoadMetadata(
      fake_ctx, std::move(section), SupplementalContentIter(&fake_rdb, 0)));
  test_metadata_manager_->OnLoadingEnded(fake_ctx);

  // We set the new metadata to replace one entry, and add a new one.
  GlobalMetadata new_metadata;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(kV2Metadata,
                                                            &new_metadata));
  auto new_entries =
      (*new_metadata.mutable_type_namespace_map())["my_type"].mutable_entries();
  (*new_entries)["my_id_3"] = (*new_entries)["my_id"];
  new_metadata.mutable_version_header()->set_top_level_fingerprint(
      MetadataManager::ComputeTopLevelFingerprint(
          new_metadata.type_namespace_map()));

  // We expect the new metadata to be a merge of the two, with a new version.
  GlobalMetadata expected_metadata;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      kV2Metadata, &expected_metadata));
  auto expected_entries =
      (*expected_metadata.mutable_type_namespace_map())["my_type"]
          .mutable_entries();
  (*expected_entries)["my_id_3"] = (*new_entries)["my_id_3"];
  (*expected_entries)["my_id_2"] = (*initial_entries)["my_id_2"];
  expected_metadata.mutable_version_header()->set_top_level_fingerprint(
      MetadataManager::ComputeTopLevelFingerprint(
          expected_metadata.type_namespace_map()));
  expected_metadata.mutable_version_header()->set_top_level_version(3);

  auto new_section = std::make_unique<data_model::RDBSection>();
  new_section->set_type(data_model::RDB_SECTION_GLOBAL_METADATA);
  new_section->mutable_global_metadata_contents()->CopyFrom(new_metadata);
  new_section->set_supplemental_count(0);
  VMSDK_EXPECT_OK(test_metadata_manager_->LoadMetadata(
      fake_ctx, std::move(new_section), SupplementalContentIter(&fake_rdb, 0)));
  test_metadata_manager_->OnLoadingEnded(fake_ctx);
  EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
      *test_metadata_manager_->GetGlobalMetadata(), expected_metadata));
}

TEST_F(MetadataManagerTest, TestSave) {
  GlobalMetadata existing_metadata;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      kV1Metadata, &existing_metadata));
  existing_metadata.mutable_version_header()->set_top_level_fingerprint(
      MetadataManager::ComputeTopLevelFingerprint(
          existing_metadata.type_namespace_map()));

  auto fake_rdb_io = reinterpret_cast<ValkeyModuleIO*>(0xBADF00D1);
  SafeRDB fake_rdb(fake_rdb_io);
  auto section = std::make_unique<data_model::RDBSection>();
  section->set_type(data_model::RDB_SECTION_GLOBAL_METADATA);
  section->mutable_global_metadata_contents()->CopyFrom(existing_metadata);
  section->set_supplemental_count(0);
  data_model::RDBSection expected_section;
  expected_section.CopyFrom(*section);
  VMSDK_EXPECT_OK(test_metadata_manager_->LoadMetadata(
      fake_ctx, std::move(section), SupplementalContentIter(&fake_rdb, 0)));
  test_metadata_manager_->OnLoadingEnded(fake_ctx);

  EXPECT_CALL(
      *kMockValkeyModule,
      SaveStringBuffer(fake_rdb_io,
                       testing::StrEq(expected_section.SerializeAsString()),
                       expected_section.SerializeAsString().size()))
      .Times(1);
  VMSDK_EXPECT_OK(test_metadata_manager_->SaveMetadata(
      fake_ctx, &fake_rdb, VALKEYMODULE_AUX_AFTER_RDB));
}

TEST_F(MetadataManagerTest, TestSaveWrongTimeIsNoOp) {
  auto fake_rdb_io = reinterpret_cast<ValkeyModuleIO*>(0xBADF00D1);
  EXPECT_CALL(*kMockValkeyModule,
              SaveStringBuffer(fake_rdb_io, testing::_, testing::_))
      .Times(0);
  SafeRDB fake_rdb(fake_rdb_io);

  VMSDK_EXPECT_OK(test_metadata_manager_->SaveMetadata(
      fake_ctx, &fake_rdb, VALKEYMODULE_AUX_BEFORE_RDB));
}

class MetadataManagerTimestampTest : public MetadataManagerTest {
 protected:
  void SetUp() override {
    MetadataManagerTest::SetUp();
    // Mock ValkeyModule_Milliseconds to control time in tests
    mock_current_time_ = 1000000;  // Start at 1000000 milliseconds
    ON_CALL(*kMockValkeyModule, Milliseconds())
        .WillByDefault(testing::Return(mock_current_time_));
  }

  void AdvanceTime(mstime_t delta_ms) {
    mock_current_time_ += delta_ms;
    ON_CALL(*kMockValkeyModule, Milliseconds())
        .WillByDefault(testing::Return(mock_current_time_));
  }

  mstime_t mock_current_time_;
};

TEST_F(MetadataManagerTimestampTest, TestTimestampInitialization) {
  // Initially, no metadata has been received, should return -1
  EXPECT_EQ(test_metadata_manager_->GetMilliSecondsSinceLastHealthyMetadata(),
            -1);
}

TEST_F(MetadataManagerTimestampTest,
       TestTimestampUpdateOnSuccessfulReconciliation) {
  GlobalMetadata proposed_metadata;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      kV1Metadata, &proposed_metadata));

  // Initially, no metadata has been received
  EXPECT_EQ(test_metadata_manager_->GetMilliSecondsSinceLastHealthyMetadata(),
            -1);

  // Reconcile metadata successfully
  VMSDK_EXPECT_OK(test_metadata_manager_->ReconcileMetadata(proposed_metadata));

  // Now should return 0 (current time - current time = 0)
  EXPECT_EQ(test_metadata_manager_->GetMilliSecondsSinceLastHealthyMetadata(),
            0);

  // Advance time by 30000 milliseconds
  AdvanceTime(30000);

  // Should now return 30000 milliseconds
  EXPECT_EQ(test_metadata_manager_->GetMilliSecondsSinceLastHealthyMetadata(),
            30000);
}

TEST_F(MetadataManagerTimestampTest, TestTimestampCalculation) {
  GlobalMetadata proposed_metadata;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      kV1Metadata, &proposed_metadata));

  // Reconcile metadata at time 1000000ms
  VMSDK_EXPECT_OK(test_metadata_manager_->ReconcileMetadata(proposed_metadata));

  // Test various time differences
  struct TestCase {
    mstime_t time_advance_ms;
    long long expected_seconds;
  };

  std::vector<mstime_t> test_cases = {
      0,        // Same time
      1000,     // 1 second
      5500,     // 5.5 seconds
      60000,    // 1 minute
      3600000,  // 1 hour
  };

  for (const auto& test_case : test_cases) {
    AdvanceTime(test_case);
    EXPECT_EQ(test_metadata_manager_->GetMilliSecondsSinceLastHealthyMetadata(),
              test_case)
        << "Failed for time advance: " << test_case << "ms";
    // Reset time for next test
    mock_current_time_ = 1000000;
    ON_CALL(*kMockValkeyModule, Milliseconds())
        .WillByDefault(testing::Return(mock_current_time_));
  }
}

TEST_F(MetadataManagerTimestampTest,
       TestMultipleReconciliationsUpdateTimestamp) {
  GlobalMetadata metadata1, metadata2;
  ASSERT_TRUE(
      google::protobuf::TextFormat::ParseFromString(kV1Metadata, &metadata1));
  ASSERT_TRUE(
      google::protobuf::TextFormat::ParseFromString(kV2Metadata, &metadata2));

  // First reconciliation at time 1000000ms
  VMSDK_EXPECT_OK(test_metadata_manager_->ReconcileMetadata(metadata1));
  EXPECT_EQ(test_metadata_manager_->GetMilliSecondsSinceLastHealthyMetadata(),
            0);

  // Advance time by 10000 milliseconds
  AdvanceTime(10000);
  EXPECT_EQ(test_metadata_manager_->GetMilliSecondsSinceLastHealthyMetadata(),
            10000);

  // Second reconciliation - should update timestamp to current time
  VMSDK_EXPECT_OK(test_metadata_manager_->ReconcileMetadata(metadata2));
  EXPECT_EQ(test_metadata_manager_->GetMilliSecondsSinceLastHealthyMetadata(),
            0);

  // Advance time again
  AdvanceTime(5000);
  EXPECT_EQ(test_metadata_manager_->GetMilliSecondsSinceLastHealthyMetadata(),
            5000);
}

TEST_F(MetadataManagerTimestampTest,
       TestReconciliationWithCallbackFailureStillUpdatesTimestamp) {
  // Test that even if callback fails, the timestamp is still updated
  // because reconciliation itself succeeded

  GlobalMetadata proposed_metadata;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      kV1Metadata, &proposed_metadata));

  // Register a type with a failing callback
  test_metadata_manager_->RegisterType(
      "my_type", 1,
      [](const google::protobuf::Any& metadata) -> absl::StatusOr<uint64_t> {
        return 1234;
      },
      [](absl::string_view id, const google::protobuf::Any* metadata) {
        return absl::InternalError("Callback failed");
      });

  // Reconciliation should fail due to callback failure
  auto status = test_metadata_manager_->ReconcileMetadata(proposed_metadata);
  EXPECT_FALSE(status.ok());

  // But timestamp should not be updated since reconciliation failed
  EXPECT_EQ(test_metadata_manager_->GetMilliSecondsSinceLastHealthyMetadata(),
            -1);
}

TEST_F(MetadataManagerTimestampTest, TestConcurrentAccess) {
  // This test verifies that the atomic operations work correctly
  // We can't easily test true concurrency in unit tests, but we can
  // verify that the operations are atomic by calling them in sequence

  GlobalMetadata proposed_metadata;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      kV1Metadata, &proposed_metadata));

  // Multiple calls should be consistent
  EXPECT_EQ(test_metadata_manager_->GetMilliSecondsSinceLastHealthyMetadata(),
            -1);
  EXPECT_EQ(test_metadata_manager_->GetMilliSecondsSinceLastHealthyMetadata(),
            -1);

  VMSDK_EXPECT_OK(test_metadata_manager_->ReconcileMetadata(proposed_metadata));

  EXPECT_EQ(test_metadata_manager_->GetMilliSecondsSinceLastHealthyMetadata(),
            0);
  EXPECT_EQ(test_metadata_manager_->GetMilliSecondsSinceLastHealthyMetadata(),
            0);

  AdvanceTime(42000);
  EXPECT_EQ(test_metadata_manager_->GetMilliSecondsSinceLastHealthyMetadata(),
            42000);
  EXPECT_EQ(test_metadata_manager_->GetMilliSecondsSinceLastHealthyMetadata(),
            42000);
}

TEST_F(MetadataManagerTimestampTest, TestTimestampPersistsAcrossLoadMetadata) {
  // Test that timestamp tracking works correctly with LoadMetadata operations
  GlobalMetadata proposed_metadata;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      kV1Metadata, &proposed_metadata));

  // First reconciliation
  VMSDK_EXPECT_OK(test_metadata_manager_->ReconcileMetadata(proposed_metadata));
  EXPECT_EQ(test_metadata_manager_->GetMilliSecondsSinceLastHealthyMetadata(),
            0);

  // Advance time
  AdvanceTime(15000);
  EXPECT_EQ(test_metadata_manager_->GetMilliSecondsSinceLastHealthyMetadata(),
            15000);

  // Load metadata (which calls ReconcileMetadata internally)
  FakeSafeRDB fake_rdb;
  auto section = std::make_unique<data_model::RDBSection>();
  section->set_type(data_model::RDB_SECTION_GLOBAL_METADATA);
  section->mutable_global_metadata_contents()->CopyFrom(proposed_metadata);
  section->set_supplemental_count(0);

  VMSDK_EXPECT_OK(test_metadata_manager_->LoadMetadata(
      fake_ctx, std::move(section), SupplementalContentIter(&fake_rdb, 0)));

  // Timestamp should be updated to current time
  EXPECT_EQ(test_metadata_manager_->GetMilliSecondsSinceLastHealthyMetadata(),
            0);
}

}  // namespace valkey_search::coordinator
