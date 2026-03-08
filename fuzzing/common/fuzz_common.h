/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef FUZZING_COMMON_FUZZ_COMMON_H_
#define FUZZING_COMMON_FUZZ_COMMON_H_

#include <atomic>
#include <memory>
#include <string>
#include <vector>

// Include the Valkey module API header first to get forward declarations
// and function pointer declarations.
#include "vmsdk/src/valkey_module_api/valkey_module.h"

// Provide concrete definitions for opaque Valkey module types that are
// only forward-declared in valkey_module.h. These are needed because
// the fuzzer cannot link against a real Valkey server.
struct ValkeyModuleCtx {};
struct ValkeyModuleIO {};
struct ValkeyModuleBlockedClient {};
struct ValkeyModuleScanCursor {
  int cursor{0};
};
struct ValkeyModuleString {
  std::string data;
  std::atomic<int> cnt{1};
};
struct ValkeyModuleKey {
  std::string key;
};
struct ValkeyModuleType {};

// Now include project headers that use these types.
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/attribute_data_type.h"
#include "src/index_schema.h"
#include "src/index_schema.pb.h"
#include "src/query/search.h"
#include "src/rdb_serialization.h"

// ---------------------------------------------------------------------------
// FuzzIndexSchema - proper subclass of IndexSchema with direct overrides,
// modeled after MockIndexSchema in testing/common.h but without any
// gtest/gmock dependency.
// ---------------------------------------------------------------------------
class FuzzIndexSchema : public valkey_search::IndexSchema {
 public:
  static std::shared_ptr<FuzzIndexSchema> Create(
      ValkeyModuleCtx *ctx, absl::string_view key,
      const std::vector<absl::string_view> &subscribed_key_prefixes,
      std::unique_ptr<valkey_search::AttributeDataType> attribute_data_type);

  void OnLoadingEnded(ValkeyModuleCtx *) override {}
  void OnSwapDB(ValkeyModuleSwapDbInfo *) override {}

  absl::Status RDBSave(valkey_search::SafeRDB *) const override {
    return absl::OkStatus();
  }

  absl::StatusOr<std::string> GetIdentifier(
      absl::string_view attribute_alias) const override {
    return std::string(attribute_alias);
  }

 private:
  FuzzIndexSchema(
      ValkeyModuleCtx *ctx,
      const valkey_search::data_model::IndexSchema &index_schema_proto,
      std::unique_ptr<valkey_search::AttributeDataType> attribute_data_type);
};

// ---------------------------------------------------------------------------
// FuzzSearchParameters - concrete SearchParameters for fuzzing,
// modeled after UnitTestSearchParameters in testing/common.h.
// ---------------------------------------------------------------------------
class FuzzSearchParameters : public valkey_search::query::SearchParameters {
 public:
  FuzzSearchParameters() {
    timeout_ms = 10000;
    db_num = 0;
  }

  void QueryCompleteBackground(std::unique_ptr<SearchParameters>) override {}
  void QueryCompleteMainThread(std::unique_ptr<SearchParameters>) override {}
};

// ---------------------------------------------------------------------------
// Global state accessible to all fuzz targets
// ---------------------------------------------------------------------------
extern ValkeyModuleCtx g_fake_ctx;
extern std::shared_ptr<FuzzIndexSchema> g_index_schema;

// Initialize Valkey module API function pointer stubs.
void InitValkeyModuleStubs();

// Full environment initialization: stubs, singletons, and index schema.
// Safe to call multiple times (only initializes once).
void InitFuzzEnvironment();

#endif  // FUZZING_COMMON_FUZZ_COMMON_H_
