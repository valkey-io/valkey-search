/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_TESTING_COORDINATOR_COMMON_H_
#define VALKEYSEARCH_TESTING_COORDINATOR_COMMON_H_

#include <cstdint>
#include <memory>

#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "src/coordinator/client.h"
#include "src/coordinator/client_pool.h"
#include "src/coordinator/server.h"

namespace valkey_search::coordinator {

class MockClientPool : public ClientPool {
 public:
  MockClientPool() : ClientPool(nullptr) {}
  MOCK_METHOD(std::shared_ptr<Client>, GetClient, (absl::string_view address),
              (override));
};

class MockClient : public Client {
 public:
  MOCK_METHOD(void, GetGlobalMetadata, (GetGlobalMetadataCallback done),
              (override));
  MOCK_METHOD(void, SearchIndexPartition,
              (std::unique_ptr<SearchIndexPartitionRequest> request,
               SearchIndexPartitionCallback done),
              (override));
  MOCK_METHOD(void, InfoIndexPartition,
              (std::unique_ptr<InfoIndexPartitionRequest> request,
               InfoIndexPartitionCallback done,
               int timeout_ms),
              (override));
};

class MockServer : public Server {
 public:
  MOCK_METHOD(uint16_t, GetPort, (), (const, override));
};

}  // namespace valkey_search::coordinator

#endif  // VALKEYSEARCH_TESTING_COORDINATOR_COMMON_H_
