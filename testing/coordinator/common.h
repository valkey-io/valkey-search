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
};

class MockServer : public Server {
 public:
  MOCK_METHOD(uint16_t, GetPort, (), (const, override));
};

}  // namespace valkey_search::coordinator

#endif  // VALKEYSEARCH_TESTING_COORDINATOR_COMMON_H_
