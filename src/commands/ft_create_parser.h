/*
 * Copyright (c) 2025, valkey-search contributors
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

#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_CREATE_PARSER_H_
#define VALKEYSEARCH_SRC_COMMANDS_FT_CREATE_PARSER_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

struct FTCreateTagParameters {
  absl::string_view separator{","};
  bool case_sensitive{false};
};

constexpr int kDefaultInitialCap{10 * 1024};

struct FTCreateVectorParameters {
  std::optional<int> dimensions;
  data_model::DistanceMetric distance_metric{
      data_model::DistanceMetric::DISTANCE_METRIC_UNSPECIFIED};
  data_model::VectorDataType vector_data_type{
      data_model::VectorDataType::VECTOR_DATA_TYPE_UNSPECIFIED};
  int initial_cap{kDefaultInitialCap};
  absl::Status Verify() const;
  std::unique_ptr<data_model::VectorIndex> ToProto() const;
};

constexpr int kDefaultBlockSize{1024};
constexpr int kDefaultM{16};
constexpr int kDefaultEFConstruction{200};
constexpr int kDefaultEFRuntime{10};

struct HNSWParameters : public FTCreateVectorParameters {
  // Tightly connected with internal dimensionality of the data
  // strongly affects the memory consumption
  int m{kDefaultM};
  int ef_construction{kDefaultEFConstruction};
  size_t ef_runtime{kDefaultEFRuntime};
  absl::Status Verify() const;
  std::unique_ptr<data_model::VectorIndex> ToProto() const;
};

struct FlatParameters : public FTCreateVectorParameters {
  // Block size holds the amount of vectors in a contiguous array. This is
  // useful when the index is dynamic with respect to addition and deletion.
  uint32_t block_size{kDefaultBlockSize};
  std::unique_ptr<data_model::VectorIndex> ToProto() const;
};

absl::StatusOr<data_model::IndexSchema> ParseFTCreateArgs(
    ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc);
}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_FT_CREATE_PARSER_H_
