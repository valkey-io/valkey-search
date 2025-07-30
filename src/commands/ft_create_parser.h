/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_CREATE_PARSER_H_
#define VALKEYSEARCH_SRC_COMMANDS_FT_CREATE_PARSER_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "vmsdk/src/module_config.h"
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

// Global text parameters (per-index) - populated in IndexSchema
struct SchemaGlobTextParams {
  std::string punctuation{",.<>{}[]\"':;!@#$%^&*()-+=~/\\\\"};
  bool with_offsets{true};
  bool no_stem{false};
  std::vector<std::string> stop_words;
  data_model::Language language{data_model::LANGUAGE_ENGLISH};
  int min_stem_size{4};
};

// Field-specific text parameters (per text field) - populated in TextIndex
struct FTCreateTextParameters {
  bool with_suffix_trie{false};
  bool no_stem{false};  // Can be overridden per field
  int min_stem_size{4};
};


constexpr int kDefaultBlockSize{1024};
constexpr int kDefaultM{16};
constexpr int kDefaultEFConstruction{200};
constexpr int kDefaultEFRuntime{10};

namespace options {

/// Return the maximum number of prefixes allowed per index.
vmsdk::config::Number& GetMaxPrefixes();

/// Return the maximum length of a tag field.
vmsdk::config::Number& GetMaxTagFieldLen();

/// Return the maximum length of a numeric field.
vmsdk::config::Number& GetMaxNumericFieldLen();

/// Return the maximum number of attributes allowed per index.
vmsdk::config::Number& GetMaxAttributes();

/// Return the maximum number of dimensions allowed for vector indices.
vmsdk::config::Number& GetMaxDimensions();

/// Return the maximum M parameter value allowed for HNSW algorithm.
vmsdk::config::Number& GetMaxM();

/// Return the maximum EF construction parameter value allowed for HNSW
/// algorithm.
vmsdk::config::Number& GetMaxEfConstruction();

/// Return the maximum EF runtime parameter value allowed for HNSW algorithm.
vmsdk::config::Number& GetMaxEfRuntime();

/// Return the current default timeout in milliseconds for FT.CREATE
vmsdk::config::Number& GetDefaultTimeoutMs();

}  // namespace options

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
    ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc);
}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_FT_CREATE_PARSER_H_
