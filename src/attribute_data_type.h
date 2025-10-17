/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_ATTRIBUTE_DATA_TYPE_H_
#define VALKEYSEARCH_SRC_ATTRIBUTE_DATA_TYPE_H_
#include <string>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/variant.h"
#include "src/index_schema.pb.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

bool HashHasRecord(ValkeyModuleKey *key, absl::string_view identifier);

class RecordsMapValue {
 public:
  RecordsMapValue(vmsdk::UniqueValkeyString identifier,
                  vmsdk::UniqueValkeyString value)
      : value(std::move(value)), identifier_(std::move(identifier)) {}
  RecordsMapValue(ValkeyModuleString *identifier,
                  vmsdk::UniqueValkeyString value)
      : value(std::move(value)), identifier_(identifier) {}
  vmsdk::UniqueValkeyString value;
  ValkeyModuleString *GetIdentifier() const {
    if (absl::holds_alternative<vmsdk::UniqueValkeyString>(identifier_)) {
      return absl::get<vmsdk::UniqueValkeyString>(identifier_).get();
    }
    return absl::get<ValkeyModuleString *>(identifier_);
  }
  friend std::ostream &operator<<(std::ostream &os,
                                  const RecordsMapValue &value) {
    return os << vmsdk::ToStringView(value.GetIdentifier()) << ":'"
              << vmsdk::ToStringView(value.value.get()) << "'";
  }

 private:
  absl::variant<ValkeyModuleString *, vmsdk::UniqueValkeyString> identifier_;
};

using RecordsMap = absl::flat_hash_map<absl::string_view, RecordsMapValue>;

inline std::ostream &operator<<(std::ostream &os, const RecordsMap &map) {
  for (const auto &[name, value] : map) {
    os << name << "=>" << value << ",";
  }
  return os;
}

class AttributeDataType {
 public:
  virtual ~AttributeDataType() = default;
  virtual absl::StatusOr<vmsdk::UniqueValkeyString> GetRecord(
      ValkeyModuleCtx *ctx, ValkeyModuleKey *open_key, absl::string_view key,
      absl::string_view identifier) const = 0;
  virtual int GetValkeyEventTypes() const {
    return VALKEYMODULE_NOTIFY_GENERIC | VALKEYMODULE_NOTIFY_EXPIRED |
           VALKEYMODULE_NOTIFY_EVICTED;
  };
  virtual absl::StatusOr<RecordsMap> FetchAllRecords(
      ValkeyModuleCtx *ctx, const std::string &vector_identifier,
      ValkeyModuleKey *open_key, absl::string_view key,
      const absl::flat_hash_set<absl::string_view> &identifiers) const = 0;
  virtual data_model::AttributeDataType ToProto() const = 0;
  virtual std::string ToString() const = 0;
  virtual bool IsProperType(ValkeyModuleKey *key) const = 0;
  // This provides indication whether the fetched content need special
  // normalization.
  virtual bool RecordsProvidedAsString() const = 0;
};

class HashAttributeDataType : public AttributeDataType {
 public:
  absl::StatusOr<vmsdk::UniqueValkeyString> GetRecord(
      ValkeyModuleCtx *ctx, ValkeyModuleKey *open_key, absl::string_view key,
      absl::string_view identifier) const override;
  inline int GetValkeyEventTypes() const override {
    return VALKEYMODULE_NOTIFY_HASH | AttributeDataType::GetValkeyEventTypes();
  }

  inline data_model::AttributeDataType ToProto() const override {
    return data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH;
  }
  inline std::string ToString() const override { return "HASH"; }
  absl::StatusOr<RecordsMap> FetchAllRecords(
      ValkeyModuleCtx *ctx, const std::string &vector_identifier,
      ValkeyModuleKey *open_key, absl::string_view key,
      const absl::flat_hash_set<absl::string_view> &identifiers) const override;
  bool IsProperType(ValkeyModuleKey *key) const override {
    return ValkeyModule_KeyType(key) == VALKEYMODULE_KEYTYPE_HASH;
  }
  bool RecordsProvidedAsString() const override { return false; }
};

inline constexpr absl::string_view kJsonCmd = "JSON.GET";
inline constexpr absl::string_view kJsonRootElementQuery = "$";

class JsonAttributeDataType : public AttributeDataType {
 public:
  absl::StatusOr<vmsdk::UniqueValkeyString> GetRecord(
      ValkeyModuleCtx *ctx, ValkeyModuleKey *open_key, absl::string_view key,
      absl::string_view identifier) const override;
  inline int GetValkeyEventTypes() const override {
    return VALKEYMODULE_NOTIFY_MODULE |
           AttributeDataType::GetValkeyEventTypes();
  }
  inline data_model::AttributeDataType ToProto() const override {
    return data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON;
  }
  inline std::string ToString() const override { return "JSON"; }
  absl::StatusOr<RecordsMap> FetchAllRecords(
      ValkeyModuleCtx *ctx, const std::string &vector_identifier,
      ValkeyModuleKey *open_key, absl::string_view key,
      const absl::flat_hash_set<absl::string_view> &identifiers) const override;
  bool IsProperType(ValkeyModuleKey *key) const override {
    return ValkeyModule_KeyType(key) == VALKEYMODULE_KEYTYPE_MODULE;
  }
  bool RecordsProvidedAsString() const override { return true; }
};

bool IsJsonModuleSupported(ValkeyModuleCtx *ctx);
using JsonSharedAPIGetValueFn = int (*)(ValkeyModuleKey *key, const char *path,
                                        ValkeyModuleString **result);
// Used just for testing
void ResetJsonLoadedCache();
}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_ATTRIBUTE_DATA_TYPE_H_
