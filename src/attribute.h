/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_ATTRIBUTE_H_
#define VALKEYSEARCH_SRC_ATTRIBUTE_H_

#include <memory>
#include <string>

#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

class IndexSchema;
class Attribute {
 public:
  Attribute(absl::string_view alias, absl::string_view identifier,
            std::shared_ptr<indexes::IndexBase> index, uint16_t pos)
      : alias_(alias), identifier_(identifier), index_(index), position_(pos) {}
  inline const std::string& GetAlias() const { return alias_; }
  inline const std::string& GetIdentifier() const { return identifier_; }
  std::shared_ptr<indexes::IndexBase> GetIndex() const { return index_; }
  std::unique_ptr<data_model::Attribute> ToProto() const {
    auto attribute_proto = std::make_unique<data_model::Attribute>();
    attribute_proto->set_alias(alias_);
    attribute_proto->set_identifier(identifier_);
    attribute_proto->set_allocated_index(index_->ToProto().release());
    return attribute_proto;
  }

  int RespondWithInfo(ValkeyModuleCtx* ctx,
                      const IndexSchema* index_schema) const;

  // Creates a new score-as string for each call.
  // We intentionally avoid caching because ValkeyModule_RetainString uses
  // non-atomic refcount increment (o->refcount++), causing race conditions
  // when multiple threads call it on the same ValkeyModuleString.
  inline vmsdk::UniqueValkeyString DefaultReplyScoreAs() const {
    return vmsdk::MakeUniqueValkeyString(absl::StrCat("__", alias_, "_score"));
  }

  // Return the attribute position during creation of the index.
  inline uint16_t GetPosition() const { return position_; }

 private:
  std::string alias_;
  std::string identifier_;
  std::shared_ptr<indexes::IndexBase> index_;
  uint16_t position_{UINT16_MAX};  // The attribute position during creation.
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_ATTRIBUTE_H_
