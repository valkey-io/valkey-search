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
#include "absl/synchronization/mutex.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

class Attribute {
 public:
  Attribute(absl::string_view alias, absl::string_view identifier,
            std::shared_ptr<indexes::IndexBase> index)
      : alias_(alias),
        identifier_(identifier),
        index_(index),
        score_as_mutex_(std::make_unique<absl::Mutex>()) {}
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
  inline int RespondWithInfo(ValkeyModuleCtx* ctx) const {
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_LEN);
    ValkeyModule_ReplyWithSimpleString(ctx, "identifier");
    ValkeyModule_ReplyWithSimpleString(ctx, GetIdentifier().c_str());
    ValkeyModule_ReplyWithSimpleString(ctx, "attribute");
    ValkeyModule_ReplyWithSimpleString(ctx, GetAlias().c_str());
    int added_fields = index_->RespondWithInfo(ctx);
    ValkeyModule_ReplySetArrayLength(ctx, added_fields + 4);
    return 1;
  }

  inline vmsdk::UniqueValkeyString DefaultReplyScoreAs() const {
    if (!cached_score_as_) {
      absl::MutexLock lock(score_as_mutex_.get());
      if (!cached_score_as_) {
        cached_score_as_ =
            vmsdk::MakeUniqueValkeyString(absl::StrCat("__", alias_, "_score"));
      }
    }
    return vmsdk::RetainUniqueValkeyString(cached_score_as_.get());
  }

 private:
  std::string alias_;
  std::string identifier_;
  std::shared_ptr<indexes::IndexBase> index_;
  // Mutex for thread-safe lazy initialization of cached_score_as_
  mutable std::unique_ptr<absl::Mutex> score_as_mutex_;
  mutable vmsdk::UniqueValkeyString cached_score_as_;
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_ATTRIBUTE_H_
