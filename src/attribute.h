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

class Attribute {
 public:
  Attribute(absl::string_view alias, absl::string_view identifier,
            std::shared_ptr<indexes::IndexBase> index)
      : alias_(alias), identifier_(identifier), index_(index) {}
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
  inline int RespondWithInfo(RedisModuleCtx* ctx) const {
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_LEN);
    RedisModule_ReplyWithSimpleString(ctx, "identifier");
    RedisModule_ReplyWithSimpleString(ctx, GetIdentifier().c_str());
    RedisModule_ReplyWithSimpleString(ctx, "attribute");
    RedisModule_ReplyWithSimpleString(ctx, GetAlias().c_str());
    int added_fields = index_->RespondWithInfo(ctx);
    RedisModule_ReplySetArrayLength(ctx, added_fields + 4);
    return 1;
  }

  inline vmsdk::UniqueRedisString DefaultReplyScoreAs() const {
    if (!cached_score_as_) {
      cached_score_as_ =
          vmsdk::MakeUniqueRedisString(absl::StrCat("__", alias_, "_score"));
    }
    return vmsdk::RetainUniqueRedisString(cached_score_as_.get());
  }

 private:
  std::string alias_;
  std::string identifier_;
  std::shared_ptr<indexes::IndexBase> index_;
  // Maintaining a cached version
  mutable vmsdk::UniqueRedisString cached_score_as_;
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_ATTRIBUTE_H_
