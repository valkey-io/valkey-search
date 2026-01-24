/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/index_schema.h"
#include "src/schema_manager.h"

namespace valkey_search {

static absl::Status DumpKey(ValkeyModuleCtx* ctx, auto& ki,
                            bool with_positions) {
  if (with_positions) {
    auto pi = ki.GetPositionIterator();
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    ValkeyModule_ReplyWithStringBuffer(ctx, ki.GetKey()->Str().data(),
                                       ki.GetKey()->Str().size());
    size_t count = 1;
    while (pi.IsValid()) {
      ValkeyModule_ReplyWithLongLong(ctx, pi.GetPosition());
      ValkeyModule_ReplyWithLongLong(ctx, pi.GetFieldMask());
      pi.NextPosition();
      count += 2;
    }
    ValkeyModule_ReplySetArrayLength(ctx, count);
  } else {
    ValkeyModule_ReplyWithStringBuffer(ctx, ki.GetKey()->Str().data(),
                                       ki.GetKey()->Str().size());
  }
  return absl::OkStatus();
}

static absl::Status DumpWord(ValkeyModuleCtx* ctx, auto& wi, bool with_keys,
                             bool with_positions) {
  if (with_keys) {
    auto ki = wi.GetTarget()->GetKeyIterator();
    auto key_count = wi.GetTarget()->GetKeyCount();
    ValkeyModule_ReplyWithArray(ctx, 1 + key_count);
    ValkeyModule_ReplyWithStringBuffer(ctx, wi.GetWord().data(),
                                       wi.GetWord().size());
    size_t count = 0;
    while (ki.IsValid()) {
      VMSDK_RETURN_IF_ERROR(DumpKey(ctx, ki, with_positions));
      ki.NextKey();
      count++;
    }
    if (count != key_count) {
      return absl::InvalidArgumentError(absl::StrCat(
          "Key count mismatch for word: ", wi.GetWord(), " Counted:", count,
          " Expected: ", wi.GetTarget()->GetKeyCount()));
    }
  } else {
    ValkeyModule_ReplyWithStringBuffer(ctx, wi.GetWord().data(),
                                       wi.GetWord().size());
  }
  return absl::OkStatus();
}

static absl::Status DumpWordIterator(ValkeyModuleCtx* ctx, auto& wi,
                                     bool with_keys, bool with_positions) {
  ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
  size_t count = 0;
  while (!wi.Done()) {
    count++;
    VMSDK_RETURN_IF_ERROR(DumpWord(ctx, wi, with_keys, with_positions));
    wi.Next();
  }
  ValkeyModule_ReplySetArrayLength(ctx, count);
  return absl::OkStatus();
}

/*
FT._DEBUG TEXTINFO <index_name> PREFIX <word> [WITHKEYS [WITHPOSITIONS]]
FT._DEBUG TEXTINFO <index_name> SUFFIX <word> [WITHKEYS [WITHPOSITIONS]]
FT._DEBUG TEXTINFO <index_name> STEM <word>
FT._DEBUG TEXTINFO <index_name> LEXER <string> [<stemsize>]

*/
absl::Status IndexSchema::TextInfoCmd(ValkeyModuleCtx* ctx,
                                      vmsdk::ArgsIterator& itr) {
  std::string index_name;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, index_name));
  VMSDK_ASSIGN_OR_RETURN(auto index_schema,
                         SchemaManager::Instance().GetIndexSchema(
                             ValkeyModule_GetSelectedDb(ctx), index_name));
  std::string subcommand;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, subcommand));
  vmsdk::ReaderMutexLock lock(&index_schema->time_sliced_mutex_);
  subcommand = absl::AsciiStrToUpper(subcommand);
  if (subcommand == "PREFIX") {
    std::string word;
    VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, word));
    auto wi = index_schema->GetTextIndexSchema()
                  ->GetTextIndex()
                  ->GetPrefix()
                  .GetWordIterator(word);
    bool with_keys = itr.PopIfNextIgnoreCase("WITHKEYS");
    bool with_positions = itr.PopIfNextIgnoreCase("WITHPOSITIONS");
    return DumpWordIterator(ctx, wi, with_keys, with_positions);
  } else if (subcommand == "SUFFIX") {
    std::string word;
    VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, word));
    auto suffix =
        index_schema->GetTextIndexSchema()->GetTextIndex()->GetSuffix();
    if (!suffix) {
      return absl::InvalidArgumentError("Suffix is not enabled");
    }
    auto wi = suffix->get().GetWordIterator(word);
    bool with_keys = itr.PopIfNextIgnoreCase("WITHKEYS");
    bool with_positions = itr.PopIfNextIgnoreCase("WITHPOSITIONS");
    return DumpWordIterator(ctx, wi, with_keys, with_positions);
  } else if (subcommand == "STEM") {
    std::string word;
    VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, word));

    // Access stem tree directly with proper thread safety
    // GetStemTree() returns a const reference, so we need to lock access
    const auto& stem_tree = index_schema->GetTextIndexSchema()->GetStemTree();
    auto stem_wi = stem_tree.GetWordIterator(word);

    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    size_t count = 0;
    while (!stem_wi.Done()) {
      count++;
      // Reply with stem word and its parent words
      ValkeyModule_ReplyWithArray(ctx, 2);
      ValkeyModule_ReplyWithStringBuffer(ctx, stem_wi.GetWord().data(),
                                         stem_wi.GetWord().size());

      // Reply with parent words set
      const auto& stem_parents_optional = stem_wi.GetTarget();
      if (stem_parents_optional.has_value()) {
        const auto& parents = stem_parents_optional.value();
        ValkeyModule_ReplyWithArray(ctx, parents.size());
        for (const auto& parent : parents) {
          ValkeyModule_ReplyWithStringBuffer(ctx, parent.data(), parent.size());
        }
      } else {
        ValkeyModule_ReplyWithArray(ctx, 0);
      }
      stem_wi.Next();
    }
    ValkeyModule_ReplySetArrayLength(ctx, count);
  } else if (subcommand == "LEXER") {
    std::string text;
    VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, text));
    auto lexer = index_schema->GetTextIndexSchema()->GetLexer();
    VMSDK_ASSIGN_OR_RETURN(auto result, lexer.Tokenize(text, false, 0));
    ValkeyModule_ReplyWithArray(ctx, result.size());
    for (auto& token : result) {
      ValkeyModule_ReplyWithStringBuffer(ctx, token.data(), token.size());
    }
  } else {
    return absl::InvalidArgumentError(
        absl::StrCat("Unknown subcommand ", subcommand));
  }
  return absl::OkStatus();
}
}  // namespace valkey_search
