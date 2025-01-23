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

#ifndef VALKEYSEARCH_SRC_COMMANDS_FILTER_PARSER_H_
#define VALKEYSEARCH_SRC_COMMANDS_FILTER_PARSER_H_
#include <cstddef>
#include <memory>
#include <string>

#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.h"
#include "src/indexes/tag.h"
#include "src/query/predicate.h"

namespace valkey_search {
namespace indexes {
class Tag;
}  // namespace indexes
struct FilterParseResults {
  std::unique_ptr<query::Predicate> root_predicate;
  absl::flat_hash_set<std::string> filter_identifiers;
};
class FilterParser {
 public:
  FilterParser(const IndexSchema& index_schema, absl::string_view expression);

  absl::StatusOr<FilterParseResults> Parse();

 private:
  const IndexSchema& index_schema_;
  absl::string_view expression_;
  size_t pos_{0};
  absl::flat_hash_set<std::string> filter_identifiers_;

  absl::StatusOr<bool> IsMatchAllExpression();
  absl::StatusOr<std::unique_ptr<query::Predicate>> ParseExpression();
  absl::StatusOr<std::unique_ptr<query::NumericPredicate>>
  ParseNumericPredicate(const std::string& attribute_alias);
  absl::StatusOr<std::unique_ptr<query::TagPredicate>> ParseTagPredicate(
      const std::string& attribute_alias);
  void SkipWhitespace();

  char Peek() const { return expression_[pos_]; }

  bool IsEnd() const { return pos_ >= expression_.length(); }
  bool Match(char expected, bool skip_whitespace = true);
  bool MatchInsensitive(const std::string& expected);
  absl::StatusOr<std::string> ParseFieldName();

  absl::StatusOr<double> ParseNumber();

  absl::StatusOr<absl::string_view> ParseTagString();

  absl::StatusOr<absl::flat_hash_set<absl::string_view>> ParseTags(
      absl::string_view tag_string, indexes::Tag* tag_index) const;
};

}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_FILTER_PARSER_H_
