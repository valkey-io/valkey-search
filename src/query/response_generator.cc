/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/response_generator.h"

#include <algorithm>
#include <deque>
#include <optional>
#include <string>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/attribute_data_type.h"
#include "src/indexes/tag.h"
#include "src/indexes/vector_base.h"
#include "src/metrics.h"
#include "src/query/predicate.h"
#include "src/query/search.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::options {

constexpr absl::string_view kMaxSearchResultRecordSizeConfig{
    "max-search-result-record-size"};
constexpr int kMaxSearchResultRecordSize{10 * 1024 * 1024};  // 10MB
constexpr int kMinSearchResultRecordSize{100};
constexpr absl::string_view kMaxSearchResultFieldsCountConfig{
    "max-search-result-record-size"};
constexpr int kMaxSearchResultFieldsCount{1000};

/// Register the "--max-search-result-record-size" flag. Controls the max
/// content size for a record in the search response
static auto max_search_result_record_size =
    vmsdk::config::NumberBuilder(
        kMaxSearchResultRecordSizeConfig,  // name
        kMaxSearchResultRecordSize / 2,    // default size
        kMinSearchResultRecordSize,        // min size
        kMaxSearchResultRecordSize)        // max size
        .WithValidationCallback(CHECK_RANGE(kMinSearchResultRecordSize,
                                            kMaxSearchResultRecordSize,
                                            kMaxSearchResultRecordSizeConfig))
        .Build();

/// Register the "--max-search-result-fields-count" flag. Controls the max
/// number of fields in the content of the search response
static auto max_search_result_fields_count =
    vmsdk::config::NumberBuilder(
        kMaxSearchResultFieldsCountConfig,  // name
        kMaxSearchResultFieldsCount / 2,    // default size
        1,                                  // min size
        kMaxSearchResultFieldsCount)        // max size
        .WithValidationCallback(CHECK_RANGE(1, kMaxSearchResultFieldsCount,
                                            kMaxSearchResultFieldsCountConfig))
        .Build();

vmsdk::config::Number &GetMaxSearchResultRecordSize() {
  return dynamic_cast<vmsdk::config::Number &>(*max_search_result_record_size);
}
vmsdk::config::Number &GetMaxSearchResultFieldsCount() {
  return dynamic_cast<vmsdk::config::Number &>(*max_search_result_fields_count);
}

}  // namespace valkey_search::options

namespace valkey_search::query {

class PredicateEvaluator : public query::Evaluator {
 public:
  explicit PredicateEvaluator(const RecordsMap &records) : records_(records) {}
  bool EvaluateTags(const query::TagPredicate &predicate) override {
    auto identifier = predicate.GetRetainedIdentifier();
    auto it = records_.find(vmsdk::ToStringView(identifier.get()));
    if (it == records_.end()) {
      return false;
    }
    auto index = predicate.GetIndex();
    // Parsing RECORD DATA: Field value from database key for post-query
    // verification. Uses schema-defined separator since this is record data,
    // not query syntax.
    auto tags = indexes::Tag::ParseSearchTags(
        vmsdk::ToStringView(it->second.value.get()), index->GetSeparator());
    if (!tags.ok()) {
      return false;
    }
    return predicate.Evaluate(&tags.value(), index->IsCaseSensitive());
  }

  bool EvaluateNumeric(const query::NumericPredicate &predicate) override {
    auto identifier = predicate.GetRetainedIdentifier();
    auto it = records_.find(vmsdk::ToStringView(identifier.get()));
    if (it == records_.end()) {
      return false;
    }
    auto out_numeric =
        vmsdk::To<double>(vmsdk::ToStringView(it->second.value.get()));
    if (!out_numeric.ok()) {
      return false;
    }
    return predicate.Evaluate(&out_numeric.value());
  }

 private:
  const RecordsMap &records_;
};

bool VerifyFilter(const query::Predicate *predicate,
                  const RecordsMap &records) {
  if (predicate == nullptr) {
    return true;
  }
  PredicateEvaluator evaluator(records);
  return predicate->Evaluate(evaluator);
}

absl::StatusOr<RecordsMap> GetContentNoReturnJson(
    ValkeyModuleCtx *ctx, const AttributeDataType &attribute_data_type,
    const query::SearchParameters &parameters, absl::string_view key,
    const std::string &vector_identifier) {
  absl::flat_hash_set<absl::string_view> identifiers;
  identifiers.insert(kJsonRootElementQuery);
  for (const auto &filter_identifier :
       parameters.filter_parse_results.filter_identifiers) {
    identifiers.insert(filter_identifier);
  }
  vmsdk::ValkeySelectDbGuard select_db_guard(ctx, parameters.db_num);
  auto key_str = vmsdk::MakeUniqueValkeyString(key);
  auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
      ctx, key_str.get(), VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ);
  VMSDK_ASSIGN_OR_RETURN(auto content, attribute_data_type.FetchAllRecords(
                                           ctx, vector_identifier,
                                           key_obj.get(), key, identifiers));
  if (parameters.filter_parse_results.filter_identifiers.empty()) {
    return content;
  }
  if (!VerifyFilter(parameters.filter_parse_results.root_predicate.get(),
                    content)) {
    return absl::NotFoundError("Verify filter failed");
  }
  RecordsMap return_content;
  static const vmsdk::UniqueValkeyString kJsonRootElementQueryPtr =
      vmsdk::MakeUniqueValkeyString(kJsonRootElementQuery);
  return_content.emplace(
      kJsonRootElementQuery,
      RecordsMapValue(
          kJsonRootElementQueryPtr.get(),
          std::move(content.find(kJsonRootElementQuery)->second.value)));
  return return_content;
}

absl::StatusOr<RecordsMap> GetContent(
    ValkeyModuleCtx *ctx, const AttributeDataType &attribute_data_type,
    const query::SearchParameters &parameters, absl::string_view key,
    const std::string &vector_identifier) {
  if (attribute_data_type.ToProto() ==
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON &&
      parameters.return_attributes.empty()) {
    return GetContentNoReturnJson(ctx, attribute_data_type, parameters, key,
                                  vector_identifier);
  }
  absl::flat_hash_set<absl::string_view> identifiers;
  for (const auto &return_attribute : parameters.return_attributes) {
    identifiers.insert(vmsdk::ToStringView(return_attribute.identifier.get()));
  }
  if (!parameters.return_attributes.empty()) {
    for (const auto &filter_identifier :
         parameters.filter_parse_results.filter_identifiers) {
      identifiers.insert(filter_identifier);
    }
  }
  vmsdk::ValkeySelectDbGuard select_db_guard(ctx, parameters.db_num);
  auto key_str = vmsdk::MakeUniqueValkeyString(key);
  auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
      ctx, key_str.get(), VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ);
  VMSDK_ASSIGN_OR_RETURN(auto content, attribute_data_type.FetchAllRecords(
                                           ctx, vector_identifier,
                                           key_obj.get(), key, identifiers));
  if (parameters.filter_parse_results.filter_identifiers.empty()) {
    return content;
  }
  if (!VerifyFilter(parameters.filter_parse_results.root_predicate.get(),
                    content)) {
    return absl::NotFoundError("Verify filter failed");
  }
  if (parameters.return_attributes.empty()) {
    return content;
  }
  RecordsMap return_content;
  for (auto &return_attribute : parameters.return_attributes) {
    auto itr =
        content.find(vmsdk::ToStringView(return_attribute.identifier.get()));
    if (itr == content.end()) {
      continue;
    }
    return_content.emplace(
        vmsdk::ToStringView(return_attribute.identifier.get()),
        RecordsMapValue(
            return_attribute.identifier.get(),
            vmsdk::RetainUniqueValkeyString(itr->second.value.get())));
  }
  return return_content;
}

// Adds all local content for neighbors to the list of neighbors.
// This function is meant to be used for non-vector queries.
void ProcessNonVectorNeighborsForReply(
    ValkeyModuleCtx *ctx, const AttributeDataType &attribute_data_type,
    std::deque<indexes::Neighbor> &neighbors,
    const query::SearchParameters &parameters) {
  ProcessNeighborsForReply(ctx, attribute_data_type, neighbors, parameters, "");
}

// Adds all local content for neighbors to the list of neighbors.
//
// Any neighbors already contained in the attribute content map will be skipped.
// Any data not found locally will be skipped.
void ProcessNeighborsForReply(ValkeyModuleCtx *ctx,
                              const AttributeDataType &attribute_data_type,
                              std::deque<indexes::Neighbor> &neighbors,
                              const query::SearchParameters &parameters,
                              const std::string &identifier) {
  const auto max_content_size =
      options::GetMaxSearchResultRecordSize().GetValue();
  const auto max_content_fields =
      options::GetMaxSearchResultFieldsCount().GetValue();
  for (auto &neighbor : neighbors) {
    // neighbors which were added from remote nodes already have attribute
    // content
    if (neighbor.attribute_contents.has_value()) {
      continue;
    }
    auto content = GetContent(ctx, attribute_data_type, parameters,
                              *neighbor.external_id, identifier);
    if (!content.ok()) {
      continue;
    }

    // Check content size before assigning

    size_t total_size = 0;
    bool size_exceeded = false;
    if (content.value().size() > max_content_fields) {
      ++Metrics::GetStats().query_result_record_dropped_cnt;
      VMSDK_LOG(WARNING, ctx)
          << "Content field number exceeds configured limit of "
          << max_content_fields
          << " for neighbor with ID: " << (*neighbor.external_id).Str();
      continue;
    }

    for (const auto &item : content.value()) {
      total_size += item.first.size();
      total_size += vmsdk::ToStringView(item.second.value.get()).size();
      if (total_size > max_content_size) {
        ++Metrics::GetStats().query_result_record_dropped_cnt;
        VMSDK_LOG(WARNING, ctx)
            << "Content size exceeds configured limit of " << max_content_size
            << " bytes for neighbor with ID: " << (*neighbor.external_id).Str();
        size_exceeded = true;
        break;
      }
    }

    // Only assign content if it's within size limit
    if (!size_exceeded) {
      neighbor.attribute_contents = std::move(content.value());
    }
  }
  // Remove all entries that don't have content now.
  // TODO: incorporate a retry in case of removal.
  neighbors.erase(
      std::remove_if(neighbors.begin(), neighbors.end(),
                     [](const indexes::Neighbor &neighbor) {
                       return !neighbor.attribute_contents.has_value();
                     }),
      neighbors.end());
}

}  // namespace valkey_search::query
