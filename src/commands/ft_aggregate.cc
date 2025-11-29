/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include <ranges>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "ft_search_parser.h"
#include "src/commands/commands.h"
#include "src/commands/ft_aggregate_exec.h"
#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/metrics.h"
#include "src/query/response_generator.h"

namespace valkey_search {
namespace aggregate {

struct RealIndexInterface : public IndexInterface {
  std::shared_ptr<IndexSchema> schema_;
  absl::StatusOr<indexes::IndexerType> GetFieldType(
      absl::string_view s) const override {
    VMSDK_ASSIGN_OR_RETURN(auto indexer, schema_->GetIndex(s));
    return indexer->GetIndexerType();
  }
  absl::StatusOr<std::string> GetIdentifier(
      absl::string_view alias) const override {
    return schema_->GetIdentifier(alias);
  }
  absl::StatusOr<std::string> GetAlias(
      absl::string_view identifier) const override {
    return schema_->GetAlias(identifier);
  }
  RealIndexInterface(std::shared_ptr<IndexSchema> schema) : schema_(schema) {}
};

absl::Status ManipulateReturnsClause(AggregateParameters &params) {
  // Figure out what fields actually need to be returned by the aggregation
  // operation. And modify the common search returns list accordingly
  CHECK(!params.no_content);
  if (params.loadall_) {
    CHECK(params.return_attributes.empty());
  } else if (params.loads_.empty()) {
    // Nothing, don't load anything
    params.no_content = true;
  } else {
    for (const auto &load : params.loads_) {
      //
      // Skip loading of the score and the key, we always get those...
      //
      if (load == "__key") {
        params.load_key = true;
        continue;
      }
      if (load == vmsdk::ToStringView(params.score_as.get())) {
        continue;
      }
      VMSDK_ASSIGN_OR_RETURN(auto indexer, params.index_schema->GetIndex(load));
      auto indexer_type = indexer->GetIndexerType();
      auto schema_identifier = params.index_schema->GetIdentifier(load);
      if (schema_identifier.ok()) {
        params.return_attributes.emplace_back(query::ReturnAttribute{
            .identifier = vmsdk::MakeUniqueValkeyString(*schema_identifier),
            .attribute_alias = vmsdk::MakeUniqueValkeyString(load),
            .alias = vmsdk::MakeUniqueValkeyString(load)});
        params.AddRecordAttribute(*schema_identifier, load, indexer_type);
      } else {
        params.return_attributes.emplace_back(query::ReturnAttribute{
            .identifier = vmsdk::MakeUniqueValkeyString(load),
            .attribute_alias = vmsdk::UniqueValkeyString(),
            .alias = vmsdk::MakeUniqueValkeyString(load)});
        params.AddRecordAttribute(load, load, indexes::IndexerType::kNone);
      }
    }
  }
  return absl::OkStatus();
}

absl::Status AggregateParameters::ParseCommand(vmsdk::ArgsIterator &itr) {
  static vmsdk::KeyValueParser<AggregateParameters> parser =
      CreateAggregateParser();
  RealIndexInterface real_index_interface(index_schema);
  parse_vars_.index_interface_ = &real_index_interface;

  VMSDK_RETURN_IF_ERROR(PreParseQueryString(*this));
  // Ensure that key is first value if it gets included...
  CHECK(AddRecordAttribute("__key", "__key", indexes::IndexerType::kNone) == 0);
  auto score_sv = vmsdk::ToStringView(score_as.get());
  CHECK(AddRecordAttribute(score_sv, score_sv, indexes::IndexerType::kNone) ==
        1);

  VMSDK_RETURN_IF_ERROR(parser.Parse(*this, itr, true));
  if (itr.DistanceEnd() > 0) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unexpected parameter at position ", (itr.Position() + 1),
                     ":", vmsdk::ToStringView(itr.Get().value())));
  }

  if (dialect < 2 || dialect > 4) {
    return absl::InvalidArgumentError("Only Dialects 2, 3 and 4 are supported");
  }

  limit.number = std::numeric_limits<uint64_t>::max();  // Override default of
                                                        // 10 from search

  VMSDK_RETURN_IF_ERROR(PostParseQueryString(*this));
  VMSDK_RETURN_IF_ERROR(ManipulateReturnsClause(*this));

  return absl::OkStatus();
}

bool ReplyWithValue(ValkeyModuleCtx *ctx,
                    data_model::AttributeDataType data_type,
                    std::string_view name, indexes::IndexerType indexer_type,
                    const expr::Value &value, int dialect) {
  if (value.IsNil()) {
    return false;
  }
  if (data_type == data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH) {
    ValkeyModule_ReplyWithSimpleString(ctx, name.data());
    auto value_sv = value.AsStringView();
    ValkeyModule_ReplyWithStringBuffer(ctx, value_sv.data(), value_sv.size());
  } else {
    char double_storage[50];
    std::string_view value_view;
    if (name == "$") {
      value_view = value.AsStringView();
    } else {
      switch (indexer_type) {
        case indexes::IndexerType::kTag:
        case indexes::IndexerType::kNone: {
          value_view = value.AsStringView();
          break;
        }
        case indexes::IndexerType::kNumeric: {
          auto dble = value.AsDouble();
          if (!dble) {
            return false;
          }
          auto double_size =
              snprintf(double_storage, sizeof(double_storage), "%.11g", *dble);
          value_view = std::string_view(double_storage, double_size);
          break;
        }
        default:
          CHECK(false) << " Received type " << int(indexer_type);
      }
    }
    ValkeyModule_ReplyWithSimpleString(ctx, name.data());
    if (dialect == 2) {
      ValkeyModule_ReplyWithStringBuffer(ctx, value_view.data(),
                                         value_view.size());
    } else {
      std::string s = absl::StrCat("[", value_view, "]");
      ValkeyModule_ReplyWithStringBuffer(ctx, s.data(), s.size());
    }
  }
  return true;
}

absl::Status SendReplyInner(ValkeyModuleCtx *ctx,
                            std::deque<indexes::Neighbor> &neighbors,
                            AggregateParameters &parameters) {
  auto identifier =
      parameters.index_schema->GetIdentifier(parameters.attribute_alias);
  if (!identifier.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
    return identifier.status();
  }

  query::ProcessNeighborsForReply(
      ctx, parameters.index_schema->GetAttributeDataType(), neighbors,
      parameters, identifier.value());

  size_t key_index = 0, scores_index = 0;
  if (parameters.load_key) {
    key_index = parameters.AddRecordAttribute("__key", "__key",
                                              indexes::IndexerType::kNone);
  }
  if (parameters.IsVectorQuery()) {
    auto score_sv = vmsdk::ToStringView(parameters.score_as.get());
    scores_index = parameters.AddRecordAttribute(score_sv, score_sv,
                                                 indexes::IndexerType::kNone);
  }

  //
  //  1. Process the collected Neighbors into Aggregate Records.
  //
  auto data_type = parameters.index_schema->GetAttributeDataType().ToProto();
  RecordSet records(&parameters);
  for (auto &n : neighbors) {
    auto rec =
        std::make_unique<Record>(parameters.record_indexes_by_alias_.size());
    if (parameters.load_key) {
      rec->fields_.at(key_index) = expr::Value(n.external_id.get()->Str());
    }
    if (parameters.IsVectorQuery()) {
      rec->fields_.at(scores_index) = expr::Value(n.distance);
    }
    // For the fields that were fetched, stash them into the RecordSet
    if (n.attribute_contents.has_value() && !parameters.no_content) {
      for (auto &[name, records_map_value] : *n.attribute_contents) {
        auto value = vmsdk::ToStringView(records_map_value.value.get());
        std::optional<size_t> record_index;
        if (auto by_alias = parameters.record_indexes_by_alias_.find(name);
            by_alias != parameters.record_indexes_by_alias_.end()) {
          record_index = by_alias->second;
          assert(record_index < rec->fields_.size());
        } else if (auto by_identifier =
                       parameters.record_indexes_by_identifier_.find(name);
                   by_identifier !=
                   parameters.record_indexes_by_identifier_.end()) {
          record_index = by_identifier->second;
          assert(record_index < rec->fields_.size());
        }
        if (record_index) {
          // Need to find the field type
          indexes::IndexerType indexer_type =
              parameters.record_info_by_index_[*record_index].data_type_;
          switch (indexer_type) {
            case indexes::IndexerType::kNumeric: {
              auto numeric_value = vmsdk::To<double>(value);
              if (numeric_value.ok()) {
                rec->fields_[*record_index] =
                    expr::Value(numeric_value.value());
              } else {
                // Skip this field, it contains an invalid number....
                // todo Prove that skipping this field is the right thing to
                // do
              }
              break;
            }
            default:
              if (data_type ==
                  data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH) {
                rec->fields_[*record_index] = expr::Value(value);
              } else {
                auto v = vmsdk::JsonUnquote(value);
                if (v) {
                  rec->fields_[*record_index] = expr::Value(std::move(*v));
                } else {
                  goto drop_record;
                }
              }
              break;
          }
        } else {
          rec->extra_fields_.push_back(
              std::make_pair(std::string(name), expr::Value(value)));
        }
      }
    }
    records.push_back(std::move(rec));
  drop_record:;
  }
  //
  //  2. Perform the aggregation stages
  //
  for (auto &stage : parameters.stages_) {
    // Todo Check for timeout
    VMSDK_RETURN_IF_ERROR(stage->Execute(records));
  }

  //
  //  3. Generate the result
  //
  ValkeyModule_ReplyWithArray(ctx, 1 + records.size());
  ValkeyModule_ReplyWithLongLong(ctx, static_cast<long long>(records.size()));
  while (!records.empty()) {
    auto rec = records.pop_front();
    ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
    //
    // First the referenced fields
    //
    size_t array_count = 0;
    CHECK(rec->fields_.size() <= parameters.record_info_by_index_.size());
    for (size_t i = 0; i < rec->fields_.size(); ++i) {
      if (ReplyWithValue(
              ctx, parameters.index_schema->GetAttributeDataType().ToProto(),
              parameters.record_info_by_index_[i].identifier_,
              parameters.record_info_by_index_[i].data_type_, rec->fields_[i],
              parameters.dialect)) {
        array_count += 2;
      }
    }
    //
    // Now the unreferenced ones
    //
    for (const auto &[name, value] : rec->extra_fields_) {
      if (ReplyWithValue(
              ctx, parameters.index_schema->GetAttributeDataType().ToProto(),
              name, indexes::IndexerType::kNone, value, parameters.dialect)) {
        array_count += 2;
      }
    }
    ValkeyModule_ReplySetArrayLength(ctx, array_count);
  }
  return absl::OkStatus();
}

void AggregateParameters::SendReply(ValkeyModuleCtx *ctx,
                                    std::deque<indexes::Neighbor> &neighbors) {
  auto identifier = index_schema->GetIdentifier(attribute_alias);
  auto result = SendReplyInner(ctx, neighbors, *this);
  if (!result.ok()) {
    ++Metrics::GetStats().query_failed_requests_cnt;
    ValkeyModule_ReplyWithError(ctx, result.message().data());
  }
}

}  // namespace aggregate

absl::Status FTAggregateCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                            int argc) {
  return QueryCommand::Execute(
      ctx, argv, argc,
      std::unique_ptr<QueryCommand>(
          new aggregate::AggregateParameters(ValkeyModule_GetSelectedDb(ctx))));
}

}  // namespace valkey_search
