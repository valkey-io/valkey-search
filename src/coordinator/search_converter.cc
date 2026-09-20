/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/coordinator/search_converter.h"

#include <memory>
#include <string>

#include "absl/status/status.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/indexes/scoring/scorer.h"
#include "src/query/search.h"
#include "src/schema_manager.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"

namespace valkey_search::coordinator {

void SortByToGRPC(const std::optional<query::SortByParameter>& sortby,
                  SearchIndexPartitionRequest* request) {
  if (!sortby.has_value()) {
    return;
  }
  auto* proto = request->mutable_sortby();
  proto->set_field(sortby->field);
  proto->set_order(sortby->order == query::SortOrder::kAscending
                       ? coordinator::SORT_ORDER_ASCENDING
                       : coordinator::SORT_ORDER_DESCENDING);
}

std::optional<query::SortByParameter> SortByFromGRPC(
    const SearchIndexPartitionRequest& request) {
  if (!request.has_sortby()) {
    return std::nullopt;
  }
  query::SortByParameter sortby;
  sortby.field = request.sortby().field();
  sortby.order = request.sortby().order() == coordinator::SORT_ORDER_ASCENDING
                     ? query::SortOrder::kAscending
                     : query::SortOrder::kDescending;
  return sortby;
}

// Exhaustive switch so a newly added ScorerType fails to compile here instead
// of silently reaching the shard as the default scorer.
Scorer ScorerToGRPC(indexes::scoring::ScorerType scorer) {
  switch (scorer) {
    case indexes::scoring::ScorerType::kBm25Std:
      return coordinator::SCORER_BM25STD;
    case indexes::scoring::ScorerType::kTfidf:
      return coordinator::SCORER_TFIDF;
  }
  return coordinator::SCORER_BM25STD;
}

// An unset or unrecognized value (older or newer peer) means the default
// scorer.
indexes::scoring::ScorerType ScorerFromGRPC(Scorer scorer) {
  switch (scorer) {
    case coordinator::SCORER_TFIDF:
      return indexes::scoring::ScorerType::kTfidf;
    default:
      return indexes::scoring::ScorerType::kBm25Std;
  }
}

absl::Status GRPCSearchRequestToParameters(
    const SearchIndexPartitionRequest& request,
    grpc::CallbackServerContext* context, query::SearchParameters* parameters) {
  parameters->timeout_ms = request.timeout_ms();
  parameters->cancellation_token = cancel::Make(request.timeout_ms(), context);
  parameters->db_num = request.db_num();
  parameters->index_schema_name = request.index_schema_name();
  parameters->attribute_alias = request.attribute_alias();
  VMSDK_ASSIGN_OR_RETURN(parameters->index_schema,
                         SchemaManager::Instance().GetIndexSchema(
                             request.db_num(), request.index_schema_name()));
  if (request.has_score_as()) {
    parameters->score_as = vmsdk::MakeUniqueValkeyString(request.score_as());
  } else {
    VMSDK_ASSIGN_OR_RETURN(parameters->score_as,
                           parameters->index_schema->DefaultReplyScoreAs(
                               request.attribute_alias()));
  }
  parameters->query = request.query();
  parameters->dialect = request.dialect();
  parameters->k = request.k();
  parameters->ef = request.ef();
  parameters->limit = query::LimitParameter{request.limit().first_index(),
                                            request.limit().number()};
  parameters->no_content = request.no_content();
  parameters->enable_partial_results = request.enable_partial_results();
  parameters->enable_consistency = request.enable_consistency();
  for (auto& return_parameter : request.return_parameters()) {
    parameters->return_attributes.emplace_back(query::ReturnAttribute(
        vmsdk::MakeUniqueValkeyString(return_parameter.identifier()),
        vmsdk::MakeUniqueValkeyString(return_parameter.alias())));
  }
  parameters->index_fingerprint_version = request.index_fingerprint_version();
  parameters->slot_fingerprint = request.slot_fingerprint();
  parameters->sortby_parameter = SortByFromGRPC(request);
  parameters->scorer = ScorerFromGRPC(request.scorer());
  parameters->verbatim = request.verbatim();
  parameters->inorder = request.inorder();
  if (request.has_slop()) {
    parameters->slop = request.slop();
  }
  parameters->filter_expression = request.filter_expression();
  return parameters->ParseFilter();
}

std::unique_ptr<SearchIndexPartitionRequest> ParametersToGRPCSearchRequest(
    const query::SearchParameters& parameters) {
  auto request = std::make_unique<SearchIndexPartitionRequest>();
  request->set_db_num(parameters.db_num);
  request->set_index_schema_name(parameters.index_schema_name);
  request->set_attribute_alias(parameters.attribute_alias);
  request->set_score_as(vmsdk::ToStringView(parameters.score_as.get()));
  request->set_query(parameters.query);
  request->set_dialect(parameters.dialect);
  request->set_k(parameters.k);
  if (parameters.ef.has_value()) {
    request->set_ef(parameters.ef.value());
  }
  request->mutable_limit()->set_first_index(parameters.limit.first_index);
  request->mutable_limit()->set_number(parameters.limit.number);
  request->set_timeout_ms(parameters.timeout_ms);
  request->set_no_content(parameters.no_content);
  request->set_enable_partial_results(parameters.enable_partial_results);
  request->set_enable_consistency(parameters.enable_consistency);
  request->set_filter_expression(parameters.filter_expression);
  request->set_verbatim(parameters.verbatim);
  request->set_inorder(parameters.inorder);
  if (parameters.slop.has_value()) {
    request->set_slop(parameters.slop.value());
  }
  for (const auto& return_attribute : parameters.return_attributes) {
    auto return_parameter = request->add_return_parameters();
    return_parameter->set_identifier(
        vmsdk::ToStringView(return_attribute.identifier.get()));
    // The receiving node consumes this as `attribute_alias` and uses it to look
    // up the index to fetch from. It must therefore be the *source* attribute
    // alias, never the (possibly renamed) output alias -- otherwise a rename
    // onto the name of another declared field would fetch that other field.
    // The output alias never crosses the wire: the requesting node labels the
    // reply from its own copy of `return_attributes`.
    return_parameter->set_alias(
        vmsdk::ToStringView(return_attribute.attribute_alias
                                ? return_attribute.attribute_alias.get()
                                : return_attribute.identifier.get()));
  }
  *request->mutable_index_fingerprint_version() =
      parameters.index_fingerprint_version;
  request->set_slot_fingerprint(parameters.slot_fingerprint);
  SortByToGRPC(parameters.sortby_parameter, request.get());
  request->set_scorer(ScorerToGRPC(parameters.scorer));
  return request;
}

}  // namespace valkey_search::coordinator
