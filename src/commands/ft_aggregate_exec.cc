/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/commands/ft_aggregate_exec.h"

#include <algorithm>
#include <queue>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "src/commands/ft_aggregate_parser.h"
#include "vmsdk/src/info.h"

// #define DBG std::cerr
#define DBG 0 && std::cerr

DEV_INTEGER_COUNTER(agg_stats, agg_limit_stages);
DEV_INTEGER_COUNTER(agg_stats, agg_group_by_stages);
DEV_INTEGER_COUNTER(agg_stats, agg_apply_stages);
DEV_INTEGER_COUNTER(agg_stats, agg_reducer_stages);
DEV_INTEGER_COUNTER(agg_stats, agg_sort_by_stages);
DEV_INTEGER_COUNTER(agg_stats, agg_filter_stages);
DEV_INTEGER_COUNTER(agg_stats, agg_filter_input_records);
DEV_INTEGER_COUNTER(agg_stats, agg_filter_output_records);
DEV_INTEGER_COUNTER(agg_stats, agg_limit_input_records);
DEV_INTEGER_COUNTER(agg_stats, agg_limit_output_records);
DEV_INTEGER_COUNTER(agg_stats, agg_group_by_input_records);
DEV_INTEGER_COUNTER(agg_stats, agg_group_by_output_records);
DEV_INTEGER_COUNTER(agg_stats, agg_apply_records);
DEV_INTEGER_COUNTER(agg_stats, agg_sort_by_records);

namespace valkey_search {
namespace aggregate {

expr::Value Attribute::GetValue(expr::Expression::EvalContext& ctx,
                                const expr::Expression::Record& record) const {
  auto rec = reinterpret_cast<const Record&>(record);
  return rec.fields_.at(record_index_);
};

expr::Expression::EvalContext ctx;

std::ostream& operator<<(std::ostream& os, const RecordSet& rs) {
  os << "<RecordSet> " << rs.size() << "\n";
  for (size_t i = 0; i < rs.size(); ++i) {
    os << i << ": ";
    rs[i]->Dump(os, rs.agg_params_);
    os << "\n";
  }
  os << "</RecordSet>\n";
  return os;
}

void Record::Dump(std::ostream& os,
                  const AggregateParameters* agg_params) const {
  os << '[';
  for (size_t i = 0; i < fields_.size(); ++i) {
    if (!fields_[i].IsNil()) {
      os << ' ';
      if (agg_params) {
        os << agg_params->record_info_by_index_[i] << ':';
      } else {
        os << '?' << i << '?';
      }
      os << fields_[i];
    }
  }
  if (!extra_fields_.empty()) {
    os << " Extra:" << extra_fields_.size() << ' ';
    for (const auto& [field, value] : extra_fields_) {
      os << " " << field << ":" << value;
    }
  }
  os << ']';
}

absl::Status Limit::Execute(RecordSet& records) const {
  DBG << "Executing LIMIT with offset: " << offset_ << " and limit: " << limit_
      << "\n";
  agg_limit_stages.Increment();
  agg_limit_input_records.Increment(records.size());
  for (auto i = 0; i < offset_ && !records.empty(); ++i) {
    records.pop_front();
  }
  while (records.size() > limit_) {
    records.pop_back();
  }
  agg_limit_output_records.Increment(records.size());
  return absl::OkStatus();
}

void SetField(Record& record, Attribute& dest, expr::Value value) {
  if (record.fields_.size() <= dest.record_index_) {
    record.fields_.resize(dest.record_index_ + 1);
  }
  record.fields_[dest.record_index_] = value;
}

absl::Status Apply::Execute(RecordSet& records) const {
  DBG << "Executing APPLY with expr: " << *expr_ << "\n";
  agg_apply_stages.Increment();
  agg_apply_records.Increment(records.size());
  for (auto& r : records) {
    SetField(*r, *name_, expr_->Evaluate(ctx, *r));
  }
  return absl::OkStatus();
}

absl::Status Filter::Execute(RecordSet& records) const {
  DBG << "Executing FILTER with expr: " << *expr_ << "\n";
  agg_filter_stages.Increment();
  agg_filter_input_records.Increment(records.size());
  RecordSet filtered(records.agg_params_);
  while (!records.empty()) {
    auto r = records.pop_front();
    auto result = expr_->Evaluate(ctx, *r);
    if (result.IsTrue()) {
      filtered.push_back(std::move(r));
    }
  }
  records.swap(filtered);
  agg_filter_output_records.Increment(records.size());
  return absl::OkStatus();
}

template <typename T>
struct SortFunctor {
  const absl::InlinedVector<SortBy::SortKey, 4>* sortkeys_;
  bool operator()(const T& l, const T& r) const {
    for (auto& sk : *sortkeys_) {
      auto lvalue = sk.expr_->Evaluate(ctx, *l);
      auto rvalue = sk.expr_->Evaluate(ctx, *r);
      auto cmp = expr::Compare(lvalue, rvalue);
      switch (cmp) {
        case expr::Ordering::kEQUAL:
        case expr::Ordering::kUNORDERED:
          continue;
        case expr::Ordering::kLESS:
          return sk.direction_ == SortBy::Direction::kASC;
        case expr::Ordering::kGREATER:
          return sk.direction_ == SortBy::Direction::kDESC;
      }
    }
    return false;
  }
};

absl::Status SortBy::Execute(RecordSet& records) const {
  DBG << "Executing SORTBY with sortkeys: " << sortkeys_.size() << "\n";
  agg_sort_by_stages.Increment();
  agg_sort_by_records.Increment(records.size());
  if (records.size() > max_) {
    // Sadly std::priority_queue can't operate on unique_ptr's. so we need an
    // extra copy
    SortFunctor<Record*> sorter{&sortkeys_};
    std::priority_queue<Record*, std::vector<Record*>, SortFunctor<Record*>>
        heap(sorter);
    for (auto i = 0; i < max_; ++i) {
      heap.push(records.pop_front().release());
    }
    while (!records.empty()) {
      heap.push(records.pop_front().release());
      auto top = RecordPtr(heap.top());  // no leak....
      heap.pop();
    }
    while (!heap.empty()) {
      records.emplace_front(RecordPtr(heap.top()));
      heap.pop();
    }
  } else {
    SortFunctor<RecordPtr> sorter{&sortkeys_};
    std::stable_sort(records.begin(), records.end(), sorter);
  }
  return absl::OkStatus();
}

absl::Status GroupBy::Execute(RecordSet& records) const {
  DBG << "Executing GROUPBY with groups: " << groups_.size()
      << " and reducers: " << reducers_.size() << "\n";

  // struct InstanceArgsPair {
  //   std::unique_ptr<ReducerInstance> instance;
  //   std::vector<ArgVector> args;
  // };
  using InstanceArgsPair =
      std::pair<std::unique_ptr<ReducerInstance>, std::vector<ArgVector>>;
  absl::flat_hash_map<GroupKey, absl::InlinedVector<InstanceArgsPair, 4>>
      groups;
  size_t record_field_count = 0;
  agg_group_by_stages.Increment();
  agg_group_by_input_records.Increment(records.size());
  while (!records.empty()) {
    auto record = records.pop_front();
    if (record_field_count == 0) {
      record_field_count = record->fields_.size();
    } else {
      CHECK(record_field_count == record->fields_.size());
    }
    GroupKey k;
    // todo: How do we handle keys that have a missing attribute in the key??
    // Skip them?
    for (auto& g : groups_) {
      k.keys_.emplace_back(g->GetValue(ctx, *record));
    }
    DBG << "Record: " << *record << " GroupKey: " << k << "\n";
    auto [group_it, inserted] = groups.try_emplace(std::move(k));
    if (inserted) {
      DBG << "Was inserted, now have " << groups.size() << " groups\n";
      for (auto& reducer : reducers_) {
        group_it->second.emplace_back(
            reducer.info_->make_instance(reducer.args_),
            std::vector<ArgVector>{});
      }
    }
    for (int i = 0; i < reducers_.size(); ++i) {
      ArgVector args;
      for (auto& nargs : reducers_[i].args_) {
        if (nargs) {
          args.emplace_back(nargs->Evaluate(ctx, *record));
        }
      }
      group_it->second[i].second.push_back(args);
    }
  }
  for (auto& group : groups) {
    DBG << "Making record for group " << group.first << "\n";
    RecordPtr record = std::make_unique<Record>(record_field_count);
    CHECK(groups_.size() == group.first.keys_.size());
    for (auto i = 0; i < groups_.size(); ++i) {
      SetField(*record, *groups_[i], group.first.keys_[i]);
    }
    CHECK(reducers_.size() == group.second.size());
    agg_reducer_stages.Increment(reducers_.size());
    for (auto i = 0; i < reducers_.size(); ++i) {
      auto& [instance, args] = group.second[i];
      instance->ProcessRecords(args);
      SetField(*record, *reducers_[i].output_, instance->GetResult());
    }
    DBG << "Record (" << records.size() << ") is : " << *record << "\n";
    records.push_back(std::move(record));
  }
  agg_group_by_output_records.Increment(records.size());
  return absl::OkStatus();
}

class Count : public GroupBy::ReducerInstance {
  size_t count_{0};
  void ProcessRecords(const std::vector<ArgVector>& all_values) override {
    count_ = all_values.size();
  }
  expr::Value GetResult() const override { return expr::Value(double(count_)); }
};

class Min : public GroupBy::ReducerInstance {
  expr::Value min_;
  void ProcessRecords(const std::vector<ArgVector>& all_values) override {
    for (const auto& values : all_values) {
      if (values[0].IsNil()) {
        continue;
      }
      if (min_.IsNil()) {
        DBG << "First Value Min is " << values[0] << "\n";
        min_ = values[0];
      } else if (min_ > values[0]) {
        DBG << " New Min: " << values[0] << "\n";
        min_ = values[0];
      } else {
        DBG << "Not new Min: " << values[0] << "\n";
      }
    }
  }
  expr::Value GetResult() const override { return min_; }
};

class Max : public GroupBy::ReducerInstance {
  expr::Value max_;
  void ProcessRecords(const std::vector<ArgVector>& all_values) override {
    for (const auto& values : all_values) {
      if (values[0].IsNil()) {
        continue;
      }
      if (max_.IsNil()) {
        max_ = values[0];
      } else if (max_ < values[0]) {
        max_ = values[0];
      }
    }
  }
  expr::Value GetResult() const override { return max_; }
};

class FirstValue : public GroupBy::ReducerInstance {
  expr::Value result_value_;
  // Sorted mode state.
  expr::Value comparison_value_;
  bool is_sorted_{false};
  bool is_desc_{false};

 public:
  void SetSorted(bool is_desc) {
    is_sorted_ = true;
    is_desc_ = is_desc;
  }

  void ProcessRecords(const std::vector<ArgVector>& all_values) override {
    if (all_values.empty()) {
      return;
    }
    if (!is_sorted_) {
      // Simple mode: first record wins unconditionally.
      result_value_ = all_values[0][0];
      return;
    }
    // Sorted mode: args layout is [return_field, sort_field].
    for (const auto& values : all_values) {
      const expr::Value& comparison_val = values[1];
      if (comparison_val.IsNil()) {
        continue;
      }
      if (comparison_value_.IsNil()) {
        result_value_ = values[0];
        comparison_value_ = comparison_val;
        continue;
      }
      // Strict < / > preserves first-encountered tie-breaking semantics.
      if (is_desc_ ? (comparison_val > comparison_value_)
                   : (comparison_val < comparison_value_)) {
        result_value_ = values[0];
        comparison_value_ = comparison_val;
      }
    }
  }

  expr::Value GetResult() const override { return result_value_; }
};

class Sum : public GroupBy::ReducerInstance {
  double sum_{0};
  void ProcessRecords(const std::vector<ArgVector>& all_values) override {
    for (const auto& values : all_values) {
      auto val = values[0].AsDouble();
      if (val) {
        sum_ += *val;
      }
    }
  }
  expr::Value GetResult() const override { return expr::Value(sum_); }
};

class Avg : public GroupBy::ReducerInstance {
  double sum_{0};
  size_t count_{0};
  void ProcessRecords(const std::vector<ArgVector>& all_values) override {
    for (const auto& values : all_values) {
      auto val = values[0].AsDouble();
      if (val) {
        sum_ += *val;
        count_++;
      }
    }
  }
  expr::Value GetResult() const override {
    return expr::Value(count_ ? sum_ / count_ : 0.0);
  }
};

class Stddev : public GroupBy::ReducerInstance {
  double sum_{0}, sq_sum_{0};
  size_t count_{0};
  void ProcessRecords(const std::vector<ArgVector>& all_values) override {
    for (const auto& values : all_values) {
      auto val = values[0].AsDouble();
      if (val) {
        sum_ += *val;
        sq_sum_ += (*val) * (*val);
        count_++;
      }
    }
  }
  expr::Value GetResult() const override {
    if (count_ <= 1) {
      return expr::Value(0.0);
    } else {
      double variance = (sq_sum_ - (sum_ * sum_) / count_) / (count_ - 1);
      return expr::Value(std::sqrt(variance));
    }
  }
};

class CountDistinct : public GroupBy::ReducerInstance {
  absl::flat_hash_set<expr::Value> values_;
  void ProcessRecords(const std::vector<ArgVector>& all_values) override {
    for (const auto& values : all_values) {
      if (!values[0].IsNil()) {
        values_.insert(values[0]);
      }
    }
  }
  expr::Value GetResult() const override {
    return expr::Value(double(values_.size()));
  }
};

// Custom argument parser for FIRST_VALUE.
// Syntax: <field> [BY <sort_field> [ASC|DESC]]
// Produces either 1 arg (simple mode) or 2 args (sorted mode: [field,
// sort_field]). The BY keyword and optional ASC/DESC direction are consumed
// here and encoded directly into the FirstValue instance, so the executor
// never needs to re-parse them.
absl::StatusOr<std::vector<std::unique_ptr<expr::Expression>>>
ParseFirstValueArgs(AggregateParameters& params, vmsdk::ArgsIterator& itr,
                    uint32_t nargs) {
  std::vector<std::unique_ptr<expr::Expression>> args;

  // arg 0: the field whose value to return.
  VMSDK_ASSIGN_OR_RETURN(auto field_tok, itr.PopNext());
  VMSDK_ASSIGN_OR_RETURN(
      auto field_expr,
      expr::Expression::Compile(params, vmsdk::ToStringView(field_tok)));
  args.push_back(std::move(field_expr));

  if (nargs == 1) {
    return args;  // simple mode
  }

  // Expect "BY" as the next token.
  VMSDK_ASSIGN_OR_RETURN(auto by_tok, itr.PopNext());
  auto by_upper = expr::FuncUpper(expr::Value(vmsdk::ToStringView(by_tok)));
  if (by_upper.AsStringView() != "BY") {
    return absl::InvalidArgumentError(absl::StrCat(
        "FIRST_VALUE: expected BY, got `", vmsdk::ToStringView(by_tok), "`"));
  }

  // arg 1: the field to sort by.
  VMSDK_ASSIGN_OR_RETURN(auto sort_tok, itr.PopNext());
  VMSDK_ASSIGN_OR_RETURN(
      auto sort_expr,
      expr::Expression::Compile(params, vmsdk::ToStringView(sort_tok)));
  args.push_back(std::move(sort_expr));

  // Optional direction token (default: ASC).
  if (nargs == 4) {
    VMSDK_ASSIGN_OR_RETURN(auto dir_tok, itr.PopNext());
    auto dir_upper = expr::FuncUpper(expr::Value(vmsdk::ToStringView(dir_tok)));
    auto dir_str = dir_upper.AsStringView();
    if (dir_str != "ASC" && dir_str != "DESC") {
      return absl::InvalidArgumentError(
          absl::StrCat("FIRST_VALUE: expected ASC or DESC, got `",
                       vmsdk::ToStringView(dir_tok), "`"));
    }
    // Push a sentinel arg only for DESC so MakeFirstValueReducer can detect
    // direction from args.size() without any runtime string parsing.
    if (dir_str == "DESC") {
      args.push_back(nullptr);
    }
  }

  return args;
}

// Factory that creates a FirstValue instance pre-configured from parsed args.
// args layout: [field]                    -> simple mode
//              [field, sort_field]         -> sorted ASC
//              [field, sort_field, is_desc] -> sorted DESC (args[2] unused at
//                                             runtime; direction baked in)
std::unique_ptr<GroupBy::ReducerInstance> MakeFirstValueReducer(
    const std::vector<std::unique_ptr<expr::Expression>>& args) {
  auto instance = std::make_unique<FirstValue>();
  if (args.size() >= 2) {
    // args[2] presence means DESC was specified; absence means ASC.
    instance->SetSorted(/*is_desc=*/args.size() == 3);
  }
  return instance;
}

template <typename T>
std::unique_ptr<GroupBy::ReducerInstance> MakeReducer(
    const std::vector<std::unique_ptr<expr::Expression>>& /*args*/) {
  return std::make_unique<T>();
}

absl::flat_hash_map<std::string, GroupBy::ReducerInfo> GroupBy::reducerTable{
    {"AVG", GroupBy::ReducerInfo{"AVG", 1, 1, &MakeReducer<Avg>}},
    {"COUNT", GroupBy::ReducerInfo{"COUNT", 0, 0, &MakeReducer<Count>}},
    {"COUNT_DISTINCT",
     GroupBy::ReducerInfo{"COUNT_DISTINCT", 1, 1, &MakeReducer<CountDistinct>}},
    {"FIRST_VALUE",
     GroupBy::ReducerInfo{"FIRST_VALUE", 1, 4, &MakeFirstValueReducer,
                          &ParseFirstValueArgs}},
    {"MIN", GroupBy::ReducerInfo{"MIN", 1, 1, &MakeReducer<Min>}},
    {"MAX", GroupBy::ReducerInfo{"MAX", 1, 1, &MakeReducer<Max>}},
    {"STDDEV", GroupBy::ReducerInfo{"STDDEV", 1, 1, &MakeReducer<Stddev>}},
    {"SUM", GroupBy::ReducerInfo{"SUM", 1, 1, &MakeReducer<Sum>}},
};

}  // namespace aggregate
}  // namespace valkey_search
