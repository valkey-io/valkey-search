/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/commands/ft_aggregate_exec.h"

#include <algorithm>
#include <limits>
#include <queue>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/strings/numbers.h"
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
        ArgVector args;
        for (auto& nargs : reducer.args_) {
          args.emplace_back(nargs->Evaluate(ctx, *record));
        }
        group_it->second.emplace_back(std::move(reducer.info_->make_instance()),
                                      std::vector<ArgVector>{});
      }
    }
    for (int i = 0; i < reducers_.size(); ++i) {
      ArgVector args;
      for (auto& nargs : reducers_[i].args_) {
        args.emplace_back(nargs->Evaluate(ctx, *record));
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

class Quantile : public GroupBy::ReducerInstance {
  struct Sample {
    double value;
    size_t g;      // Number of ranks this sample represents
    size_t delta;  // Uncertainty between ranks

    Sample(double v, size_t g_val, size_t d) : value(v), g(g_val), delta(d) {}
  };

  static constexpr double EPSILON = 0.01;  // 1% error bound
  static constexpr size_t kDefaultBufferSize = 500;

  mutable std::vector<double> buffer_;
  mutable std::vector<Sample> samples_;
  size_t n_{0};  // Total number of values inserted
  double quantile_{0.0};
  bool has_error_{false};

  // Calculate maximum allowed error for a given rank
  double GetMaxVal(double rank) const { return EPSILON * 2.0 * rank; }

  // Flush buffer: sort and merge into samples
  // Walk the existing sample list forward while merging
  // sorted buffer values in order.
  void Flush() {
    if (buffer_.empty()) return;

    std::sort(buffer_.begin(), buffer_.end());

    if (samples_.empty()) {
      // First flush: create samples from buffer
      for (double val : buffer_) {
        samples_.emplace_back(val, 1, 0);
      }
    } else {
      // Merge sorted buffer into existing samples
      std::vector<Sample> merged;
      size_t buf_idx = 0;
      size_t samp_idx = 0;
      double r = 0;

      while (buf_idx < buffer_.size() || samp_idx < samples_.size()) {
        if (samp_idx >= samples_.size()) {
          // Remaining buffer values
          double max_val = GetMaxVal(r);
          size_t delta =
              max_val > 1.0 ? static_cast<size_t>(std::floor(max_val)) - 1 : 0;
          merged.emplace_back(buffer_[buf_idx++], 1, delta);
        } else if (buf_idx >= buffer_.size()) {
          // Remaining samples
          r += samples_[samp_idx].g;
          merged.push_back(samples_[samp_idx++]);
        } else if (buffer_[buf_idx] < samples_[samp_idx].value) {
          // Insert buffer value before current sample
          double max_val = GetMaxVal(r);
          size_t delta =
              max_val > 1.0 ? static_cast<size_t>(std::floor(max_val)) - 1 : 0;
          merged.emplace_back(buffer_[buf_idx++], 1, delta);
        } else {
          // Keep existing sample, advance rank
          r += samples_[samp_idx].g;
          merged.push_back(samples_[samp_idx++]);
        }
      }

      samples_ = std::move(merged);
    }

    buffer_.clear();
  }

  // Compress samples to maintain space bounds.
  // Walks backward from the second-to-last sample so that merges
  // propagate toward the tail where error bounds are largest.
  void Compress() {
    if (samples_.size() < 2) return;

    // Compute rank of the last sample (n - 1 - last.g)
    double r =
        static_cast<double>(n_) - 1.0 - static_cast<double>(samples_.back().g);

    // Walk backward from second-to-last to first
    for (int i = static_cast<int>(samples_.size()) - 2; i >= 0; --i) {
      Sample& curr = samples_[i];
      Sample& parent = samples_[i + 1];
      double g_curr = curr.g;

      if (curr.g + parent.g + parent.delta <=
          static_cast<size_t>(GetMaxVal(r))) {
        // Merge curr into parent
        parent.g += curr.g;
        // Mark for removal by setting g to 0
        curr.g = 0;
      }
      r -= g_curr;
    }

    // Remove merged samples (g == 0)
    samples_.erase(std::remove_if(samples_.begin(), samples_.end(),
                                  [](const Sample& s) { return s.g == 0; }),
                   samples_.end());
  }

  // Query for quantile value
  double Query(double q) {
    Flush();

    if (samples_.empty()) {
      return std::numeric_limits<double>::quiet_NaN();
    }

    // Calculate target rank
    double t = std::ceil(q * n_);
    double max_val = GetMaxVal(t);
    t += std::floor(max_val / 2.0);

    // Walk forward: find the first sample whose cumulative rank
    // plus uncertainty reaches the target.
    double rank = 0;
    const Sample* prev = &samples_[0];

    for (size_t i = 1; i < samples_.size(); ++i) {
      const Sample& cur = samples_[i];
      if (rank + cur.g + cur.delta >= t) {
        break;
      }
      rank += cur.g;
      prev = &cur;
    }

    return prev->value;
  }

  // Try to insert a single value. Returns true if it was numeric.
  bool InsertValue(const expr::Value& val) {
    if (val.IsNil()) return false;

    // Direct numeric
    auto d = val.AsDouble();
    if (d) {
      buffer_.push_back(*d);
      n_++;
      if (buffer_.size() >= kDefaultBufferSize) {
        Flush();
        Compress();
      }
      return true;
    }

    // String that can be parsed as a number
    if (val.IsString()) {
      double parsed;
      if (absl::SimpleAtod(val.AsStringView(), &parsed)) {
        buffer_.push_back(parsed);
        n_++;
        if (buffer_.size() >= kDefaultBufferSize) {
          Flush();
          Compress();
        }
        return true;
      }
    }

    return false;
  }

  void ProcessRecords(const std::vector<ArgVector>& all_values) override {
    if (all_values.empty()) {
      return;
    }

    // Extract and validate quantile value from first record
    auto quantile_opt = all_values[0][1].AsDouble();
    if (!quantile_opt) {
      // Non-numeric quantile - set error flag
      has_error_ = true;
      quantile_ = -1.0;
      return;
    }

    quantile_ = *quantile_opt;

    // Validate quantile range
    if (quantile_ < 0.0 || quantile_ > 1.0) {
      // Invalid range - set error flag
      has_error_ = true;
      return;
    }

    // Insert values, handling both scalar and string-encoded values
    for (const auto& values : all_values) {
      InsertValue(values[0]);
    }
  }

  expr::Value GetResult() const override {
    // Handle error conditions
    if (has_error_ || quantile_ < 0.0 || quantile_ > 1.0) {
      return expr::Value();  // Return nil for invalid quantile
    }

    // Handle empty group or all-nil values
    if (n_ == 0) {
      return expr::Value();
    }

    // Query for quantile (const_cast is safe because Flush only modifies
    // mutable members)
    double result = const_cast<Quantile*>(this)->Query(quantile_);

    if (std::isnan(result)) {
      return expr::Value();
    }

    return expr::Value(result);
  }
};

template <typename T>
std::unique_ptr<GroupBy::ReducerInstance> MakeReducer() {
  return std::unique_ptr<GroupBy::ReducerInstance>(std::make_unique<T>());
}

absl::flat_hash_map<std::string, GroupBy::ReducerInfo> GroupBy::reducerTable{
    {"AVG", GroupBy::ReducerInfo{"AVG", 1, 1, &MakeReducer<Avg>}},
    {"COUNT", GroupBy::ReducerInfo{"COUNT", 0, 0, &MakeReducer<Count>}},
    {"COUNT_DISTINCT",
     GroupBy::ReducerInfo{"COUNT_DISTINCT", 1, 1, &MakeReducer<CountDistinct>}},
    {"MIN", GroupBy::ReducerInfo{"MIN", 1, 1, &MakeReducer<Min>}},
    {"MAX", GroupBy::ReducerInfo{"MAX", 1, 1, &MakeReducer<Max>}},
    {"QUANTILE",
     GroupBy::ReducerInfo{"QUANTILE", 2, 2, &MakeReducer<Quantile>}},
    {"STDDEV", GroupBy::ReducerInfo{"STDDEV", 1, 1, &MakeReducer<Stddev>}},
    {"SUM", GroupBy::ReducerInfo{"SUM", 1, 1, &MakeReducer<Sum>}},
};

}  // namespace aggregate
}  // namespace valkey_search
