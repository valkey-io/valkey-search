/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <variant>

#include "src/indexes/vector_base.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/type_conversions.h"

namespace valkey_search {

class AttributeData {
 public:
  using Variant = std::variant<std::monostate, vmsdk::UniqueValkeyString,
                               indexes::VectorRecordWithSize>;

  AttributeData() = default;

  explicit AttributeData(indexes::DeletionType del) : deletion_type(del) {}

  explicit AttributeData(vmsdk::UniqueValkeyString str)
      : data_(std::move(str)) {}

  explicit AttributeData(indexes::VectorRecordWithSize vec)
      : data_(std::move(vec)) {}

  bool IsNull() const {
    if (std::holds_alternative<std::monostate>(data_)) {
      return true;
    }
    if (auto *str = std::get_if<vmsdk::UniqueValkeyString>(&data_)) {
      return *str == nullptr;
    }
    return !std::get<indexes::VectorRecordWithSize>(data_).vector_record;
  }

  bool IsVector() const {
    return std::holds_alternative<indexes::VectorRecordWithSize>(data_);
  }

  // Read-only view of the string payload, for callers that must inspect a
  // value without taking it -- FILTER evaluation reads the fetched attributes
  // of a document it does not own. Empty when this holds no string.
  absl::string_view GetStringView() const {
    if (auto *str = std::get_if<vmsdk::UniqueValkeyString>(&data_)) {
      return str->get() ? vmsdk::ToStringView(str->get()) : absl::string_view{};
    }
    // A vector's payload is its raw bytes. Returning empty here instead would
    // make a FILTER read every vector field as "", so `@vec == ''` matched
    // any key that had one; Redis compares the value itself.
    if (auto *vec = std::get_if<indexes::VectorRecordWithSize>(&data_)) {
      if (vec->vector_record) {
        return absl::string_view(vec->vector_record->GetRawVector(), vec->size);
      }
    }
    return {};
  }

  size_t GetLength() const {
    if (std::holds_alternative<std::monostate>(data_)) {
      return 0;
    }
    if (auto *str = std::get_if<vmsdk::UniqueValkeyString>(&data_)) {
      return vmsdk::ToStringView(str->get()).length();
    }
    return std::get<indexes::VectorRecordWithSize>(data_).size;
  }

  vmsdk::UniqueValkeyString ConsumeString() {
    return std::get<vmsdk::UniqueValkeyString>(std::move(data_));
  }

  std::shared_ptr<indexes::VectorRecord> ConsumeVector() {
    return std::get<indexes::VectorRecordWithSize>(std::move(data_))
        .vector_record;
  }

  indexes::DeletionType deletion_type{indexes::DeletionType::kNone};

 private:
  Variant data_{std::monostate{}};
};

}  // namespace valkey_search
