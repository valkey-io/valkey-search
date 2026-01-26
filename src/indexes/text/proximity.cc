#include "proximity.h"

#include "vmsdk/src/module_config.h"

namespace valkey_search::options {

constexpr absl::string_view kProximityInorderCompModeConfig{
    "proximity-inorder-compat-mode"};

/// Register the "--proximity-inorder-compat-mode" flag. Controls proximity
/// iterator's inorder/overlap violation check logic. When enabled, the iterator
/// uses compatibility mode logic for inorder/overlap. When disabled (default
/// behavior), the iterator uses a stricter and more natural logic for
/// inorder/overlap checks.
static auto proximity_inorder_comp_mode =
    vmsdk::config::BooleanBuilder(kProximityInorderCompModeConfig,  // name
                                  false)  // default value
        .Build();
bool GetProximityInorderCompatMode() {
  return proximity_inorder_comp_mode->GetValue();
}
}  // namespace valkey_search::options

namespace valkey_search::indexes::text {

ProximityIterator::ProximityIterator(
    absl::InlinedVector<std::unique_ptr<TextIterator>,
                        kProximityTermsInlineCapacity>&& iters,
    std::optional<uint32_t> slop, bool in_order,
    FieldMaskPredicate query_field_mask,
    const InternedStringSet* untracked_keys)
    : iters_(std::move(iters)),
      slop_(slop),
      in_order_(in_order),
      untracked_keys_(untracked_keys),
      current_position_(std::nullopt),
      current_field_mask_(0ULL),
      query_field_mask_(query_field_mask) {
  CHECK(!iters_.empty()) << "must have at least one text iterator";
  CHECK(slop_.has_value() || in_order_)
      << "ProximityIterator requires either slop or inorder=true";
  // Pre-allocate vectors used for positional checks to avoid reallocation
  positions_.resize(iters_.size());
  pos_with_idx_.resize(iters_.size());
  // Prime iterators to the first common key and valid position combo
  NextKey();
}

FieldMaskPredicate ProximityIterator::QueryFieldMask() const {
  return query_field_mask_;
}

bool ProximityIterator::DoneKeys() const {
  for (auto& iter : iters_) {
    if (iter->DoneKeys()) {
      return true;
    }
  }
  return false;
}

const Key& ProximityIterator::CurrentKey() const {
  CHECK(current_key_);
  return current_key_;
}

bool ProximityIterator::NextKey() {
  // On the second call onwards, advance any text iterators that are still
  // sitting on the old key.
  auto advance = [&]() -> void {
    for (auto& iter : iters_) {
      if (!iter->DoneKeys() && iter->CurrentKey() == current_key_) {
        iter->NextKey();
      }
    }
  };
  if (current_key_) {
    advance();
  }
  while (!DoneKeys()) {
    // 1) Move to the next common key amongst all text iterators.
    if (FindCommonKey()) {
      current_position_ = std::nullopt;
      current_field_mask_ = 0ULL;
      // 2) Move to the next valid position combination across all text
      // iterators.
      // Exit, if no key with a valid position combination is found.
      if (NextPosition()) {
        return true;
      }
    }
    // Otherwise, loop and try again.
    advance();
  }
  current_key_ = nullptr;
  return false;
}

bool ProximityIterator::FindCommonKey() {
  // 1) Validate children and compute min/max among current keys
  Key min_key;
  Key max_key;
  for (auto& iter : iters_) {
    auto k = iter->CurrentKey();
    if (!min_key || k < min_key) min_key = k;
    if (!max_key || k > max_key) max_key = k;
  }
  // 2) If min == max, we found a common key
  if (min_key == max_key) {
    current_key_ = max_key;
    return true;
  }
  // 3) Advance all iterators that are strictly behind the current max_key
  for (auto& iter : iters_) {
    iter->SeekForwardKey(max_key);
  }
  return false;
}

bool ProximityIterator::SeekForwardKey(const Key& target_key) {
  // If current key is already >= target_key, no need to seek
  if (current_key_ && current_key_ >= target_key) {
    return true;
  }
  // Skip all keys less than target_key for all iterators
  for (auto& iter : iters_) {
    if (!iter->DoneKeys() && iter->CurrentKey() < target_key) {
      iter->SeekForwardKey(target_key);
    }
  }
  // Find next valid key/position combination
  while (!DoneKeys()) {
    if (FindCommonKey()) {
      current_position_ = std::nullopt;
      current_field_mask_ = 0ULL;
      if (NextPosition()) {
        return true;
      }
    }
    // Advance past current key and try again
    for (auto& iter : iters_) {
      if (!iter->DoneKeys() && iter->CurrentKey() == current_key_) {
        iter->NextKey();
      }
    }
  }
  current_key_ = nullptr;
  return false;
}

bool ProximityIterator::DonePositions() const {
  for (auto& iter : iters_) {
    if (iter->DonePositions()) {
      return true;
    }
  }
  return false;
}

const PositionRange& ProximityIterator::CurrentPosition() const {
  CHECK(current_position_.has_value());
  return current_position_.value();
}

// Check if there is an INORDER violation between two iterators.
bool ProximityIterator::HasOrderingViolation(size_t first_idx,
                                             size_t second_idx) const {
  if (IsCompatModeInorder()) {
    // Compatibility mode: relaxed check for order using only start positions
    // only. There is no overlap check in compatibility mode.
    return positions_[first_idx].start > positions_[second_idx].start;
  } else {
    // Default mode: stricter check using range for order AND overlap check
    return positions_[first_idx].end >= positions_[second_idx].start;
  }
}

bool ProximityIterator::IsCompatModeInorder() const {
  return valkey_search::options::GetProximityInorderCompatMode() && in_order_;
}

// In case of violations, returns the iterator that should be advanced
// and optionally a target position to seek to.
// In case of no violations, std::nullopt is returned.
std::optional<ProximityIterator::ViolationInfo>
ProximityIterator::FindViolatingIterator() {
  const size_t n = positions_.size();
  uint32_t current_slop = 0;
  if (in_order_) {
    // Check ordering / overlap violations.
    for (size_t i = 0; i < n - 1; ++i) {
      if (HasOrderingViolation(i, i + 1)) {
        Position seek_target =
            IsCompatModeInorder() ? positions_[i].start : positions_[i].end;
        std::optional<Position> target_opt =
            seek_target > positions_[i + 1].start
                ? std::optional<Position>(seek_target)
                : std::nullopt;
        return ViolationInfo{i + 1, target_opt};
      }
    }
    // Check slop violations.
    // The ordering / overlap check above gives us a text group with start and
    // end. Slop violations are by summing distances between adjacent terms, so
    // we advance the first iterator to try and reduce slop in the next text
    // sequence tested.
    for (size_t i = 0; i < n - 1; ++i) {
      int32_t distance = (positions_[i + 1].start - positions_[i].start) - 1;
      current_slop += std::max(0, distance);
    }
    if (slop_.has_value() && current_slop > *slop_) {
      return ViolationInfo{0, std::nullopt};
    }
    // Check for field mask intersection (terms exist in the same field)
    FieldMaskPredicate field_mask = query_field_mask_;
    for (size_t i = 0; i < n; ++i) {
      field_mask &= iters_[i]->CurrentFieldMask();
      // No common fields, advance this iterator which is lagging behind the
      // previous one.
      if (field_mask == 0) {
        return ViolationInfo{i, std::nullopt};
      }
    }
    return std::nullopt;
  }
  // For unordered, use an index mapping to help validate constraints.
  for (size_t i = 0; i < n; ++i) {
    pos_with_idx_[i] = {positions_[i].start, i};
  }
  std::sort(pos_with_idx_.begin(), pos_with_idx_.end());
  for (size_t i = 0; i < n - 1; ++i) {
    size_t curr_idx = pos_with_idx_[i].second;
    size_t next_idx = pos_with_idx_[i + 1].second;
    // Check ordering / overlap violations.
    if (HasOrderingViolation(curr_idx, next_idx)) {
      Position seek_target = positions_[curr_idx].end;
      std::optional<Position> target_opt =
          seek_target > positions_[next_idx].start
              ? std::make_optional(seek_target)
              : std::nullopt;
      return ViolationInfo{next_idx, target_opt};
    }
  }
  // Check slop violations.
  if (slop_.has_value()) {
    // Slop violations are computed by summing distances between adjacent terms,
    // advance the first iterator to try and reduce slop in the next text
    // sequence tested.
    for (size_t i = 0; i < n - 1; ++i) {
      size_t curr_idx = pos_with_idx_[i].second;
      size_t next_idx = pos_with_idx_[i + 1].second;
      int32_t distance =
          (positions_[next_idx].start - positions_[curr_idx].start) - 1;
      current_slop += std::max(0, distance);
    }
    if (current_slop > *slop_) {
      return ViolationInfo{pos_with_idx_[0].second, std::nullopt};
    }
  }
  // Check for field mask intersection (terms exist in the same field)
  FieldMaskPredicate field_mask = query_field_mask_;
  for (size_t i = 0; i < n; ++i) {
    field_mask &= iters_[i]->CurrentFieldMask();
    // No common fields, advance this iterator which is lagging behind the
    // previous one.
    if (field_mask == 0) {
      return ViolationInfo{i, std::nullopt};
    }
  }
  return std::nullopt;
}

bool ProximityIterator::NextPosition() {
  const size_t n = iters_.size();
  bool should_advance = current_position_.has_value();
  while (!DonePositions()) {
    // 1. Synchronize physical positions cache
    for (size_t i = 0; i < n; ++i) {
      positions_[i] = iters_[i]->CurrentPosition();
    }
    auto violation = FindViolatingIterator();
    if (should_advance) {
      should_advance = false;
      if (violation) {
        size_t idx = violation->iter_idx;
        Position current_start = iters_[idx]->CurrentPosition().start;
        if (violation->seek_target.has_value()) {
          iters_[idx]->SeekForwardPosition(*violation->seek_target);
        } else {
          iters_[idx]->NextPosition();
        }
      } else {
        // No violations: advance the term appearing first physically
        size_t first_idx = in_order_ ? 0 : pos_with_idx_[0].second;
        iters_[first_idx]->NextPosition();
      }
      continue;
    }
    // 3. Validation: If no violations found, this combination is a match
    if (!violation.has_value()) {
      // Set the current field based on field mask intersection.
      current_field_mask_ = iters_[0]->CurrentFieldMask();
      for (size_t i = 1; i < n; ++i) {
        current_field_mask_ &= iters_[i]->CurrentFieldMask();
      }
      // Set the current position range.
      if (in_order_) {
        current_position_ =
            PositionRange(positions_[0].start, positions_[n - 1].end);
      } else {
        // Use sorted positions for unordered queries
        Position min_start = pos_with_idx_[0].first;
        Position max_end = positions_[pos_with_idx_[n - 1].second].end;
        current_position_ = PositionRange(min_start, max_end);
      }
      return true;
    }
    should_advance = true;
  }
  current_position_ = std::nullopt;
  current_field_mask_ = 0ULL;
  return false;
}

bool ProximityIterator::SeekForwardPosition(Position target_position) {
  // Already at or past target
  if (current_position_.has_value() &&
      current_position_.value().start >= target_position) {
    return true;
  }
  // Seek all child iterators to target position
  for (auto& iter : iters_) {
    if (!iter->DonePositions() &&
        target_position > iter->CurrentPosition().start) {
      iter->SeekForwardPosition(target_position);
    }
  }
  // Reset state and find next valid proximity match
  current_position_ = std::nullopt;
  current_field_mask_ = 0ULL;
  return NextPosition();
}

FieldMaskPredicate ProximityIterator::CurrentFieldMask() const {
  CHECK(current_field_mask_ != 0ULL);
  return current_field_mask_;
}

}  // namespace valkey_search::indexes::text
