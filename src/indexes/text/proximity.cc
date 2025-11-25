#include "proximity.h"

#include "vmsdk/src/log.h"  // TODO: remove
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
    std::vector<std::unique_ptr<TextIterator>>&& iters,
    std::optional<uint32_t> slop, bool in_order, FieldMaskPredicate field_mask,
    const InternedStringSet* untracked_keys)
    : iters_(std::move(iters)),
      slop_(slop),
      in_order_(in_order),
      untracked_keys_(untracked_keys),
      current_key_(nullptr),
      current_position_(std::nullopt),
      field_mask_(field_mask) {
  CHECK(!iters_.empty()) << "must have at least one text iterator";
  CHECK(slop_.has_value() || in_order_)
      << "ProximityIterator requires either slop or inorder=true";
  // Pre-allocate vectors used for positional checks to avoid reallocation
  positions_.resize(iters_.size());
  pos_with_idx_.resize(iters_.size());
  // Prime iterators to the first common key and valid position combo
  NextKey();
}

FieldMaskPredicate ProximityIterator::FieldMask() const { return field_mask_; }

bool ProximityIterator::DoneKeys() const {
  for (auto& iter : iters_) {
    if (iter->DoneKeys()) {
      return true;
    }
  }
  return false;
}

const Key& ProximityIterator::CurrentKey() const {
  CHECK(current_key_ != nullptr);
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
  Key min_key = nullptr;
  Key max_key = nullptr;
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
  if (valkey_search::options::GetProximityInorderCompatMode()) {
    // Compatibility mode: relaxed check for order using only start positions
    // only. There is no overlap check in compatibility mode.
    return positions_[first_idx].start > positions_[second_idx].start;
  } else {
    // Default mode: stricter check using range for order AND overlap check
    return positions_[first_idx].end >= positions_[second_idx].start;
  }
}

// In case of violations, the returned iterator is the one that should be
// advanced to try and find a valid sequence.
// In case of no violations, std::nullopt is returned and we have found a valid
// position combination.
std::optional<size_t> ProximityIterator::FindViolatingIterator() {
  const size_t n = positions_.size();
  if (in_order_) {
    // Check ordering / overlap violations.
    for (size_t i = 0; i < n - 1; ++i) {
      if (HasOrderingViolation(i, i + 1)) {
        return i + 1;
      }
    }
    // Check slop violations.
    auto current_slop =
        ((positions_[n - 1].start - positions_[0].start) - 1) - (n - 2);
    if (slop_.has_value() && current_slop > *slop_) {
      // The ordering / overlap check above gives us a text group with start and
      // end. Once we have a valid ordered, non-overlapping text sequence, any
      // rearrangement of the middle elements doesn't change the slop. Slop
      // violations are computed by using the boundary positions, so we advance
      // the first iterator to try and reduce slop in the next text sequence
      // tested.
      VMSDK_LOG(WARNING, nullptr)
          << "Slop violation detected: current_slop=" << current_slop
          << ", allowed_slop=" << *slop_ << ", advancing iterator 0"
          << ", current position start: " << positions_[0].start
          << ", end: " << positions_[n - 1].end << ", n: " << n;
      return 0;
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
      return next_idx;
    }
  }
  // Check slop violations.
  if (slop_.has_value()) {
    // Slop violations are computed by using the boundary positions, so we
    // advance the first iterator to try and reduce slop in the next text
    // sequence tested.
    size_t min_pos = pos_with_idx_[0].first;
    size_t max_pos = pos_with_idx_[n - 1].first;
    auto current_slop = ((max_pos - min_pos) - 1) - (n - 2);
    if (current_slop > *slop_) {
      return pos_with_idx_[0].second;
    }
  }
  return std::nullopt;
}

bool ProximityIterator::NextPosition() {
  const size_t n = iters_.size();
  bool should_advance = current_position_.has_value();
  while (!DonePositions()) {
    for (size_t i = 0; i < n; ++i) {
      positions_[i] = iters_[i]->CurrentPosition();
    }
    if (should_advance) {
      should_advance = false;
      if (auto iter_to_advance = FindViolatingIterator()) {
        iters_[*iter_to_advance]->NextPosition();
      } else {
        // No violations, advance first non-done iterator
        for (size_t i = 0; i < n; ++i) {
          if (!iters_[i]->DonePositions()) {
            iters_[i]->NextPosition();
            break;
          }
        }
      }
      continue;
    }
    // No violations mean that this positional combination is valid.
    if (!FindViolatingIterator().has_value()) {
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
  return false;
}

}  // namespace valkey_search::indexes::text
