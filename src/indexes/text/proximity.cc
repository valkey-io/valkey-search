#include "proximity.h"

namespace valkey_search::indexes::text {

ProximityIterator::ProximityIterator(
    std::vector<std::unique_ptr<TextIterator>>&& iters, size_t slop,
    bool in_order, FieldMaskPredicate field_mask,
    const InternedStringSet* untracked_keys)
    : iters_(std::move(iters)),
      done_(false),
      slop_(slop),
      in_order_(in_order),
      untracked_keys_(untracked_keys),
      current_key_(nullptr),
      current_start_pos_(std::nullopt),
      current_end_pos_(std::nullopt),
      field_mask_(field_mask) {
  VMSDK_LOG(WARNING, nullptr) << "PI::init";
  VMSDK_LOG(WARNING, nullptr) << "iters_ size" << iters_.size();
  if (iters_.empty()) {
    VMSDK_LOG(WARNING, nullptr) << "PI::init done 1";
    done_ = true;
    return;
  }
  // Prime iterators to the first common key and valid position combo
  NextKey();
}

uint64_t ProximityIterator::FieldMask() const { return field_mask_; }

bool ProximityIterator::DoneKeys() const {
  VMSDK_LOG(WARNING, nullptr) << "PI::DoneKeys";
  if (done_) {
    return true;
  }
  for (auto& c : iters_) {
    if (c->DoneKeys()) {
      return true;
    }
  }
  return false;
}

const InternedStringPtr& ProximityIterator::CurrentKey() const {
  VMSDK_LOG(WARNING, nullptr) << "PI::CurrentKey";
  CHECK(current_key_ != nullptr);
  return current_key_;
}

bool ProximityIterator::NextKey() {
  VMSDK_LOG(WARNING, nullptr) << "PI::NextKey1";
  // On the second call onwards, advance any text iterators that are still
  // sitting on the old key.
  auto advance = [&]() -> void {
    for (auto& c : iters_) {
      if (!c->DoneKeys() && c->CurrentKey() == current_key_) {
        c->NextKey();
      }
    }
  };
  if (current_key_) {
    advance();
  }
  while (!DoneKeys()) {
    VMSDK_LOG(WARNING, nullptr) << "PI::NextKey - in loop";
    // 1) Move to the next common key amongst all text iterators.
    if (FindCommonKey()) {
      current_start_pos_ = std::nullopt;
      current_end_pos_ = std::nullopt;
      // 2) Move to the next valid position combination across all text
      // iterators.
      //    Exit, if no valid position combination key is found.
      if (NextPosition()) {
        return true;
      }
    }
    // Otherwise, loop and try again.
    advance();
  }
  current_key_ = nullptr;
  VMSDK_LOG(WARNING, nullptr) << "PI::NextKey keys exhausted";
  return false;
}

bool ProximityIterator::FindCommonKey() {
  VMSDK_LOG(WARNING, nullptr) << "PI::FindCommonKey";
  // 1) Validate children and compute min/max among current keys
  InternedStringPtr min_key = nullptr;
  InternedStringPtr max_key = nullptr;
  for (auto& iter : iters_) {
    auto k = iter->CurrentKey();
    if (!min_key || k->Str() < min_key->Str()) min_key = k;
    if (!max_key || k->Str() > max_key->Str()) max_key = k;
  }
  // 2) If min == max, we found a common key
  if (min_key->Str() == max_key->Str()) {
    current_key_ = max_key;
    VMSDK_LOG(WARNING, nullptr) << "PI::FindCommonKey found common key " << current_key_->Str();
    return true;
  }
  // 3) Advance all iterators that are strictly behind the current max_key
  for (auto& iter : iters_) {
    iter->SeekForwardKey(max_key);
    VMSDK_LOG(WARNING, nullptr)
                << "PI::FindCommonKey advancing child from " << iter->CurrentKey()->Str()
                << " toward " << max_key->Str();
  }
  return false;
}

bool ProximityIterator::SeekForwardKey(const InternedStringPtr& target_key) {
  // If current key is already >= target_key, no need to seek
  if (current_key_ && current_key_->Str() >= target_key->Str()) {
    return true;
  }
  // Skip all keys less than target_key for all iterators
  for (auto& iter : iters_) {
    while (!iter->DoneKeys() && iter->CurrentKey()->Str() < target_key->Str()) {
      iter->NextKey();
    }
  }
  // Find next valid key/position combination
  while (!DoneKeys()) {
    if (FindCommonKey()) {
      current_start_pos_ = std::nullopt;
      current_end_pos_ = std::nullopt;
      if (NextPosition()) {
        return true;
      }
    }
    // Advance past current key and try again
    for (auto& c : iters_) {
      if (!c->DoneKeys() && c->CurrentKey() == current_key_) {
        c->NextKey();
      }
    }
  }
  current_key_ = nullptr;
  return false;
}

// I am concerned about this. What if we don't test all combinations across text iterators?
// bool ProximityIterator::DonePositions() const {
//   VMSDK_LOG(WARNING, nullptr) << "PI::DonePositions";
//   if (done_) {
//     return true;
//   }
//   for (auto& c : iters_) {
//     if (c->DonePositions()) {
//       return true;
//     }
//   }
//   return false;
// }

bool ProximityIterator::DonePositions() const {
  VMSDK_LOG(WARNING, nullptr) << "PI::DonePositions";
  if (done_) {
    return true;
  }
  for (auto& c : iters_) {
    if (!c->DonePositions()) {
      return false;
    }
  }
  return true;
}

std::pair<uint32_t, uint32_t> ProximityIterator::CurrentPosition() const {
  CHECK(current_start_pos_.has_value() && current_end_pos_.has_value());
  return std::make_pair(current_start_pos_.value(), current_end_pos_.value());
}

bool ProximityIterator::NextPosition() {
  const size_t n = iters_.size();
  std::vector<std::pair<uint32_t, uint32_t>> positions(n);
  
  auto get_iterator_to_advance = [&]() -> std::optional<size_t> {
    if (in_order_) {
      // Check for order violation first
      for (size_t i = 0; i < n - 1; ++i) {
        if (positions[i].first >= positions[i + 1].first) {
          return i + 1;  // Advance the right position when order is violated
        }
        
        // Then check for slop violation
        if (positions[i + 1].first - positions[i].second - 1 > slop_) {
          return i + 1;  // Advance the right position when slop is violated
        }
      }
    } else {
      // For unordered case with slop
      if (slop_ >= 0) {
        // Create sorted positions for checking consecutive differences
        std::vector<std::pair<uint32_t, uint32_t>> sorted_pos = positions;
        std::sort(sorted_pos.begin(), sorted_pos.end());
        
        for (size_t i = 0; i < n - 1; ++i) {
          if (sorted_pos[i + 1].first - sorted_pos[i].second - 1 > slop_) {
            // Find original index of the smaller position
            for (size_t j = 0; j < n; ++j) {
              if (positions[j] == sorted_pos[i]) {
                return j;
              }
            }
          }
        }
      }
    }
    // If no violations, try to advance first non-done iterator
    for (size_t i = 0; i < n; ++i) {
        if (!iters_[i]->DonePositions()) {
            return i;
        }
    }
    return std::nullopt;  // All iterators are done
  };

  bool should_advance = false;
  if (current_start_pos_.has_value() && current_end_pos_.has_value()) {
    should_advance = true;
  }

  while (!DonePositions()) {
    // Collect current positions
    for (size_t i = 0; i < n; ++i) {
      positions[i] = iters_[i]->CurrentPosition();
    }

    if (should_advance) {
      should_advance = false;
      // size_t idx_to_advance = get_iterator_to_advance().value();
      // if (!iters_[idx_to_advance]->DonePositions()) {
      //   iters_[idx_to_advance]->NextPosition();
      // }
      // This is brave code. But relies on get_iterator_to_advance doing its job.
      should_advance = false;
      size_t idx_to_advance = get_iterator_to_advance().value();
      iters_[idx_to_advance]->NextPosition();
    }

    bool valid = true;
    if (in_order_) {
      // Check order and slop for ordered case
      for (size_t i = 0; i + 1 < n && valid; ++i) {
        if (positions[i].first >= positions[i + 1].first) {
          valid = false;
        } else if (positions[i + 1].first - positions[i].second - 1 > slop_) {
          valid = false;
        }
      }
    } else if (slop_ >= 0) {
      // Check only slop for unordered case
      std::vector<std::pair<uint32_t, uint32_t>> sorted_pos = positions;
      std::sort(sorted_pos.begin(), sorted_pos.end());
      
      for (size_t i = 0; i + 1 < n && valid; ++i) {
        if (sorted_pos[i + 1].first - sorted_pos[i].second - 1 > slop_) {
          valid = false;
        }
      }
    }

    if (valid) {
      current_start_pos_ = positions[0].first;
      current_end_pos_ = positions[n - 1].second;
      return true;
    }

    auto idx_opt = get_iterator_to_advance();
    if (!idx_opt.has_value()) {
      // No valid iterator to advance, we're done
      break;
    }
    iters_[idx_opt.value()]->NextPosition();
  }

  current_start_pos_ = std::nullopt;
  current_end_pos_ = std::nullopt;
  return false;
}

// bool ProximityIterator::NextPosition() {
//   VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition";
//   const size_t n = iters_.size();
//   std::vector<std::pair<uint32_t, uint32_t>> positions(n);
//   auto advance_smallest = [&]() -> void {
//     // size_t advance_idx = 0;
//     // for (size_t i = 1; i < n; ++i) {
//     //   if (positions[i].first < positions[advance_idx].first) {
//     //     advance_idx = i;
//     //   }
//     // }
//     // iters_[advance_idx]->NextPosition();
//     uint32_t min_pos = UINT32_MAX;
//     size_t leftmost_idx = SIZE_MAX;
    
//     // Find minimum position and leftmost iterator with that position
//     // Should be fixed.
//     for (size_t i = 0; i < n; ++i) {
//       if (positions[i].first < min_pos) {
//         min_pos = positions[i].first;
//         leftmost_idx = i;
//       }
//     }
    
//     // Count how many iterators are at the minimum position
//     size_t count_at_min = 0;
//     for (size_t i = 0; i < n; ++i) {
//       if (positions[i].first == min_pos) {
//         count_at_min++;
//       }
//     }
    
//     if (count_at_min > 1) {
//       VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition multiple iterators at same position";
//       // Multiple iterators at same position - advance all except leftmost (if not done)
//       bool advanced_any = false;
//       for (size_t i = 0; i < n; ++i) {
//         if (positions[i].first == min_pos && i != leftmost_idx && !iters_[i]->DonePositions()) {
//           VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition advancing iterator " << i << " at position " << min_pos;
//           iters_[i]->NextPosition(); // this takes it (wi:gre*) to the end (done).
//           advanced_any = true;
//         }
//       }
      
//       // If no non-leftmost iterator could be advanced, advance leftmost
//       if (!advanced_any && !iters_[leftmost_idx]->DonePositions()) {
//         VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition advancing leftmost iterator " << leftmost_idx << " at position " << min_pos;
//         iters_[leftmost_idx]->NextPosition();
//       }
//     } else {
//       VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition single iterator at min position";
//       // Single iterator at minimum position - advance it if not done
//       if (!iters_[leftmost_idx]->DonePositions()) {
//         VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition advancing leftmost iterator";
//         iters_[leftmost_idx]->NextPosition();
//       } else {
//         VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition leftmost iterator is done";
//         // If min position iterator is done, advance first non-done iterator
//         for (size_t i = 0; i < n; ++i) {
//           if (!iters_[i]->DonePositions()) {
//             iters_[i]->NextPosition();
//             break;
//           }
//         }
//       }
//     }
//   };
//   bool should_advance = false;
//   // On a second call, we advance.
//   if (current_start_pos_.has_value() && current_end_pos_.has_value()) {
//     should_advance = true;
//   }
//   while (!DonePositions()) {
//     VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition in loop";
//     // Collect current positions (start, end) of the text iterators.
//     for (size_t i = 0; i < n; ++i) {
//       positions[i] = iters_[i]->CurrentPosition();
//     }
//     if (should_advance) {
//       should_advance = false;
//       advance_smallest();
//     }
//     bool valid = true;
//     // Check if the current combination of positions satisfies the proximity
//     // constraints (order/slop) using start and end positions.
//     // We should check both. not just one constraint.
//     for (size_t i = 0; i + 1 < n && valid; ++i) {
//       if (in_order_ && positions[i].first >= positions[i + 1].first) {
//         VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition order failed between "
//                                     << iters_[i]->CurrentKey()->Str() << " and "
//                                     << iters_[i + 1]->CurrentKey()->Str() << " at "
//                                     << positions[i].first << " and "
//                                     << positions[i + 1].first;
        
//         valid = false;
//       } else if (positions[i + 1].first - positions[i].second - 1 > slop_) {
//         VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition slop failed between "
//                                     << iters_[i]->CurrentKey()->Str() << " and "
//                                     << iters_[i + 1]->CurrentKey()->Str() << " by "
//                                     << (positions[i + 1].first - positions[i].second - 1)
//                                     << ", slop is " << slop_;
//         valid = false;
//       }
//     }
//     if (valid) {
//       current_start_pos_ = positions[0].first;
//       current_end_pos_ = positions[n - 1].second;
//       VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition returning true on "
//                                   << current_start_pos_.value() << " and "
//                                   << current_end_pos_.value();
//       return true;
//     }
//     // Should be fixed... Need to advance what is breaking the constraint. or try all combos.
//     advance_smallest();
//   }
//   current_start_pos_ = std::nullopt;
//   current_end_pos_ = std::nullopt;
//   VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition positions exhausted";
//   return false;
// }

}  // namespace valkey_search::indexes::text
