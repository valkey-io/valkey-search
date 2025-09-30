#include "proximity.h"

namespace valkey_search::indexes::text {

ProximityIterator::ProximityIterator(
    std::vector<std::unique_ptr<TextIterator>>&& iters, size_t slop,
    bool in_order, FieldMaskPredicate field_mask,
    const InternedStringSet* untracked_keys)
    : iters_(std::move(iters)),
      done_(false),
      slop_(0),
      in_order_(true),
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

bool ProximityIterator::DonePositions() const {
  VMSDK_LOG(WARNING, nullptr) << "PI::DonePositions";
  if (done_) {
    return true;
  }
  for (auto& c : iters_) {
    if (c->DonePositions()) {
      return true;
    }
  }
  return false;
}

std::pair<uint32_t, uint32_t> ProximityIterator::CurrentPosition() const {
  CHECK(current_start_pos_.has_value() && current_end_pos_.has_value());
  return std::make_pair(current_start_pos_.value(), current_end_pos_.value());
}

bool ProximityIterator::IsValidPositionCombination(
    const std::vector<std::pair<uint32_t, uint32_t>>& positions) const {
  const size_t n = positions.size();
  // Check for same positions first
  for (size_t i = 0; i < n; ++i) {
    for (size_t j = i + 1; j < n; ++j) {
      if (positions[i].first == positions[j].first) {
        return false;
      }
    }
  }
  if (in_order_) {
    for (size_t i = 0; i < n - 1; ++i) {
      if (positions[i].first >= positions[i + 1].first) {
        VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition order failed between "
                                << iters_[i]->CurrentKey()->Str() << " and "
                                << iters_[i + 1]->CurrentKey()->Str() << " at "
                                << positions[i].first << " and "
                                << positions[i + 1].first;
        return false;
      }
      if (positions[i + 1].first - positions[i].second - 1 > slop_) {
        VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition slop failed between "
                                << iters_[i]->CurrentKey()->Str() << " and "
                                << iters_[i + 1]->CurrentKey()->Str() << " by "
                                << (positions[i + 1].first - positions[i].second - 1)
                                << ", slop is " << slop_;
        return false;
      }
    }
  } else if (slop_ >= 0) {
    std::vector<std::pair<uint32_t, uint32_t>> sorted_pos = positions;
    std::sort(sorted_pos.begin(), sorted_pos.end());
    for (size_t i = 0; i < n - 1; ++i) {
      if (sorted_pos[i + 1].first > sorted_pos[i].first) {
        if (sorted_pos[i + 1].first - sorted_pos[i].second - 1 > slop_) {
          VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition slop failed between positions "
                                  << sorted_pos[i].first << "," << sorted_pos[i].second 
                                  << " and " << sorted_pos[i + 1].first << "," << sorted_pos[i + 1].second
                                  << " by " << (sorted_pos[i + 1].first - sorted_pos[i].second - 1)
                                  << ", slop is " << slop_;
          return false;
        }
      }
    }
  }
  return true;
}

bool ProximityIterator::NextPosition() {
  VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition";
  const size_t n = iters_.size();
  std::vector<std::pair<uint32_t, uint32_t>> positions(n);
  bool should_advance = current_start_pos_.has_value();
  while (!DonePositions()) {
    VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition in loop";
    // Collect current positions
    for (size_t i = 0; i < n; ++i) {
      positions[i] = iters_[i]->CurrentPosition();
    }
    if (should_advance) {
      should_advance = false; 
      // Inline advance logic
      std::optional<size_t> iter_to_advance;
      // Check for same positions
      for (size_t i = 0; i < n && !iter_to_advance; ++i) {
        for (size_t j = i + 1; j < n; ++j) {
          if (positions[i].first == positions[j].first) {
            iter_to_advance = j;
            break;
          }
        }
      }
      // Check order violations
      if (!iter_to_advance && in_order_) {
        for (size_t i = 0; i < n - 1; ++i) {
          if (positions[i].first >= positions[i + 1].first) {
            iter_to_advance = i + 1;
            break; // Is this right?
          }
        }
      }
      // Check slop violations
      if (!iter_to_advance && slop_ >= 0) {
        std::vector<uint32_t> sorted_pos;
        for (const auto& p : positions) {
          sorted_pos.push_back(p.first);
        }
        std::sort(sorted_pos.begin(), sorted_pos.end());
        for (size_t i = 0; i < n - 1; ++i) {
          if (sorted_pos[i + 1] - sorted_pos[i] > slop_ + 1) {
            for (size_t j = 0; j < n; ++j) {
              if (positions[j].first == sorted_pos[i]) {
                iter_to_advance = j;
                break;
              }
            }
            break;
          }
        }
      }
      if (iter_to_advance) {
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
    if (IsValidPositionCombination(positions)) {
      current_start_pos_ = positions[0].first;
      current_end_pos_ = positions[n - 1].second;
      VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition returning true on "
                                << current_start_pos_.value() << " and "
                                << current_end_pos_.value();
      return true;
    }
    should_advance = true;
  }
  current_start_pos_ = std::nullopt;
  current_end_pos_ = std::nullopt;
  VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition positions exhausted";
  return false;
}


// bool ProximityIterator::NextPosition() {
//   VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition";
//   const size_t n = iters_.size();
//   std::vector<std::pair<uint32_t, uint32_t>> positions(n);

//   auto check_advance = [&](const std::vector<std::pair<uint32_t, uint32_t>>& pos) 
//       -> std::optional<size_t> {
//     // First, check for same positions
//     for (size_t i = 0; i < n; ++i) {
//         for (size_t j = i + 1; j < n; ++j) {
//             if (pos[i].first == pos[j].first) {
//                 // Advance the later iterator when positions are the same
//                 return j;
//             }
//         }
//     }

//     if (in_order_) {
//         for (size_t i = 0; i < n - 1; ++i) {
//             if (pos[i].first >= pos[i + 1].first) {
//                 return i + 1;
//             }
//         }
//     }

//     if (slop_ >= 0) {
//         std::vector<uint32_t> sorted_pos;
//         for (const auto& p : pos) {
//             sorted_pos.push_back(p.first);
//         }
//         std::sort(sorted_pos.begin(), sorted_pos.end());
        
//         for (size_t i = 0; i < n - 1; ++i) {
//             if (sorted_pos[i + 1] - sorted_pos[i] > slop_ + 1) {
//                 for (size_t j = 0; j < n; ++j) {
//                     if (pos[j].first == sorted_pos[i]) {
//                         return j;
//                     }
//                 }
//             }
//         }
//     }
//     return std::nullopt;
//   };

//   auto is_valid = [&](const std::vector<std::pair<uint32_t, uint32_t>>& pos) -> bool {
//     // Check for same positions first
//     for (size_t i = 0; i < n; ++i) {
//         for (size_t j = i + 1; j < n; ++j) {
//             if (pos[i].first == pos[j].first) {
//                 return false;
//             }
//         }
//     }

//     if (in_order_) {
//         for (size_t i = 0; i < n - 1; ++i) {
//             if (pos[i].first >= pos[i + 1].first) {
//                 VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition order failed between "
//                                         << iters_[i]->CurrentKey()->Str() << " and "
//                                         << iters_[i + 1]->CurrentKey()->Str() << " at "
//                                         << pos[i].first << " and "
//                                         << pos[i + 1].first;
//                 return false;
//             }
//             if (pos[i + 1].first - pos[i].second - 1 > slop_) {
//                 VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition slop failed between "
//                                         << iters_[i]->CurrentKey()->Str() << " and "
//                                         << iters_[i + 1]->CurrentKey()->Str() << " by "
//                                         << (pos[i + 1].first - pos[i].second - 1)
//                                         << ", slop is " << slop_;
//                 return false;
//             }
//         }
//     } else if (slop_ >= 0) {
//         std::vector<std::pair<uint32_t, uint32_t>> sorted_pos = pos;
//         std::sort(sorted_pos.begin(), sorted_pos.end());
        
//         for (size_t i = 0; i < n - 1; ++i) {
//             if (sorted_pos[i + 1].first > sorted_pos[i].first) {
//                 if (sorted_pos[i + 1].first - sorted_pos[i].second - 1 > slop_) {
//                     VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition slop failed between positions "
//                                             << sorted_pos[i].first << "," << sorted_pos[i].second 
//                                             << " and " << sorted_pos[i + 1].first << "," << sorted_pos[i + 1].second
//                                             << " by " << (sorted_pos[i + 1].first - sorted_pos[i].second - 1)
//                                             << ", slop is " << slop_;
//                     return false;
//                 }
//             }
//         }
//     }
//     return true;
//   };

//   auto advance = [&]() {
//     auto iter_to_advance = check_advance(positions);
//     if (iter_to_advance.has_value()) {
//       iters_[iter_to_advance.value()]->NextPosition();
//     } else {
//       // No violations, advance first non-done iterator
//       for (size_t i = 0; i < n; ++i) {
//         if (!iters_[i]->DonePositions()) {
//           iters_[i]->NextPosition();
//           return;
//         }
//       }
//     }
//   };

//   bool should_advance = current_start_pos_.has_value();

//   while (!DonePositions()) {
//     VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition in loop";
//     // Collect current positions
//     for (size_t i = 0; i < n; ++i) {
//       positions[i] = iters_[i]->CurrentPosition();
//     }

//     if (should_advance) {
//       should_advance = false;
//       advance();
//       continue;
//     }

//     if (is_valid(positions)) {
//       current_start_pos_ = positions[0].first;
//       current_end_pos_ = positions[n - 1].second;
//       VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition returning true on "
//                                 << current_start_pos_.value() << " and "
//                                 << current_end_pos_.value();
//       return true;
//     }

//     advance();
//   }

//   current_start_pos_ = std::nullopt;
//   current_end_pos_ = std::nullopt;
//   VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition positions exhausted";
//   return false;
// }

}  // namespace valkey_search::indexes::text
