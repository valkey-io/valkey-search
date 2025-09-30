#include "proximity.h"

namespace valkey_search::indexes::text {

ProximityIterator::ProximityIterator(
    std::vector<std::unique_ptr<TextIterator>>&& iters, size_t slop,
    bool in_order, FieldMaskPredicate field_mask,
    const InternedStringSet* untracked_keys)
    : iters_(std::move(iters)),
      done_(false),
      // slop_(1),
      // in_order_(false),
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

// I am concerned about this. What if we don't test all combinations across text iterators?
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

// bool ProximityIterator::DonePositions() const {
//   VMSDK_LOG(WARNING, nullptr) << "PI::DonePositions";
//   if (done_) {
//     return true;
//   }
//   for (auto& c : iters_) {
//     if (!c->DonePositions()) {
//       return false;
//     }
//   }
//   return true;
// }

std::pair<uint32_t, uint32_t> ProximityIterator::CurrentPosition() const {
  CHECK(current_start_pos_.has_value() && current_end_pos_.has_value());
  return std::make_pair(current_start_pos_.value(), current_end_pos_.value());
}




// Original code , work.

// bool ProximityIterator::NextPosition() {
//   VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition";
//   const size_t n = iters_.size();
//   std::vector<std::pair<uint32_t, uint32_t>> positions(n);
//   // auto advance_smallest = [&]() -> void {
//   //   // size_t advance_idx = 0;
//   //   // for (size_t i = 1; i < n; ++i) {
//   //   //   if (positions[i].first < positions[advance_idx].first) {
//   //   //     advance_idx = i;
//   //   //   }
//   //   // }
//   //   // iters_[advance_idx]->NextPosition();
//   //   uint32_t min_pos = UINT32_MAX;
//   //   size_t leftmost_idx = SIZE_MAX;
    
//   //   // Find minimum position and leftmost iterator with that position
//   //   // Should be fixed.
//   //   for (size_t i = 0; i < n; ++i) {
//   //     if (positions[i].first < min_pos) {
//   //       min_pos = positions[i].first;
//   //       leftmost_idx = i;
//   //     }
//   //   }
    
//   //   // Count how many iterators are at the minimum position
//   //   size_t count_at_min = 0;
//   //   for (size_t i = 0; i < n; ++i) {
//   //     if (positions[i].first == min_pos) {
//   //       count_at_min++;
//   //     }
//   //   }
    
//   //   if (count_at_min > 1) {
//   //     VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition multiple iterators at same position";
//   //     // Multiple iterators at same position - advance all except leftmost (if not done)
//   //     bool advanced_any = false;
//   //     for (size_t i = 0; i < n; ++i) {
//   //       if (positions[i].first == min_pos && i != leftmost_idx && !iters_[i]->DonePositions()) {
//   //         VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition advancing iterator " << i << " at position " << min_pos;
//   //         iters_[i]->NextPosition(); // this takes it (wi:gre*) to the end (done).
//   //         advanced_any = true;
//   //       }
//   //     }
      
//   //     // If no non-leftmost iterator could be advanced, advance leftmost
//   //     if (!advanced_any && !iters_[leftmost_idx]->DonePositions()) {
//   //       VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition advancing leftmost iterator " << leftmost_idx << " at position " << min_pos;
//   //       iters_[leftmost_idx]->NextPosition();
//   //     }
//   //   } else {
//   //     VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition single iterator at min position";
//   //     // Single iterator at minimum position - advance it if not done
//   //     if (!iters_[leftmost_idx]->DonePositions()) {
//   //       VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition advancing leftmost iterator";
//   //       iters_[leftmost_idx]->NextPosition();
//   //     } else {
//   //       VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition leftmost iterator is done";
//   //       // If min position iterator is done, advance first non-done iterator
//   //       for (size_t i = 0; i < n; ++i) {
//   //         if (!iters_[i]->DonePositions()) {
//   //           iters_[i]->NextPosition();
//   //           break;
//   //         }
//   //       }
//   //     }
//   //   }
//   // };
//     auto advance_smallest = [&]() {
//     if (in_order_) {
//       // Check positions must be ascending
//       for (size_t i = 0; i < n - 1; ++i) {
//         if (positions[i].first >= positions[i + 1].first) {
//           iters_[i + 1]->NextPosition();
//           return;
//         }
//       }
//     }

//     // Check slop (number of terms allowed between positions)
//     if (slop_ >= 0) {
//       std::vector<uint32_t> sorted_pos;
//       for (const auto& p : positions) {
//           sorted_pos.push_back(p.first);
//       }
//       std::sort(sorted_pos.begin(), sorted_pos.end());
      
//       for (size_t i = 0; i < n - 1; ++i) {
//           if (sorted_pos[i + 1] - sorted_pos[i] > slop_ + 1) {
//               // Find and advance iterator with smaller position
//               for (size_t j = 0; j < n; ++j) {
//                   if (positions[j].first == sorted_pos[i]) {
//                       iters_[j]->NextPosition();
//                       return;
//                   }
//               }
//           }
//       }
//     }
    
//     // No violations, advance first non-done iterator
//     for (size_t i = 0; i < n; ++i) {
//       if (!iters_[i]->DonePositions()) {
//         iters_[i]->NextPosition();
//         return;
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
//     // bool valid = true;
//     // // Check if the current combination of positions satisfies the proximity
//     // // constraints (order/slop) using start and end positions.
//     // // We should check both. not just one constraint.
//     // for (size_t i = 0; i + 1 < n && valid; ++i) {
//     //   if (in_order_ && positions[i].first >= positions[i + 1].first) {
//     //     VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition order failed between "
//     //                                 << iters_[i]->CurrentKey()->Str() << " and "
//     //                                 << iters_[i + 1]->CurrentKey()->Str() << " at "
//     //                                 << positions[i].first << " and "
//     //                                 << positions[i + 1].first;
        
//     //     valid = false;
//     //   } else if (positions[i + 1].first - positions[i].second - 1 > slop_) {
//     //     VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition slop failed between "
//     //                                 << iters_[i]->CurrentKey()->Str() << " and "
//     //                                 << iters_[i + 1]->CurrentKey()->Str() << " by "
//     //                                 << (positions[i + 1].first - positions[i].second - 1)
//     //                                 << ", slop is " << slop_;
//     //     valid = false;
//     //   }
//     // }
//     bool valid = true;
//     if (in_order_) {
//         // For ordered case, check positions in sequence
//         for (size_t i = 0; i + 1 < n && valid; ++i) {
//             if (positions[i].first >= positions[i + 1].first) {
//                 VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition order failed between "
//                                           << iters_[i]->CurrentKey()->Str() << " and "
//                                           << iters_[i + 1]->CurrentKey()->Str() << " at "
//                                           << positions[i].first << " and "
//                                           << positions[i + 1].first;
//                 valid = false;
//             } else if (positions[i + 1].first - positions[i].second - 1 > slop_) {
//                 VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition slop failed between "
//                                           << iters_[i]->CurrentKey()->Str() << " and "
//                                           << iters_[i + 1]->CurrentKey()->Str() << " by "
//                                           << (positions[i + 1].first - positions[i].second - 1)
//                                           << ", slop is " << slop_;
//                 valid = false;
//             }
//         }
//     } else if (slop_ >= 0) {
//         // For unordered case, sort positions and check consecutive differences
//         std::vector<std::pair<uint32_t, uint32_t>> sorted_pos = positions;
//         std::sort(sorted_pos.begin(), sorted_pos.end());
        
//         for (size_t i = 0; i + 1 < n && valid; ++i) {
//             if (sorted_pos[i + 1].first - sorted_pos[i].second - 1 > slop_) {
//                 VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition slop failed between positions "
//                                           << sorted_pos[i].first << "," << sorted_pos[i].second 
//                                           << " and " << sorted_pos[i + 1].first << "," << sorted_pos[i + 1].second
//                                           << " by " << (sorted_pos[i + 1].first - sorted_pos[i].second - 1)
//                                           << ", slop is " << slop_;
//                 valid = false;
//             }
//         }
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






// Handles inorder and slop0
// Cleaner code. We believe it handles most combos of positions if not all hopefully.

// bool ProximityIterator::NextPosition() {
//   VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition";
//   const size_t n = iters_.size();
//   std::vector<std::pair<uint32_t, uint32_t>> positions(n);

//   // For deciding which iterator to advance
//   auto check_advance = [&](const std::vector<std::pair<uint32_t, uint32_t>>& pos) 
//       -> std::optional<size_t> {
//     if (in_order_) {
//       for (size_t i = 0; i < n - 1; ++i) {
//         if (pos[i].first >= pos[i + 1].first) {
//           return i + 1;
//         }
//       }
//     }

//     if (slop_ >= 0) {
//       std::vector<uint32_t> sorted_pos;
//       for (const auto& p : pos) {
//           sorted_pos.push_back(p.first);
//       }
//       std::sort(sorted_pos.begin(), sorted_pos.end());
      
//       for (size_t i = 0; i < n - 1; ++i) {
//           if (sorted_pos[i + 1] - sorted_pos[i] > slop_ + 1) {
//               for (size_t j = 0; j < n; ++j) {
//                   if (pos[j].first == sorted_pos[i]) {
//                       return j;
//                   }
//               }
//           }
//       }
//     }
//     return std::nullopt;
//   };

//   // For validating current positions
//   auto is_valid = [&](const std::vector<std::pair<uint32_t, uint32_t>>& pos) -> bool {
//     if (in_order_) {
//       for (size_t i = 0; i < n - 1; ++i) {
//         if (pos[i].first >= pos[i + 1].first) {
//           VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition order failed between "
//                                     << iters_[i]->CurrentKey()->Str() << " and "
//                                     << iters_[i + 1]->CurrentKey()->Str() << " at "
//                                     << pos[i].first << " and "
//                                     << pos[i + 1].first;
//           return false;
//         }
//         if (pos[i + 1].first - pos[i].second - 1 > slop_) {
//           VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition slop failed between "
//                                     << iters_[i]->CurrentKey()->Str() << " and "
//                                     << iters_[i + 1]->CurrentKey()->Str() << " by "
//                                     << (pos[i + 1].first - pos[i].second - 1)
//                                     << ", slop is " << slop_;
//           return false;
//         }
//       }
//     } else if (slop_ >= 0) {
//       std::vector<std::pair<uint32_t, uint32_t>> sorted_pos = pos;
//       std::sort(sorted_pos.begin(), sorted_pos.end());
      
//       for (size_t i = 0; i < n - 1; ++i) {
//         if (sorted_pos[i + 1].first - sorted_pos[i].second - 1 > slop_) {
//           VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition slop failed between positions "
//                                     << sorted_pos[i].first << "," << sorted_pos[i].second 
//                                     << " and " << sorted_pos[i + 1].first << "," << sorted_pos[i + 1].second
//                                     << " by " << (sorted_pos[i + 1].first - sorted_pos[i].second - 1)
//                                     << ", slop is " << slop_;
//           return false;
//         }
//       }
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


bool ProximityIterator::NextPosition() {
  VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition";
  const size_t n = iters_.size();
  std::vector<std::pair<uint32_t, uint32_t>> positions(n);

  auto check_advance = [&](const std::vector<std::pair<uint32_t, uint32_t>>& pos) 
      -> std::optional<size_t> {
    // First, check for same positions
    for (size_t i = 0; i < n; ++i) {
        for (size_t j = i + 1; j < n; ++j) {
            if (pos[i].first == pos[j].first) {
                // Advance the later iterator when positions are the same
                return j;
            }
        }
    }

    if (in_order_) {
        for (size_t i = 0; i < n - 1; ++i) {
            if (pos[i].first >= pos[i + 1].first) {
                return i + 1;
            }
        }
    }

    if (slop_ >= 0) {
        std::vector<uint32_t> sorted_pos;
        for (const auto& p : pos) {
            sorted_pos.push_back(p.first);
        }
        std::sort(sorted_pos.begin(), sorted_pos.end());
        
        for (size_t i = 0; i < n - 1; ++i) {
            if (sorted_pos[i + 1] - sorted_pos[i] > slop_ + 1) {
                for (size_t j = 0; j < n; ++j) {
                    if (pos[j].first == sorted_pos[i]) {
                        return j;
                    }
                }
            }
        }
    }
    return std::nullopt;
  };

  auto is_valid = [&](const std::vector<std::pair<uint32_t, uint32_t>>& pos) -> bool {
    // Check for same positions first
    for (size_t i = 0; i < n; ++i) {
        for (size_t j = i + 1; j < n; ++j) {
            if (pos[i].first == pos[j].first) {
                return false;
            }
        }
    }

    if (in_order_) {
        for (size_t i = 0; i < n - 1; ++i) {
            if (pos[i].first >= pos[i + 1].first) {
                VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition order failed between "
                                        << iters_[i]->CurrentKey()->Str() << " and "
                                        << iters_[i + 1]->CurrentKey()->Str() << " at "
                                        << pos[i].first << " and "
                                        << pos[i + 1].first;
                return false;
            }
            if (pos[i + 1].first - pos[i].second - 1 > slop_) {
                VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition slop failed between "
                                        << iters_[i]->CurrentKey()->Str() << " and "
                                        << iters_[i + 1]->CurrentKey()->Str() << " by "
                                        << (pos[i + 1].first - pos[i].second - 1)
                                        << ", slop is " << slop_;
                return false;
            }
        }
    } else if (slop_ >= 0) {
        std::vector<std::pair<uint32_t, uint32_t>> sorted_pos = pos;
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
  };

  auto advance = [&]() {
    auto iter_to_advance = check_advance(positions);
    if (iter_to_advance.has_value()) {
      iters_[iter_to_advance.value()]->NextPosition();
    } else {
      // No violations, advance first non-done iterator
      for (size_t i = 0; i < n; ++i) {
        if (!iters_[i]->DonePositions()) {
          iters_[i]->NextPosition();
          return;
        }
      }
    }
  };

  bool should_advance = current_start_pos_.has_value();

  while (!DonePositions()) {
    VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition in loop";
    // Collect current positions
    for (size_t i = 0; i < n; ++i) {
      positions[i] = iters_[i]->CurrentPosition();
    }

    if (should_advance) {
      should_advance = false;
      advance();
      continue;
    }

    if (is_valid(positions)) {
      current_start_pos_ = positions[0].first;
      current_end_pos_ = positions[n - 1].second;
      VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition returning true on "
                                << current_start_pos_.value() << " and "
                                << current_end_pos_.value();
      return true;
    }

    advance();
  }

  current_start_pos_ = std::nullopt;
  current_end_pos_ = std::nullopt;
  VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition positions exhausted";
  return false;
}








// This was a sample to get the inorder false , slop1 working. that worked ,but it broke the main inorder code.

// bool ProximityIterator::NextPosition() {
//   VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition";
//   const size_t n = iters_.size();
//   std::vector<std::pair<uint32_t, uint32_t>> positions(n);

//   auto advance = [&]() {
//     if (in_order_) {
//       // Check positions must be ascending
//       for (size_t i = 0; i < n - 1; ++i) {
//         if (positions[i].first >= positions[i + 1].first) {
//           iters_[i + 1]->NextPosition();
//           return;
//         }
//       }

//       // Check slop for ordered case
//       if (slop_ >= 0) {
//         for (size_t i = 0; i < n - 1; ++i) {
//           if (positions[i + 1].first - positions[i].second - 1 > slop_) {
//             iters_[i + 1]->NextPosition();
//             return;
//           }
//         }
//       }
//     } else if (slop_ >= 0) {
//       // First check for same positions
//       for (size_t i = 0; i < n - 1; ++i) {
//         for (size_t j = i + 1; j < n; ++j) {
//           if (positions[i].first == positions[j].first) {
//             iters_[j]->NextPosition();  // Advance later iterator when positions are same
//             return;
//           }
//         }
//       }

//       // Sort positions to check consecutive differences
//       std::vector<std::pair<std::pair<uint32_t, uint32_t>, size_t>> sorted_pairs;
//       for (size_t i = 0; i < n; ++i) {
//         sorted_pairs.push_back({positions[i], i});
//       }
//       std::sort(sorted_pairs.begin(), sorted_pairs.end());

//       // Check consecutive positions after sorting
//       for (size_t i = 0; i < n - 1; ++i) {
//         if (sorted_pairs[i + 1].first.first - sorted_pairs[i].first.second - 1 > slop_) {
//           iters_[sorted_pairs[i + 1].second]->NextPosition();  // Advance right position when slop violated
//           return;
//         }
//       }
//     }
    
//     // No violations, advance first non-done iterator
//     for (size_t i = 0; i < n; ++i) {
//       if (!iters_[i]->DonePositions()) {
//         iters_[i]->NextPosition();
//         return;
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

//     bool valid = true;
//     if (in_order_) {
//       // For ordered case, check positions in sequence
//       for (size_t i = 0; i < n - 1 && valid; ++i) {
//         if (positions[i].first >= positions[i + 1].first) {
//           VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition order failed between "
//                                     << iters_[i]->CurrentKey()->Str() << " and "
//                                     << iters_[i + 1]->CurrentKey()->Str() << " at "
//                                     << positions[i].first << " and "
//                                     << positions[i + 1].first;
//           valid = false;
//         } else if (positions[i + 1].first - positions[i].second - 1 > slop_) {
//           VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition slop failed between "
//                                     << iters_[i]->CurrentKey()->Str() << " and "
//                                     << iters_[i + 1]->CurrentKey()->Str() << " by "
//                                     << (positions[i + 1].first - positions[i].second - 1)
//                                     << ", slop is " << slop_;
//           valid = false;
//         }
//       }
//     } else if (slop_ >= 0) {
//       // Check for same positions first
//       for (size_t i = 0; i < n - 1 && valid; ++i) {
//         for (size_t j = i + 1; j < n; ++j) {
//           if (positions[i].first == positions[j].first) {
//             valid = false;
//             break;
//           }
//         }
//       }

//       if (valid) {
//         // Check slop between consecutive positions after sorting
//         std::vector<std::pair<uint32_t, uint32_t>> sorted_pos = positions;
//         std::sort(sorted_pos.begin(), sorted_pos.end());
        
//         for (size_t i = 0; i < n - 1 && valid; ++i) {
//           if (sorted_pos[i + 1].first - sorted_pos[i].second - 1 > slop_) {
//             VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition slop failed between positions "
//                                     << sorted_pos[i].first << "," << sorted_pos[i].second 
//                                     << " and " << sorted_pos[i + 1].first << "," << sorted_pos[i + 1].second
//                                     << " by " << (sorted_pos[i + 1].first - sorted_pos[i].second - 1)
//                                     << ", slop is " << slop_;
//             valid = false;
//           }
//         }
//       }
//     }

//     if (valid) {
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



// Ideal, but did not work.

// bool ProximityIterator::NextPosition() {
//   VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition";
//   const size_t n = iters_.size();
//   std::vector<std::pair<uint32_t, uint32_t>> positions(n);

//   // Common check function used for both advance and validation
//   auto check_constraints = [&](const std::vector<std::pair<uint32_t, uint32_t>>& pos) 
//       -> std::optional<size_t> {
//     if (in_order_) {
//       for (size_t i = 0; i < n - 1; ++i) {
//         if (pos[i].first >= pos[i + 1].first) {
//           VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition order failed between "
//                                     << iters_[i]->CurrentKey()->Str() << " and "
//                                     << iters_[i + 1]->CurrentKey()->Str() << " at "
//                                     << pos[i].first << " and "
//                                     << pos[i + 1].first;
//           return i + 1;
//         }
//         if (pos[i + 1].first - pos[i].second - 1 > slop_) {
//           VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition slop failed between "
//                                     << iters_[i]->CurrentKey()->Str() << " and "
//                                     << iters_[i + 1]->CurrentKey()->Str() << " by "
//                                     << (pos[i + 1].first - pos[i].second - 1)
//                                     << ", slop is " << slop_;
//           return i + 1;
//         }
//       }
//     } else if (slop_ >= 0) {
//       std::vector<std::pair<uint32_t, uint32_t>> sorted_pos = pos;
//       std::sort(sorted_pos.begin(), sorted_pos.end());
      
//       for (size_t i = 0; i < n - 1; ++i) {
//         if (sorted_pos[i + 1].first - sorted_pos[i].second - 1 > slop_) {
//           // Find original index
//           for (size_t j = 0; j < n; ++j) {
//             if (pos[j] == sorted_pos[i]) {
//               return j;
//             }
//           }
//         }
//       }
//     }
//     return std::nullopt;
//   };

//   auto advance = [&]() {
//     auto iter_to_advance = check_constraints(positions);
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

//     auto violation = check_constraints(positions);
//     if (!violation.has_value()) {
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
