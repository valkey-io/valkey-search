#include "proximity.h"

namespace valkey_search::indexes::text {

ProximityIterator::ProximityIterator(std::vector<std::unique_ptr<TextIterator>>&& iters,
                                     size_t slop,
                                     bool in_order,
                                     FieldMaskPredicate field_mask,
                                     const InternedStringSet* untracked_keys)
    : iters_(std::move(iters)),     // move the iterators in
      done_(false),                 // initially not done
      slop_(slop),
      in_order_(in_order),
      untracked_keys_(untracked_keys),
      current_key_(nullptr),        // no key yet
      current_pos_(0),              // start at 0
      field_mask_(field_mask)
{
    VMSDK_LOG(WARNING, nullptr) << "PI::init";
    VMSDK_LOG(WARNING, nullptr) << "iters_ size" << iters_.size();
    if (iters_.empty()) {
        VMSDK_LOG(WARNING, nullptr) << "PI::init done 1";
        done_ = true;
        return;
    }
    // initialize iterators to first key/position
    for (auto& iter : iters_) {
        if (iter->DoneKeys()) {
            done_ = true;
            VMSDK_LOG(WARNING, nullptr) << "PI::init done 2";
            return;
        }
        // Safety check on init.
        if (!iter->CurrentKey()) {  // no keys in this iterator
            done_ = true;
            VMSDK_LOG(WARNING, nullptr) << "PI::init done 3";
            return;
        }
    }
    // Prime first common key
    if (NextKey()) {
        done_ = false;
    } else {
        done_ = true;
        current_key_ = nullptr;
    }
}

void ProximityIterator::Next() {
}

bool ProximityIterator::NextKey() {
    VMSDK_LOG(WARNING, nullptr) << "PI::Next1";    
    for (;;) {
        if (done_) return false;
        // 1) Advance any child iterators that are still sitting on the old key.
        for (auto& c : iters_) {
            if (!c->DoneKeys() && c->CurrentKey() == current_key_) {
                c->NextKey();  // move any child iterators sitting at current_key_
            }
        }
        // 2) Align all children to the next common key
        if (!NextKeyMain()) {
            // NextKeyMain sets done_ on child exhaustion but be safe here too
            done_ = true;
            return false;
        }
        // For nested proximity, we need to check if the start and end positions match the slop and order.
        // 3) Try to find the first valid position in this key
        if (NextPosition()) {
            // Found a valid match; NextPosition sets current_key_ and current_pos_
            // If a child had been advanced and exhausted positions but still has
            // more keys, last_pos_exhausted_idx_ may be set; we'll handle on next call.
            return true;
        }
        // otherwise, loop and try again (defensive).
    }
    return false;
}
bool ProximityIterator::Done() const {
    VMSDK_LOG(WARNING, nullptr) << "PI::Done";   
    return done_;
}

bool ProximityIterator::DoneKeys() const {
    VMSDK_LOG(WARNING, nullptr) << "PI::DoneKeys";   
    return done_;
}

bool ProximityIterator::DonePositions() const {
    VMSDK_LOG(WARNING, nullptr) << "PI::DonePositions";   
    return done_;
}

const InternedStringPtr& ProximityIterator::CurrentKey() {
  VMSDK_LOG(WARNING, nullptr) << "PI::CurrentKey";
  return current_key_;
}

uint32_t ProximityIterator::CurrentPosition() {
  VMSDK_LOG(WARNING, nullptr) << "PI::CurrentPosition";
  return current_pos_;
}

// ---- Internal helpers ----

bool ProximityIterator::NextKeyMain() {
    VMSDK_LOG(WARNING, nullptr) << "PI::NextKey";

    if (iters_.empty()) {
        VMSDK_LOG(WARNING, nullptr) << "PI::NextKey Done 1";
        done_ = true;
        return false;
    }

    for (;;) {
        // 1) Validate children and compute min/max among current keys
        InternedStringPtr min_key = nullptr;
        InternedStringPtr max_key = nullptr;

        for (auto& iter : iters_) {
            // I suggest commenting this out
            // if (iter->DoneKeys()) {
            //     done_ = true;
            //     VMSDK_LOG(WARNING, nullptr) << "PI::NextKey child done -> exhausted";
            //     return false;
            // }
            auto k = iter->CurrentKey();
            if (!k) {
                done_ = true;
                VMSDK_LOG(WARNING, nullptr) << "PI::NextKey child CurrentKey() null -> exhausted";
                return false;
            }
            if (!min_key || k->Str() < min_key->Str()) min_key = k;
            if (!max_key || k->Str() > max_key->Str()) max_key = k;
        }

        // 2) If everyone is already equal -> we found a common key
        if (min_key->Str() == max_key->Str()) {
            current_key_ = max_key;
            VMSDK_LOG(WARNING, nullptr) << "PI::NextKey found common key " << current_key_->Str();
            return true;
        }

        // 3) Advance all iterators that are strictly behind the current max_key
        bool advanced_any = false;
        for (auto& iter : iters_) {
            while (!iter->DoneKeys() &&
                   iter->CurrentKey() &&
                   iter->CurrentKey()->Str() < max_key->Str()) {
                VMSDK_LOG(WARNING, nullptr)
                    << "PI::NextKey advancing child from " << iter->CurrentKey()->Str()
                    << " toward " << max_key->Str();
                if (!iter->NextKey()) {
                    done_ = true;
                    VMSDK_LOG(WARNING, nullptr) << "PI::NextKey child exhausted while advancing";
                    return false;
                }
                advanced_any = true;
            }
            // I suggest commenting this out
            // if (iter->DoneKeys()) {
            //     done_ = true;
            //     VMSDK_LOG(WARNING, nullptr) << "PI::NextKey child became done after advancing";
            //     return false;
            // }
        }

        // 4) Safety: if min != max but nothing advanced, bump the (previous) min forward
        //    to guarantee progress (should be rare; mostly defensive against buggy children).
        if (!advanced_any) {
            for (auto& iter : iters_) {
                if (iter->CurrentKey() && iter->CurrentKey()->Str() == min_key->Str()) {
                    VMSDK_LOG(WARNING, nullptr)
                        << "PI::NextKey safety advance from stuck min " << min_key->Str();
                    if (!iter->NextKey()) {
                        done_ = true;
                        VMSDK_LOG(WARNING, nullptr) << "PI::NextKey child exhausted in safety advance";
                        return false;
                    }
                    break;
                }
            }
        }

        // Loop continues: max_key may increase, or everyone may now meet at a common key.
    }
}

bool ProximityIterator::NextPosition() {
    VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition";

    if (done_) {  // <-- Check done upfront
        return false;
    }

    const size_t n = iters_.size();
    while (true) {
        VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition loop start";
        // // 1. Collect current positions
        // std::vector<uint64_t> positions(n);
        // for (size_t i = 0; i < n; ++i) {
        //     // if (iters_[i]->DonePositions()) {
        //     //     done_ = true;
        //     //     return false;
        //     // }
        //     positions[i] = iters_[i]->CurrentPosition();
        // }

        // // 2. Check left-to-right for slop / in-order violations
        // size_t failure_idx = n; // n means no violation
        // for (size_t i = 0; i + 1 < n; ++i) {
        //     uint64_t gap = positions[i + 1] - positions[i] - 1;
        //     if (gap > slop_ || (in_order_ && positions[i] >= positions[i + 1])) {
        //         failure_idx = i; // left term is lagging
        //         break;
        //     }
        // }

        // if (failure_idx == n) {
        //     // ✅ Found a valid combination
        //     current_pos_ = positions[0];
        //     current_key_ = iters_[0]->CurrentKey();

        //     // // --- Prepare for next combination ---
        //     // // Try to advance the **rightmost iterator** that can move.
        //     // // If none can move, lagging-advance in the next iteration will handle it.
        //     // for (ssize_t i = n - 1; i >= 0; --i) {
        //     //     if (iters_[i]->NextPosition()) {
        //     //         // Successfully advanced; reset all to the right
        //     //         for (size_t j = i + 1; j < n; ++j) {
        //     //             iters_[j]->ResetPositions();
        //     //         }
        //     //         break;
        //     //     } else if (i == 0) {
        //     //         // Leftmost iterator exhausted → no more combinations
        //     //         return true; // yield last combination now
        //     //     }
        //     // }

        //     return true;
        // }

        // // // 3. Violation occurred → advance the lagging iterator (left of failure)
        // // size_t advance_idx = failure_idx;
        // // while (true) {
        // //     if (!iters_[advance_idx]->NextPosition()) {
        // //         if (advance_idx == 0) {
        // //             done_ = true;
        // //             return false;
        // //         }
        // //         --advance_idx;
        // //     } else {
        // //         // Successfully advanced lagging iterator
        // //         break;
        // //     }
        // // }

        // // // 4. Reset all iterators to the right of the one we just advanced
        // // for (size_t i = advance_idx + 1; i < n; ++i) {
        // //     iters_[i]->ResetPositions();
        // // }

        // // Loop again to check new combination
        // // Done 
        // return false;

        // 1. Collect positions
        std::vector<uint64_t> positions;
        positions.reserve(iters_.size());
        for (auto& it : iters_) {
            // if (it->DonePositions()) return false;
            positions.push_back(it->CurrentPosition());
            VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition pos: " << it->CurrentPosition();
        }
        
        // 2. Ensure order if required
        // if (in_order_ && !std::is_sorted(positions.begin(), positions.end())) {
        //     // Advance smallest iterator and retry
        //     // TODO: Need to read this.
        //     size_t min_idx = std::distance(
        //         positions.begin(),
        //         std::min_element(positions.begin(), positions.end())
        //     );
        //     if (!iters_[min_idx]->NextPosition()) return false;
        //     continue;
        // }
        if (std::is_sorted(positions.begin(), positions.end())) {
            VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition is sorted";
            return true;
        }
        return false;
        // // 3. Compute max gap between adjacent positions
        // uint64_t max_gap = 0;
        // for (size_t i = 0; i + 1 < positions.size(); ++i) {
        //     uint64_t gap = positions[i+1] > positions[i] ? positions[i+1] - positions[i] - 1 : 0;
        //     if (gap > max_gap) max_gap = gap;
        // }

        // if (max_gap <= slop_) {
        //     // Found a match
        //     current_pos_ = positions.front();

        //     // Prepare for next candidate by advancing iterator with smallest position
        //     size_t min_idx = std::distance(
        //         positions.begin(),
        //         std::min_element(positions.begin(), positions.end())
        //     );
        //     iters_[min_idx]->NextPosition();
        //     return true;
        // }

        // // No match -> advance smallest and retry
        // size_t min_idx = std::distance(
        //     positions.begin(),
        //     std::min_element(positions.begin(), positions.end())
        // );
        // if (!iters_[min_idx]->NextPosition()) return false;
    }
}

// bool ProximityIterator::NextKey() {
//     VMSDK_LOG(WARNING, nullptr) << "PI::NextKey";

//     if (iters_.empty()) {
//         VMSDK_LOG(WARNING, nullptr) << "PI::NextKey Done 1";
//         done_ = true;  // <-- Set done flag here
//         return false;
//     }

//     while (!Done()) {
//         // 1. Find the minimum current key among all iterators
//         VMSDK_LOG(WARNING, nullptr) << "PI::NextKey in loop1. Starting 1";
//         InternedStringPtr min_key = nullptr;
//         for (auto& iter : iters_) {
//             VMSDK_LOG(WARNING, nullptr) << "PI::NextKey in loop11";
//             if (iter->Done()) {
//                 done_ = true;
//                 VMSDK_LOG(WARNING, nullptr) << "PI::NextKey Done 2";
//                 return false;
//             }
//             auto key = iter->CurrentKey();
//             if (!key) {
//                 done_ = true;
//                 VMSDK_LOG(WARNING, nullptr) << "PI::NextKey Done 3";
//                 return false;
//             }
//             if (!min_key || key->Str() < min_key->Str()) {
//                 min_key = key;
//             }
//         }
//         // 2. Advance iterators that are behind min_key
//         VMSDK_LOG(WARNING, nullptr) << "PI::NextKey check Starting 2";
//         bool all_equal = true;
//         for (auto& iter : iters_) {
//             VMSDK_LOG(WARNING, nullptr) << "PI::NextKey in loop12";
//             while (!iter->Done() && iter->CurrentKey()->Str() < min_key->Str()) {
//                 VMSDK_LOG(WARNING, nullptr) << "PI::NextKey in loop121";
//                 if (!iter->NextKey()) {
//                     done_ = true;
//                     VMSDK_LOG(WARNING, nullptr) << "PI::NextKey Done 4";
//                     return false;
//                 }
//             }
//             if (iter->Done()) {
//                 done_ = true;
//                 VMSDK_LOG(WARNING, nullptr) << "PI::NextKey Done 5";
//                 return false;
//             }
//             if (iter->CurrentKey()->Str() != min_key->Str()) {
//                 // This is an issue.
//                 // if (iter->CurrentKey()->Str() > min_key->Str()) {
//                 // }
//                 VMSDK_LOG(WARNING, nullptr) << "PI::NextKey Keys dont match" << "Key1: " << iter->CurrentKey()->Str() << " Key2: " << min_key->Str();
//                 all_equal = false;
//             }
//         }
//         // 3. If all iterators are at the same key, we found a common key
//         VMSDK_LOG(WARNING, nullptr) << "PI::NextKey Starting 3";
//         if (all_equal) {
//             current_key_ = min_key;
//             VMSDK_LOG(WARNING, nullptr) << "PI::NextKey Done with key " << current_key_->Str();
//             VMSDK_LOG(WARNING, nullptr) << "PI::NextKey Done 6";
//             return true;
//         }
//         // Otherwise, repeat loop to find next common key
//     }
//     return false;
// }

}  // namespace valkey_search::indexes::text
