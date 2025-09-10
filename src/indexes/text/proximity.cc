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
        if (iter->Done()) {
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
    if (done_) return false;
    VMSDK_LOG(WARNING, nullptr) << "PI::Next2";   
    // We are currently sitting on a valid key. Try to advance past it.
    for (auto& c : iters_) {
        if (!c->Done() && c->CurrentKey() == current_key_) {
            c->NextKey();  // move any child iterators sitting at current_key_
        }
    }
    // TODO: Change NextKey with the function that operates on words and uses position context.
    if (!NextKeyMain()) {
        done_ = true;
        return false;
    }
    return true;
}

bool ProximityIterator::Done() const {
    VMSDK_LOG(WARNING, nullptr) << "PI::Done";   
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

void ProximityIterator::SyncCurrentKey() {
  // Since all children are aligned to the same doc key:
  VMSDK_LOG(WARNING, nullptr) << "PI::SyncCurrentKey";   
  current_key_ = iters_[0]->CurrentKey();
}

// Note: Does not yet handle looping over words. Change the top level IF statement to a loop for words
bool ProximityIterator::MatchPositions() {
  VMSDK_LOG(WARNING, nullptr) << "PI::MatchPositions";   
  // Keep looping until we find a valid proximity match OR run out of keys.
  if (!Done()) {
    // 1. Align all children to the same key
    if (NextKey()) {
        SyncCurrentKey();
        return true;
    }
    

    // 2. Scan positions in this key to see if slop/in-order condition met
    // if (NextPosition()) return true;

    // 3. If no position match, advance to next key and repeat
  }
  return false;
}

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
            if (iter->Done()) {
                done_ = true;
                VMSDK_LOG(WARNING, nullptr) << "PI::NextKey child done -> exhausted";
                return false;
            }
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
            while (!iter->Done() &&
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
            if (iter->Done()) {
                done_ = true;
                VMSDK_LOG(WARNING, nullptr) << "PI::NextKey child became done after advancing";
                return false;
            }
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

    if (iters_.empty() || done_) {  // <-- Check done upfront
        done_ = true;
        return false;
    }

    while (true) {
        // 1. Stop if any iterator is done
        for (auto& uptr : iters_) {
            if (uptr->Done()) { // Done needs to take into account the position iterator in all TextIterators.
                done_ = true;  // <-- set done when a child iterator exhausted
                return false;
            }
        }

        // 2. Collect current positions
        std::vector<uint64_t> positions;
        positions.reserve(iters_.size());
        for (auto& uptr : iters_) {
            positions.push_back(uptr->CurrentPosition());
        }

        // 3. Check span
        auto minmax = std::minmax_element(positions.begin(), positions.end());
        uint64_t span = *minmax.second - *minmax.first;

        bool ok = (span <= slop_);
        if (in_order_) {
            ok = ok && std::is_sorted(positions.begin(), positions.end());
        }

        if (ok) {
            // Found a match
            current_pos_ = *minmax.first;
            // current_word_ = iters_[0]->CurrentWord();  // assume all aligned in same key
            current_key_ = iters_[0]->CurrentKey();

            // Advance all positions for next call, but stop if exhausted
            bool any_exhausted = false;
            for (auto& uptr : iters_) {
                if (!uptr->NextPosition()) {  // <-- use return value to detect exhaustion
                    any_exhausted = true;
                }
            }
            if (any_exhausted) {  // <-- if any child exhausted, we are done
                done_ = true;
            }
            return true;
        }

        // 4. Advance the iterator with smallest position
        size_t min_idx = std::distance(
            positions.begin(),
            std::min_element(positions.begin(), positions.end())
        );
        if (!iters_[min_idx]->NextPosition()) {  // <-- check if child exhausted
            done_ = true;  // <-- propagate done
            return false;
        }
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
