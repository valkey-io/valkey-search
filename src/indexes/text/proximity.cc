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

uint64_t ProximityIterator::GetFieldMask() const {
    // CHECK that all iterators have the same field mask and crash otherwise.
    uint64_t common_fields = iters_[0]->GetFieldMask();
    for (size_t i = 1; i < iters_.size(); ++i) {
        common_fields &= iters_[i]->GetFieldMask();
        // Using the intersection of field masks, check if any field is common
        if (common_fields == 0) {
            // valid = false;
            CHECK(false) << "ProximityIterator::GetFieldMask - Mismatched field masks in child iterators";
        }
    }
    return common_fields;
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
    if (done_) return false;

    const size_t n = iters_.size();
    
    while (true) {
        VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition in loop";
        // Check if any iterator is done with positions
        for (size_t i = 0; i < n; ++i) {
            if (iters_[i]->DonePositions()) {
                return false;
            }
        }

        // Collect current positions
        std::vector<uint32_t> positions(n);
        for (size_t i = 0; i < n; ++i) {
            positions[i] = iters_[i]->CurrentPosition();
        }

        // Check if current combination satisfies constraints
        bool valid = true;
        // First check all positions are in same field
        uint64_t common_fields = iters_[0]->GetFieldMask();
        for (size_t i = 1; i < n && valid; ++i) {
            common_fields &= iters_[i]->GetFieldMask();
            // Using the intersection of field masks, check if any field is common
            if (common_fields == 0) {
                valid = false;
            }
        }
        if (valid) {
            // Then check order/slop constraints
            for (size_t i = 0; i + 1 < n && valid; ++i) {
                if (in_order_ && positions[i] >= positions[i + 1]) {
                    valid = false;
                } else if (positions[i + 1] - positions[i] - 1 > slop_) {
                    valid = false;
                }
            }
        }
        if (valid) {
            current_pos_ = positions[0];
            current_key_ = iters_[0]->CurrentKey();
            VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition returning as valid";
            // Advance rightmost iterator for next call
            if (!iters_[n-1]->NextPosition()) {
                done_ = true; // No more combinations
            }
            return true;
        }

        // // Find which iterator to advance
        // size_t advance_idx = n - 1; // default to rightmost
        // for (size_t i = 0; i + 1 < n; ++i) {
        //     if (iters_[i]->GetFieldMask() != iters_[i + 1]->GetFieldMask()) {
        //         // Advance iterator with smaller field
        //         advance_idx = (iters_[i]->GetFieldMask() < iters_[i + 1]->GetFieldMask()) ? i : i + 1;
        //         break;
        //     }
        //     else if (in_order_ && positions[i] >= positions[i + 1]) {
        //         advance_idx = i + 1; // advance right iterator
        //         break;
        //     } else if (in_order_ && positions[i + 1] - positions[i] - 1 > slop_) {
        //         advance_idx = i; // advance left iterator when it's lagging
        //         break;
        //     }
        // }
        // Always advance iterator with smallest position
        size_t advance_idx = 0;
        for (size_t i = 1; i < n; ++i) {
            if (positions[i] < positions[advance_idx]) {
                advance_idx = i;
            }
        }


        // Advance the iterator
        if (!iters_[advance_idx]->NextPosition()) {
            VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition returning false";
            return false;
        }
    }
}

}  // namespace valkey_search::indexes::text
