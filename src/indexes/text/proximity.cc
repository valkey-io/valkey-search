#include "proximity.h"

namespace valkey_search::indexes::text {

// TODO: Each loop needs to be smart and safe and always have a logicl exit path.
// For "recursive like" loops, we need a limit on max iterations.

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
      current_start_pos_(std::nullopt),
      current_end_pos_(std::nullopt),
      current_field_mask_(std::nullopt),
      field_mask_(field_mask)
{
    VMSDK_LOG(WARNING, nullptr) << "PI::init";
    VMSDK_LOG(WARNING, nullptr) << "iters_ size" << iters_.size();
    if (iters_.empty()) {
        VMSDK_LOG(WARNING, nullptr) << "PI::init done 1";
        done_ = true;
        return;
    }
    // Prime iterators to the first common key and valid position combo
    if (NextKey()) {
        done_ = false;
    } else {
        done_ = true;
        current_key_ = nullptr;
    }
}

bool ProximityIterator::NextKey() {
    VMSDK_LOG(WARNING, nullptr) << "PI::NextKey1";
    while (!done_) {
        // 1) Advance any child iterators that are still sitting on the old key.
        for (auto& c : iters_) {
            if (!c->DoneKeys() && c->CurrentKey() == current_key_) {
                c->NextKey();  // move any child iterators sitting at current_key_
            }
        }
        // 2) Find the next common key amongst all text iterators
        // This does not shift to the next common key on success. It is handled in (1).
        if (!NextKeyMain()) {
            return false;
        }
        // 3) Find the next common position combination across all text iterators.
        //  This shifts to the next valid position combo on success.
        if (NextPosition()) {
            return true;
        }
        // otherwise, loop and try again (defensive).
    }
    return false;
}

// TODO: This API works fine currently when there are no nested proximity iterators.
// When there are nested operations, we need the NextKeyMain to accurately be able to know
// when no more common keys exist across iterators. 
bool ProximityIterator::DoneKeys() const {
    VMSDK_LOG(WARNING, nullptr) << "PI::DoneKeys";   
    return done_;
}

// TODO: This API works fine currently when there are no nested proximity iterators.
// When there are nested operations, we need the NextPosition to accurately be able to know
// when no more valid position combinations across iterators. 
bool ProximityIterator::DonePositions() const {
    VMSDK_LOG(WARNING, nullptr) << "PI::DonePositions";   
    return done_;
}

const InternedStringPtr& ProximityIterator::CurrentKey() {
  VMSDK_LOG(WARNING, nullptr) << "PI::CurrentKey";
  CHECK(current_key_ != nullptr);
  return current_key_;
}

std::pair<uint32_t, uint32_t> ProximityIterator::CurrentPosition() {
  VMSDK_LOG(WARNING, nullptr) << "PI::CurrentPosition";
  CHECK(current_start_pos_.has_value() && current_end_pos_.has_value());
  return std::make_pair(current_start_pos_.value(), current_end_pos_.value());
}

uint64_t ProximityIterator::CurrentFieldMask() const {
    // CHECK that all iterators have the same field mask and crash otherwise.
    uint64_t common_fields = iters_[0]->CurrentFieldMask();
    for (size_t i = 1; i < iters_.size(); ++i) {
        common_fields &= iters_[i]->CurrentFieldMask();
        // Using the intersection of field masks, check if any field is common
        // The nested proximity iterator's NextPosition should ensure that we are on a valid
        // combination of positions.
        CHECK(common_fields != 0) << "ProximityIterator::CurrentFieldMask - No common fields in child iterators";
    }
    return common_fields;
}

bool ProximityIterator::NextKeyMain() {
    VMSDK_LOG(WARNING, nullptr) << "PI::NextKeyMain";
    while (!done_) {
        VMSDK_LOG(WARNING, nullptr) << "PI::NextKeyMain - in loop";
        // 1) Validate children and compute min/max among current keys
        InternedStringPtr min_key = nullptr;
        InternedStringPtr max_key = nullptr;
        for (auto& iter : iters_) {
            if (iter->DoneKeys()) {
                done_ = true;
                VMSDK_LOG(WARNING, nullptr) << "PI::NextKey child done -> exhausted";
                return false;
            }
            auto k = iter->CurrentKey();
            if (!min_key || k->Str() < min_key->Str()) min_key = k;
            if (!max_key || k->Str() > max_key->Str()) max_key = k;
        }
        // 2) If everyone is already equal -> we found a common key
        if (min_key->Str() == max_key->Str()) {
            current_key_ = max_key;
            VMSDK_LOG(WARNING, nullptr) << "PI::NextKeyMain found common key " << current_key_->Str();
            return true;
        }
        // 3) Advance all iterators that are strictly behind the current max_key
        bool advanced_any = false;
        for (auto& iter : iters_) {
            // TODO: Replace this block with SeekForward on the Key.
            while (!iter->DoneKeys() && iter->CurrentKey()->Str() < max_key->Str()) {
                VMSDK_LOG(WARNING, nullptr)
                    << "PI::NextKeyMain advancing child from " << iter->CurrentKey()->Str()
                    << " toward " << max_key->Str();
                if (!iter->NextKey()) {
                    done_ = true;
                    VMSDK_LOG(WARNING, nullptr) << "PI::NextKeyMain child exhausted while advancing";
                    return false;
                }
                advanced_any = true;
            }
        }
        // Loop continues: max_key may increase, or everyone may now meet at a common key.
    }
    return false;
}

bool ProximityIterator::NextPosition() {
    VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition";
    if (done_) return false;
    const size_t n = iters_.size();
    while (!done_) {
        VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition in loop";
        // Check if any iterator is done with positions
        for (size_t i = 0; i < n; ++i) {
            if (iters_[i]->DonePositions()) {
                return false;
            }
        }
        // Collect current positions (start, end)
        std::vector<std::pair<uint32_t, uint32_t>> positions(n);
        for (size_t i = 0; i < n; ++i) {
            positions[i] = iters_[i]->CurrentPosition();
        }
        // Check if current combination satisfies constraints
        bool valid = true;
        // First check all positions are in same field
        uint64_t common_fields = iters_[0]->CurrentFieldMask();
        for (size_t i = 1; i < n && valid; ++i) {
            common_fields &= iters_[i]->CurrentFieldMask();
            // Using the intersection of field masks, check if any field is common
            if (common_fields == 0) {
                valid = false;
            }
        }
        if (valid) {
            // Then check order/slop constraints using start and end positions
            for (size_t i = 0; i + 1 < n && valid; ++i) {
                if (in_order_ && positions[i].first >= positions[i + 1].first) {
                    valid = false;
                } else if (positions[i + 1].first - positions[i].second - 1 > slop_) {
                    valid = false;
                }
            }
        }
        if (valid) {
            current_start_pos_ = positions[0].first;
            current_end_pos_ = positions[n-1].second;
            VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition returning as valid";
            if (!iters_[n-1]->NextPosition()) {
                // done_ = true;
                // Think of what to do here.
            }
            return true;
        }
        // Always advance iterator with smallest position
        size_t advance_idx = 0;
        for (size_t i = 1; i < n; ++i) {
            if (positions[i].first < positions[advance_idx].first) {
                advance_idx = i;
            }
        }
        // Advance the iterator
        // TODO: Use the seek forward capability to seek to the required position
        if (!iters_[advance_idx]->NextPosition()) {
            VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition returning false";
            return false;
        }
    }
    return false;
}

}  // namespace valkey_search::indexes::text
