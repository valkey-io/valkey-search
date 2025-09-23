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
    NextKey();
}

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
        // Using the intersection of field masks, check if any field is common.
        // The nested proximity iterator's NextPosition ensures that we are on a valid
        // combination of positions.
        CHECK(common_fields != 0) << "ProximityIterator::CurrentFieldMask - positions are not on the common fields in child iterators";
    }
    return common_fields;
}

bool ProximityIterator::NextKey() {
    VMSDK_LOG(WARNING, nullptr) << "PI::NextKey1";
    // On the second call onwards, advance any text iterators that are still sitting on the old key.
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
            current_field_mask_ = std::nullopt;
            // 2) Move to the next valid position combination across all text iterators.
            //    Exit, if no valid position combination key is found.
            if (NextPosition()) {
                return true;
            }
        }
		advance();
        // Otherwise, loop and try again.
    }
    VMSDK_LOG(WARNING, nullptr) << "PI::NextKey keys exhausted";
    current_key_ = nullptr;
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
    // 2) If everyone is already equal -> we found a common key
    if (min_key->Str() == max_key->Str()) {
        current_key_ = max_key;
        VMSDK_LOG(WARNING, nullptr) << "PI::FindCommonKey found common key " << current_key_->Str();
        return true;
    }
    // 3) Advance all iterators that are strictly behind the current max_key
    for (auto& iter : iters_) {
        // TODO: Replace this block with SeekForward on the Key.
        while (!iter->DoneKeys() && iter->CurrentKey()->Str() < max_key->Str()) {
            VMSDK_LOG(WARNING, nullptr)
                << "PI::FindCommonKey advancing child from " << iter->CurrentKey()->Str()
                << " toward " << max_key->Str();
            iter->NextKey();
        }
    }
    return false;
}

bool ProximityIterator::NextPosition() {
    VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition";
    const size_t n = iters_.size();
    std::vector<std::pair<uint32_t, uint32_t>> positions(n);
    auto advance_smallest = [&]() -> void {
        size_t advance_idx = 0;
        for (size_t i = 1; i < n; ++i) {
            if (positions[i].first < positions[advance_idx].first) {
                advance_idx = i;
            }
        }
        iters_[advance_idx]->NextPosition();
    };
    bool should_advance = false;
    // On a second call, we advance.
    if (current_start_pos_.has_value() && current_end_pos_.has_value()) {
        should_advance = true;
    }
    while (!DonePositions()) {
        VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition in loop";
        // Collect current positions (start, end) of the text iterators.
        for (size_t i = 0; i < n; ++i) {
            positions[i] = iters_[i]->CurrentPosition();
        }
        if (should_advance) {
            should_advance = false;
            advance_smallest();
        }
        // Check if current combination of positions satisfies the proximity constraints
        bool valid = true;
        // First, using the intersection of field masks, check if all terms are in the same field.
        uint64_t common_fields = iters_[0]->CurrentFieldMask();
        for (size_t i = 1; i < n && valid; ++i) {
            common_fields &= iters_[i]->CurrentFieldMask();
            if (common_fields == 0) {
                valid = false;
            }
        }
        // Second, check order/slop constraints using start and end positions.
        if (valid) {
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
            current_field_mask_ = common_fields;
            VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition returning as valid";
            return true;
        }
        advance_smallest();
    }
    VMSDK_LOG(WARNING, nullptr) << "PI::NextPosition positions exhausted";
    current_start_pos_ = std::nullopt;
    current_end_pos_ = std::nullopt;
    current_field_mask_ = std::nullopt;
    return false;
}

}  // namespace valkey_search::indexes::text
