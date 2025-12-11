/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/rax_wrapper.h"

#include <cerrno>
#include <cstring>

#include "src/indexes/text/posting.h"

namespace valkey_search::indexes::text {

namespace {

// C-compatible callback wrapper
// We can't pass the mutation callback closure directly to C APIs, so we wrap it
// in a C-style function and pass the closure as opaque caller context.
extern "C" void* MutateCallbackWrapper(void* current, void* caller_context) {
  auto* fn = static_cast<absl::FunctionRef<void*(void*)>*>(caller_context);
  return (*fn)(current);
}

}  // namespace


// Constructor
Rax::Rax(void (*free_callback)(void*)) 
    : rax_(raxNew()), free_callback_(free_callback) {
  CHECK(rax_ != nullptr) << "Failed to create rax tree";
}

// Destructor
Rax::~Rax() {
  if (rax_) {
    raxFreeWithCallback(rax_, free_callback_);
    rax_ = nullptr;
  }
}

// Move constructor
Rax::Rax(Rax&& other) noexcept 
    : rax_(other.rax_), free_callback_(other.free_callback_) {
  other.rax_ = nullptr;
  other.free_callback_ = nullptr;
}

// Move assignment
Rax& Rax::operator=(Rax&& other) noexcept {
  if (this != &other) {
    if (rax_) {
      raxFreeWithCallback(rax_, free_callback_);
    }
    rax_ = other.rax_;
    free_callback_ = other.free_callback_;
    other.rax_ = nullptr;
    other.free_callback_ = nullptr;
  }
  return *this;
}

void Rax::MutateTarget(absl::string_view word,
                       absl::FunctionRef<void*(void*)> mutate) {
  CHECK(!word.empty()) << "Can't mutate the target for an empty word";

  unsigned char* c_word = const_cast<unsigned char*>(
      reinterpret_cast<const unsigned char*>(word.data()));
  void* opaque_callback = reinterpret_cast<void*>(&mutate);
  int res = raxMutate(rax_, c_word, word.size(), MutateCallbackWrapper, opaque_callback);
  CHECK(res) << "Rax mutation failed for word: " << word << ", errno: " << errno << " (" << strerror(errno) << ")";
}

size_t Rax::GetTotalWordCount() const { return raxSize(rax_); }

size_t Rax::GetWordCount(absl::string_view prefix) const {
  // TODO: Implement word counting
  return 0;
}

size_t Rax::GetLongestWord() const {
  // TODO: Implement longest word calculation
  return 0;
}

bool Rax::IsValid() const { 
  return rax_ != nullptr && raxSize(rax_) > 0; 
}

Rax::WordIterator Rax::GetWordIterator(absl::string_view prefix) const {
  return WordIterator(rax_, prefix);
}

/*** WordIterator ***/

Rax::WordIterator::WordIterator(rax* rax, absl::string_view prefix)
    : prefix_(prefix) {
  raxStart(&iter_, rax);

  // Seek to prefix with ">=" operator (works for empty prefix too)
  int seek_result = raxSeek(&iter_, ">=",
                            const_cast<unsigned char*>(
                                reinterpret_cast<const unsigned char*>(prefix.data())),
                            prefix.size());
  
  // After raxSeek, we need to call raxNext to actually position at the first data node
  // raxSeek positions the iterator in the tree but doesn't guarantee we're at a data node
  valid_ = seek_result && raxNext(&iter_);

  // Check if we're still in the prefix range
  if (valid_ && !raxEOF(&iter_)) {
    absl::string_view current_key(reinterpret_cast<const char*>(iter_.key),
                                  iter_.key_len);
    if (!current_key.starts_with(prefix)) {
      valid_ = false;
    }
  } else {
    valid_ = false;
  }
}

Rax::WordIterator::~WordIterator() { raxStop(&iter_); }

Rax::WordIterator::WordIterator(WordIterator&& other) noexcept
    : iter_(other.iter_),
      prefix_(std::move(other.prefix_)),
      valid_(other.valid_) {
  // Invalidate the source iterator
  other.valid_ = false;
}

typename Rax::WordIterator& Rax::WordIterator::operator=(
    WordIterator&& other) noexcept {
  if (this != &other) {
    raxStop(&iter_);
    iter_ = other.iter_;
    prefix_ = std::move(other.prefix_);
    valid_ = other.valid_;
    other.valid_ = false;
  }
  return *this;
}

bool Rax::WordIterator::Done() const { 
  return !valid_ || raxEOF(const_cast<raxIterator*>(&iter_)); 
}

void Rax::WordIterator::Next() {
  if (Done()) return;

  if (!raxNext(&iter_)) {
    valid_ = false;
    return;
  }

  // Check if still within prefix
  if (!raxEOF(&iter_)) {
    absl::string_view current_key(reinterpret_cast<const char*>(iter_.key),
                                  iter_.key_len);
    if (!current_key.starts_with(prefix_)) {
      valid_ = false;
    }
  } else {
    valid_ = false;
  }
}

bool Rax::WordIterator::SeekForward(absl::string_view word) {
  if (Done()) return false;

  // Check if word matches prefix
  if (!word.starts_with(prefix_)) {
    valid_ = false;
    return false;
  }

  // Seek to the word
  if (!raxSeek(&iter_, ">=",
               const_cast<unsigned char*>(
                   reinterpret_cast<const unsigned char*>(word.data())),
               word.size())) {
    valid_ = false;
    return false;
  }

  if (raxEOF(&iter_)) {
    valid_ = false;
    return false;
  }

  // Check if we're still in prefix range
  absl::string_view current_key(reinterpret_cast<const char*>(iter_.key),
                                iter_.key_len);
  if (!current_key.starts_with(prefix_)) {
    valid_ = false;
    return false;
  }

  // Return true if exact match, false if greater
  return current_key == word;
}

absl::string_view Rax::WordIterator::GetWord() const {
  CHECK(!Done()) << "Cannot get word from invalid iterator";
  return absl::string_view(reinterpret_cast<const char*>(iter_.key),
                           iter_.key_len);
}

void* Rax::WordIterator::GetTarget() const {
  CHECK(!Done()) << "Cannot get target from invalid iterator";
  return iter_.data;
}

InvasivePtr<Postings> Rax::WordIterator::GetPostingsTarget() const {
  return InvasivePtr<Postings>::CopyRaw(
      static_cast<InvasivePtrRaw<Postings>>(GetTarget()));
}

/*** PathIterator ***/

bool Rax::PathIterator::Done() const { throw std::logic_error("TODO"); }

bool Rax::PathIterator::IsWord() const { throw std::logic_error("TODO"); }

void Rax::PathIterator::Next() { throw std::logic_error("TODO"); }

bool Rax::PathIterator::SeekForward(char target) {
  throw std::logic_error("TODO");
}

bool Rax::PathIterator::CanDescend() const { throw std::logic_error("TODO"); }

typename Rax::PathIterator Rax::PathIterator::DescendNew() const {
  throw std::logic_error("TODO");
}

absl::string_view Rax::PathIterator::GetPath() {
  throw std::logic_error("TODO");
}

const void* Rax::PathIterator::GetTarget() const {
  throw std::logic_error("TODO");
}

void Rax::PathIterator::Defrag() { throw std::logic_error("TODO"); }

}  // namespace valkey_search::indexes::text
