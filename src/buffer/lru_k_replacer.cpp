//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

#include <algorithm>
#include <limits>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

LRUKReplacer::~LRUKReplacer() = default;

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock lock(latch_);

  if (curr_size_ == 0) {
    return false;
  }

  frame_id_t candidate = -1;
  size_t max_k_distance = 0;
  size_t earliest_timestamp = std::numeric_limits<size_t>::max();

  for (const auto &[fid, frame_info] : frame_table_) {
    if (!frame_info.is_evictable_) {
      continue;
    }

    size_t k_distance;
    size_t frame_earliest;

    if (frame_info.history_.size() < k_) {
      // Frame has less than K accesses - use +inf
      k_distance = std::numeric_limits<size_t>::max();
      frame_earliest = frame_info.history_.front();  // Use first access time
    } else {
      // Frame has K or more accesses - use Kth previous access time
      k_distance = current_timestamp_ - frame_info.history_.front();
      frame_earliest = frame_info.history_.front();
    }

    // Select frame with largest k-distance
    // If k-distance is equal, select the one with earliest timestamp
    if (k_distance > max_k_distance || (k_distance == max_k_distance && frame_earliest < earliest_timestamp)) {
      candidate = fid;
      max_k_distance = k_distance;
      earliest_timestamp = frame_earliest;
    }
  }

  if (candidate == -1) {
    return false;
  }

  *frame_id = candidate;
  frame_table_.erase(candidate);
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);

  if (static_cast<size_t>(frame_id) > replacer_size_) {
    return;
  }

  current_timestamp_++;

  // Create frame info if it doesn't exist
  if (frame_table_.find(frame_id) == frame_table_.end()) {
    frame_table_[frame_id] = FrameInfo();
  }

  auto &frame_info = frame_table_[frame_id];

  // Add current timestamp to history
  frame_info.history_.push_back(current_timestamp_);

  // Maintain only K most recent accesses (keep the oldest K accesses)
  if (frame_info.history_.size() > k_) {
    frame_info.history_.pop_front();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock lock(latch_);

  auto it = frame_table_.find(frame_id);
  if (it == frame_table_.end()) {
    return;
  }

  auto &frame_info = it->second;

  if (frame_info.is_evictable_ && !set_evictable) {
    curr_size_--;
  } else if (!frame_info.is_evictable_ && set_evictable) {
    curr_size_++;
  }

  frame_info.is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);

  auto it = frame_table_.find(frame_id);
  if (it == frame_table_.end()) {
    return;
  }

  if (!it->second.is_evictable_) {
    return;  // Should throw exception, but for compatibility return silently
  }

  frame_table_.erase(it);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock lock(latch_);
  return curr_size_;
}

}  // namespace bustub
