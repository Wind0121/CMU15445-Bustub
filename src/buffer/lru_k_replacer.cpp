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
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  for (int i = 0; i < static_cast<int>(replacer_size_); i++) {
    st_[i] = false;
  }
}

LRUKReplacer::~LRUKReplacer() {
  st_.clear();
  buf_.clear();
}

auto LRUKReplacer::Judge(frame_id_t s, frame_id_t t) -> bool {
  if (buf_[s].size() < k_ && buf_[t].size() == k_) {
    return true;
  }
  if (buf_[s].size() == k_ && buf_[t].size() < k_) {
    return false;
  }
  return buf_[s].front() < buf_[t].front();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t select_id = -1;
  for (auto &pt : buf_) {
    if (st_[pt.first]) {
      if (select_id == -1 || Judge(pt.first, select_id)) {
        select_id = pt.first;
      }
    }
  }
  if (select_id == -1) {
    return false;
  }
  st_[select_id] = false;
  buf_[select_id].clear();
  curr_size_--;
  *frame_id = select_id;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw std::exception();
  }
  current_timestamp_++;
  if (buf_.count(frame_id) == 0) {
    buf_.insert({frame_id, std::deque<size_t>{}});
  }
  std::deque<size_t> &q = buf_[frame_id];
  if (q.size() == k_) {
    q.pop_front();
  }
  q.push_back(current_timestamp_);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw std::exception();
  }
  if (buf_[frame_id].empty()) {
    return;
  }
  if (!st_[frame_id] && set_evictable) {
    curr_size_++;
  }
  if (st_[frame_id] && !set_evictable) {
    curr_size_--;
  }
  st_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw std::exception();
  }
  if (buf_[frame_id].empty()) {
    return;
  }
  if (!st_[frame_id]) {
    throw std::exception();
  }
  st_[frame_id] = false;
  buf_[frame_id].clear();
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
