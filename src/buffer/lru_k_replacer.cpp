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

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  for (int i = 1; i <= static_cast<int>(replacer_size_); i++) {
    st_[i] = false;
  }
}

LRUKReplacer::~LRUKReplacer() {
  st_.clear();
  buf_.clear();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  const size_t inf = 0x3f3f3f3f;
  latch_.lock();
  size_t max_dis = 0;
  frame_id_t select_id = -1;
  for (int i = 1; i <= static_cast<int>(replacer_size_); i++) {
    if (st_[i]) {
      size_t dis;
      if (buf_[i].size() == k_) {
        dis = buf_[i].back() - buf_[i].front();
      } else {
        dis = inf;
      }
      if (select_id == -1 || max_dis < dis) {
        select_id = i;
        max_dis = dis;
      } else if (max_dis == inf && dis == inf) {
        if (!buf_[select_id].empty() && !buf_[i].empty() && buf_[select_id].front() > buf_[i].front()) {
          select_id = i;
        }
      }
    }
  }
  if (select_id == -1) {
    latch_.unlock();
    return false;
  }
  st_[select_id] = false;
  buf_[select_id].clear();
  curr_size_--;
  *frame_id = select_id;
  latch_.unlock();
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw std::exception();
  }
  latch_.lock();
  current_timestamp_++;
  if (buf_.count(frame_id) == 0) {
    buf_.insert({frame_id, std::deque<size_t>{}});
  }
  std::deque<size_t> &q = buf_[frame_id];
  if (q.size() == k_) {
    q.pop_front();
  }
  q.push_back(current_timestamp_);
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw std::exception();
  }
  latch_.lock();
  if (!st_[frame_id] && set_evictable) {
    curr_size_++;
  } else if (st_[frame_id] && !set_evictable) {
    curr_size_--;
  }
  st_[frame_id] = set_evictable;
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw std::exception();
  }
  if (buf_[frame_id].empty()) {
    return;
  }
  if (!st_[frame_id]) {
    throw std::exception();
  }
  latch_.lock();
  st_[frame_id] = false;
  if (buf_.count(frame_id) != 0) {
    buf_[frame_id].clear();
  }
  curr_size_--;
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
