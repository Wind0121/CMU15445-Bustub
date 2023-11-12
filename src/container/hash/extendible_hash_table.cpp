//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  std::shared_ptr<Bucket> new_bucket(new Bucket(bucket_size_, 0));
  dir_.push_back(new_bucket);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  latch_.lock();
  int id = IndexOf(key);
  auto pt = dir_[id];
  bool flag = pt->Find(key, value);
  latch_.unlock();
  return flag;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  latch_.lock();
  int id = IndexOf(key);
  auto pt = dir_[id];
  bool flag = pt->Remove(key);
  latch_.unlock();
  return flag;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  int id;
  latch_.lock();
  while (true) {
    id = IndexOf(key);
    if (!dir_[id]->IsFull()) {
      break;
    }
    if (global_depth_ == GetLocalDepthInternal(id)) {
      for (int i = 0; i < num_buckets_; i++) {
        auto tmp_pt = dir_[i];
        dir_.push_back(tmp_pt);
      }
      global_depth_++;
      num_buckets_ *= 2;
    }
    id = IndexOf(key);
    // 分裂bucket，并重新链接directory entry
    RedistributeBucket(dir_[id]);
  }
  dir_[id]->Insert(key, value);
  latch_.unlock();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::LowNNumber(int number, int n) -> int {
  int mask = (1 << n) - 1;
  return number & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  bucket->IncrementDepth();
  int depth = bucket->GetDepth();
  std::shared_ptr<Bucket> new_bucket(new Bucket(bucket_size_, depth));
  int pre_mask = LowNNumber(std::hash<K>()(bucket->GetItems().begin()->first), depth - 1);
  for (auto pt = bucket->GetItems().begin(); pt != bucket->GetItems().end();) {
    int now_mask = LowNNumber(std::hash<K>()(pt->first), depth);
    if (now_mask != pre_mask) {  // 说明要移到新的bucket中
      new_bucket->Insert(pt->first, pt->second);
      bucket->GetItems().erase(pt++);
    } else {
      pt++;
    }
  }
  for (int i = 0; i < num_buckets_; i++) {
    if (LowNNumber(i, depth - 1) == pre_mask && LowNNumber(i, depth) != pre_mask) {
      dir_[i] = new_bucket;
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  bool flag = false;
  for (auto pt = list_.begin(); pt != list_.end(); pt++) {
    if (pt->first == key) {
      flag = true;
      value = pt->second;
    }
  }
  return flag;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  bool flag = false;
  for (auto pt = list_.begin(); pt != list_.end(); pt++) {
    if (pt->first == key) {
      flag = true;
      list_.erase(pt);
      break;
    }
  }
  return flag;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto pt = list_.begin(); pt != list_.end(); pt++) {
    if (pt->first == key) {
      pt->second = value;
      return true;
    }
  }
  if (IsFull()) {
    return false;
  }
  list_.push_back({key, value});
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
