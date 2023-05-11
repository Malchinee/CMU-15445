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
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <shared_mutex>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  std::shared_ptr<Bucket> bucket(new Bucket(bucket_size, 0));
  dir_.emplace_back(bucket);
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
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  return InsertInternal(key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::InsertInternal(const K &key, const V &value) {
  size_t index = IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_[index];
  bool res = bucket->Insert(key, value);
  if (res) {
    return;
  }
  // 如果桶的depth等于global_depth_; directory翻倍
  if (GetGlobalDepth() == bucket->GetDepth()) {
    global_depth_++;
    size_t capacity = dir_.size();
    dir_.resize(capacity * 2);
    for (size_t i = 0; i < capacity; i++) {
      dir_[i + capacity] = dir_[i];
    }
  }
  int new_depth = bucket->GetDepth() + 1;
  // base_mask和split_mask用来确定查分的bucket的位置
  size_t base_mask = (1 << bucket->GetDepth()) - 1;
  size_t split_mask = (1 << new_depth) - 1;

  // 两个需要查分的桶的指针
  auto first_bucket = std::make_shared<Bucket>(bucket_size_, new_depth);
  auto second_bucket = std::make_shared<Bucket>(bucket_size_, new_depth);

  size_t low_index = index & base_mask;
  for (const auto &ele : bucket->GetItems()) {
    size_t split_index = IndexOf(ele.first);
    if ((split_index & split_mask) == low_index) {
      first_bucket->GetItems().emplace_back(ele);
      continue;
    }
    second_bucket->GetItems().emplace_back(ele);
  }
  for (size_t i = 0; i < dir_.size(); i++) {
    if ((i & base_mask) == low_index) {
      if ((i & split_mask) == low_index) {
        dir_[i] = first_bucket;
        continue;
      }
      dir_[i] = second_bucket;
    }
  }
  num_buckets_++;
  return InsertInternal(key, value);
}
//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto &[k, v] : list_) {
    if (k == key) {
      value = v;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto ele = list_.begin(); ele != list_.end(); ele++) {
    if (ele->first == key) {
      list_.erase(ele);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto &[k, v] : list_) {
    if (k == key) {
      v = value;
      return true;
    }
  }
  if (IsFull()) {
    return false;
  }
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
