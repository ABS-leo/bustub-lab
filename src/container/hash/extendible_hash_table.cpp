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

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) 
  
	: global_depth_(0),
	bucket_size_(bucket_size), num_buckets_(1) {
  // 确保初始目录有一个有效的桶
  dir_.push_back(std::make_shared<Bucket>(bucket_size_, 0));
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
        if (dir_index < 0 || dir_index >= static_cast<int>(dir_.size())) {
   	   return 0;
  	}
  
 	 if (dir_[dir_index] == nullptr) {
   	   return 0;
  	}
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
  auto index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key);
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  
  while (true) {
    auto index = IndexOf(key);
    auto bucket = dir_[index];
    
    // 尝试插入
    if (bucket->Insert(key, value)) {
      return; // 插入成功
    }


    // 桶已满，需要分裂
    if (bucket->GetDepth() == global_depth_) {
      // 扩展目录
      size_t length = dir_.size();
      dir_.resize(length * 2);
      for (size_t i = 0; i < length; i++) {
        dir_[i + length] = dir_[i];
      }
      global_depth_++;
    }

    // 增加桶的局部深度
    bucket->IncrementDepth();
    int local_depth = bucket->GetDepth();

    // 创建两个新桶
    auto bucket0 = std::make_shared<Bucket>(bucket_size_, local_depth);
    auto bucket1 = std::make_shared<Bucket>(bucket_size_, local_depth);

    // 重新分配原桶中的条目
    for (const auto &[k, v] : bucket->GetItems()) {
      auto hash_val = std::hash<K>{}(k);
      if ((hash_val >> (local_depth - 1)) & 1) {
        bucket1->Insert(k, v);
      } else {
        bucket0->Insert(k, v);
      }
    }

    // 更新目录指针
    for (size_t i = 0; i < dir_.size(); i++) {
      if (dir_[i] == bucket) {
        if ((i >> (local_depth - 1)) & 1) {
          dir_[i] = bucket1;
        } else {
          dir_[i] = bucket0;
        }
      }
    }

    num_buckets_++;

    // 重新尝试插入
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &[k, v] : list_) {
    if (k == key) {
      value = v;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // 如果键已存在，更新值
  for (auto &[k, v] : list_) {
    if (k == key) {
      v = value;
      return true;
    }
  }
  
  // 如果桶已满，插入失败
  if (IsFull()) {
    return false;
  }
  
  // 插入新键值对
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
