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
// 创建了一个初始的可扩展哈希表，它只有一个桶，全局深度为0，目录大小为1。
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) 
  
	: global_depth_(0), //全局深度，初始化为0。
// 确保初始目录有一个有效的桶，每个桶（bucket）可以容纳的键值对的最大数量为bucket_size
	bucket_size_(bucket_size), num_buckets_(1) {
// 这一行在目录（dir_）中插入一个共享指针，指向一个新创建的Bucket对象，0是桶的局部深度
  dir_.push_back(std::make_shared<Bucket>(bucket_size_, 0));
}

template <typename K, typename V>
// IndexOf函数用于计算键(key)对应的目录索引，核心映射函数，决定键存储在哪个目录项。
// 通过只改变全局深度，相同的键会映射到新的索引位置，这就是目录扩展时重新分配条目的原理
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
	// 创建掩码
  int mask = (1 << global_depth_) - 1;
	// 计算key的哈希值并应用掩码，只保留哈希值的低global_depth_位
	// 因为目录大小是2^global_depth_，索引范围是0到2^global_depth_ - 1。通过掩码操作，可以确保计算出的索引在这个范围内
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
// 获取当前全局深度，与GetGlobalDepthInternal()构成了典型的双重接口设计模式。避免死锁
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
	// 获取互斥锁latch，确保线程安全
  std::scoped_lock<std::mutex> lock(latch_);
	// 调用内部函数
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
// 类内部其他方法调用，已有锁保护，直接返回全局深度值
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
// 局部深度的双重接口，将目录索引传递给内部函数
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
	///检查索引是否在有效范围内
         if (dir_index < 0 || dir_index >= static_cast<int>(dir_.size()))  {
   	   return 0;
  	}
  ///防止空指针
 	 if (dir_[dir_index] == nullptr) {
   	   return 0;
  	}
	// 返回桶的局部深度值
       	return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
// 获得桶的数量，双重接口
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
// 返回桶的数量
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
// 查找键值对
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key);
	// 调用桶的Find方法进行实际查找
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
	  // 获取key对应的桶的指针
    auto bucket = dir_[index];
    
    // 调用桶的Insert方法尝试插入
    if (bucket->Insert(key, value)) {
      return; // 插入成功
    }


    // 桶已满，局部深度=全局深度，则需要分裂
    if (bucket->GetDepth() == global_depth_) {
      // 扩展目录
		// 获取目录大小
      size_t length = dir_.size();
      dir_.resize(length * 2);
		// 新目录复制旧目录
      for (size_t i = 0; i < length; i++) {
        dir_[i + length] = dir_[i];
      }
      global_depth_++;
    }

    // 调用IncrementDepth()，桶的局部深度++
    bucket->IncrementDepth();
    int local_depth = bucket->GetDepth();

    // 创建两个新桶
    auto bucket0 = std::make_shared<Bucket>(bucket_size_, local_depth);
    auto bucket1 = std::make_shared<Bucket>(bucket_size_, local_depth);

    // 重新分配原桶中的条目
	  // 遍历原桶中所有键值对
    for (const auto &[k, v] : bucket->GetItems()) {
		// 对每个键计算哈希值
      auto hash_val = std::hash<K>{}(k);
		// 检查哈希值的第(local_depth-1)
		// 如果为0 →放入bucket0
		//如果为1 →放入bucket1
      if ((hash_val >> (local_depth - 1)) & 1) {
        bucket1->Insert(k, v);
      } else {
        bucket0->Insert(k, v);
      }
    }

    // 更新目录指针
	  // 找到所有指向原桶的目录项，根据目录索引的第(local_depth-1)位决定指向哪个新桶
    for (size_t i = 0; i < dir_.size(); i++) {
      if (dir_[i] == bucket) {
        if ((i >> (local_depth - 1)) & 1) {
          dir_[i] = bucket1;
        } else {
          dir_[i] = bucket0;
        }
      }
    }
	// 更新桶数量
    num_buckets_++;

    // 重新尝试插入，因为新键可能仍然插入失败（如果新桶也满了）
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
// Bucket 类的构造函数
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
// 查找键值对
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
	// 使用迭代器遍历 list_ 中的所有键值对，删除并返回
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
  
  // 链表末尾构造键值对
  list_.emplace_back(key, value);
  return true;
}
// 维护页面ID到页面帧的映射
template class ExtendibleHashTable<page_id_t, Page *>;
// 快速找到页面在链表中的位置
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub

