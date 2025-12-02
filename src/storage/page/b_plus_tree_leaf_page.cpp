//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  // 先设置基础属性，最后设置页面类型
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);  // 最后设置页面类型
  
  // 初始化叶子页面特定成员
  next_page_id_ = INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { 
  return next_page_id_; 
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { 
  next_page_id_ = next_page_id; 
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

// 添加的方法实现
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) -> MappingType & {
  return array_[index];
}
/**
 * 查找键的插入位置，如果键已存在返回-1
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindKeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  int left = 0;
  int right = GetSize() - 1;
  
  // 二分查找插入位置
  while (left <= right) {
    int mid = left + (right - left) / 2;
    int cmp_result = comparator(array_[mid].first, key);
    
    if (cmp_result == 0) {
      // 键已存在
      return -1;
    } else if (cmp_result < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  
  // 返回插入位置
  return left;
}

/**
 * 插入键值对到叶子页面
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> bool {
  // 检查是否有空间
  if (GetSize() >= GetMaxSize()) {
    return false;
  }
  
  // 查找插入位置
  int insert_index = FindKeyIndex(key, comparator);
  if (insert_index == -1) {
    // 键已存在
    return false;
  }
  
  // 将后面的元素向后移动
  for (int i = GetSize(); i > insert_index; i--) {
    array_[i] = array_[i - 1];
  }
  
  // 插入新元素
  array_[insert_index] = MappingType(key, value);
  IncreaseSize(1);
  
  return true;
}

/**
 * 移动一半键值对到接收者页面
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *recipient) -> KeyType {
  int start_index = GetSize() / 2;
  int num_to_move = GetSize() - start_index;
  
  // 分裂键是新叶子节点的第一个键
  KeyType split_key = array_[start_index].first;
  
  // 移动元素到recipient
  for (int i = 0; i < num_to_move; i++) {
    recipient->array_[i] = array_[start_index + i];
  }
  
  // 更新大小
  IncreaseSize(-num_to_move);
  recipient->IncreaseSize(num_to_move);
  
  return split_key;
}

/**
 * 查找键的索引位置，如果不存在返回-1
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindKey(const KeyType &key, const KeyComparator &comparator) const -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (comparator(array_[i].first, key) == 0) {
      return i;
    }
  }
  return -1;
}

/**
 * 删除指定位置的键值对
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAt(int index) {
  // 检查索引有效性
  if (index < 0 || index >= GetSize()) {
    return;
  }
  
  // 将后面的元素向前移动
  for (int i = index; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  
  IncreaseSize(-1);
}
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub