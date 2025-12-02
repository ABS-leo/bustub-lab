//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { 
  return array_[index].second; 
}

/**
 * 插入键值对到内部页面
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> bool {
  // 检查是否有空间
  if (GetSize() >= GetMaxSize()) {
    return false;
  }
  
  // 特殊情况1：如果页面是空的（新创建的根节点）
  if (GetSize() == 0) {
    // 第一个元素：无效键 + 左子指针
    array_[0] = MappingType(KeyType(), value);
    SetSize(1);
    return true;
  }
  
  // 查找插入位置
  // 从索引1开始，因为索引0的键是无效的
  int insert_index = 1;
  
  // 找到第一个大于等于key的位置
  while (insert_index < GetSize() && comparator(array_[insert_index].first, key) < 0) {
    insert_index++;
  }
  
  // 检查是否重复键
  if (insert_index < GetSize() && comparator(array_[insert_index].first, key) == 0) {
    return false;  // 重复键
  }
  
  // 将insert_index及之后的元素向后移动一位
  // 注意：i > insert_index，因为我们要空出insert_index的位置
  for (int i = GetSize(); i > insert_index; i--) {
    array_[i] = array_[i - 1];
  }
  
  // // 在insert_index位置插入
  // if (insert_index == 1) {
  //   // 特殊情况：插入到最开头（比所有有效键都小）
  //   // array_[1] 的键是新的最小键，值是原来的最左子指针
  //   array_[1] = MappingType(key, array_[0].second);
    
  //   // 更新 array_[0] 的值为新的最左子指针
  //   array_[0].second = value;
  // } else {
  //   // 正常情况：在中间或末尾插入
  //   array_[insert_index] = MappingType(key, value);
  // }
  array_[insert_index] = MappingType(key, value);
  IncreaseSize(1);
  return true;
}

/**
 * 在指定值后插入新节点
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value) {
  // 找到old_value的位置
  int old_index = -1;
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == old_value) {
      old_index = i;
      break;
    }
  }

  if (old_index == -1) {
    return; // 没找到
  }

  // 在old_index+1的位置插入新节点
  int insert_index = old_index + 1;
  for (int i = GetSize(); i > insert_index; i--) {
    array_[i] = array_[i - 1];
  }

  array_[insert_index] = MappingType(new_key, new_value);
  IncreaseSize(1);
}

/**
 * 移动一半键值对到接收者页面
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *recipient) -> KeyType {
  // 计算分裂点
  int start_index = std::max(1, GetSize() / 2);
  int num_to_move = GetSize() - start_index;
  
  // 分裂键是原节点中间位置的键（将被提升到父节点）
  // 注意：对于内部节点，array_[0].first 是无效的
  KeyType split_key = array_[start_index].first;
  
  // 移动元素到recipient
  for (int i = 0; i < num_to_move; i++) {
    recipient->array_[i] = array_[start_index + i];
  }
  
  // 确保recipient的第一个键是无效的
  recipient->array_[0].first = KeyType();
  
  // 更新大小
  IncreaseSize(-num_to_move);
  recipient->IncreaseSize(num_to_move);
  
  return split_key;
}

/**
 * 填充新根节点
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &left_value, const KeyType &key, const ValueType &right_value) {
  // 第一个元素：key无效，value为left_child
  array_[0] = MappingType(KeyType(), left_value);
  // 第二个元素：key为传入的key，value为right_child  
  array_[1] = MappingType(key, right_value);
  SetSize(2);
}

/**
 * 查找值的索引位置
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindValue(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

/**
 * 删除指定位置的键值对
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAt(int index) {
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

template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub