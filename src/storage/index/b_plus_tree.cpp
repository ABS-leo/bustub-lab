#include <string>
#include <fstream>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {

  INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { 
  return root_page_id_ == INVALID_PAGE_ID; 
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // 1. 获取全局锁（整个函数都被保护）
  std::lock_guard<std::mutex> lock(root_latch_);

  if (IsEmpty()) {
    return false;
  }

  auto *leaf_page = FindLeafPage(key);
  if (leaf_page == nullptr) {
    return false;
  }

  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  
  // 线性搜索查找key
  for (int i = 0; i < leaf_node->GetSize(); i++) {
    if (comparator_(leaf_node->KeyAt(i), key) == 0) {
      result->push_back(leaf_node->ValueAt(i));
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
      return true;
    }
  }

  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // 1. 获取全局锁
  std::lock_guard<std::mutex> lock(root_latch_);
  
  if (IsEmpty()) {
    bool result = StartNewTree(key, value);
    return result;
  }
  
  bool result = InsertIntoLeaf(key, value, transaction);
  return result;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // 1. 获取全局锁
  std::lock_guard<std::mutex> lock(root_latch_);

  if (IsEmpty()) {
    return;
  }

  auto *leaf_page = FindLeafPage(key);
  if (leaf_page == nullptr) {
    return;
  }

  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  
  // 在叶子节点中查找key的位置
  int index = -1;
  for (int i = 0; i < leaf_node->GetSize(); i++) {
    if (comparator_(leaf_node->KeyAt(i), key) == 0) {
      index = i;
      break;
    }
  }
  
  // 如果key不存在，直接返回
  if (index == -1) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return;
  }

  // 从叶子节点中删除key
  leaf_node->RemoveAt(index);
  
  // 检查是否需要合并或重分配
  if (leaf_node->GetSize() < leaf_node->GetMinSize()) {
    CoalesceOrRedistribute(leaf_node, transaction);
  } else {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // 1. 获取全局锁
  std::lock_guard<std::mutex> lock(root_latch_);

  
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  
  // 找到最左边的叶子页面
  page_id_t current_page_id = root_page_id_;
  auto *current_page = buffer_pool_manager_->FetchPage(current_page_id);
  auto *current_node = reinterpret_cast<BPlusTreePage *>(current_page->GetData());
  
  // 一直向左下遍历直到叶子节点
  while (!current_node->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(current_node);
    page_id_t next_page_id = internal->ValueAt(0);  // 第一个子节点是最左边的
    
    buffer_pool_manager_->UnpinPage(current_page_id, false);
    current_page_id = next_page_id;
    current_page = buffer_pool_manager_->FetchPage(current_page_id);
    current_node = reinterpret_cast<BPlusTreePage *>(current_page->GetData());
  }
  
  auto *leaf = reinterpret_cast<LeafPage *>(current_node);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf, 0);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // 1. 获取全局锁
  std::lock_guard<std::mutex> lock(root_latch_);

  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  
  // 找到包含该key的叶子页面
  auto *leaf_page = FindLeafPage(key);
  if (leaf_page == nullptr) {
    return INDEXITERATOR_TYPE();
  }
  
  auto *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  
  // 在叶子页面中查找key的位置
  int index = 0;
  while (index < leaf->GetSize() && comparator_(leaf->KeyAt(index), key) < 0) {
    index++;
  }
  
  // 这里我们不再unpin页面，因为迭代器会管理页面的生命周期
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf, index);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  // 返回一个空的迭代器（表示结束）
  return INDEXITERATOR_TYPE();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { 
  return root_page_id_; 
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key) -> Page * {
  if (IsEmpty()) {
    return nullptr;
  }

  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (page == nullptr) {
    return nullptr;
  }
  
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  // 逐层向下查找，直到叶子节点
  while (!node->IsLeafPage()) {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    
    // 查找合适的子节点
    int index = 1;
    while (index < internal_node->GetSize() && comparator_(internal_node->KeyAt(index), key) <= 0) {
      index++;
    }
    
    page_id_t child_page_id = internal_node->ValueAt(index - 1);

    // 获取子页面
    Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);
    if (child_page == nullptr) {
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      return nullptr;
    }
    
    // 现在可以unpin当前页面了
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    
    // 移动到子页面
    page = child_page;
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }

  return page;  // 返回叶子页面，调用者负责unpin
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) -> bool {
  page_id_t new_page_id;
  auto *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    return false;
  }

  auto *root_node = reinterpret_cast<LeafPage *>(new_page->GetData());
  root_node->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
  
  // 插入第一个键值对
  bool success = root_node->Insert(key, value, comparator_);
  if (!success) {
    buffer_pool_manager_->UnpinPage(new_page_id, false);
    buffer_pool_manager_->DeletePage(new_page_id);
    return false;
  }
  
  root_page_id_ = new_page_id;
  UpdateRootPageId(1);
  
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  Page *leaf_page = FindLeafPage(key);
  if (leaf_page == nullptr) {
    return false;
  }

  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  
  // 显式检查重复键
  for (int i = 0; i < leaf_node->GetSize(); i++) {
    if (comparator_(leaf_node->KeyAt(i), key) == 0) {
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
      return false;
    }
  }
  
  // 插入键值对
  bool success = leaf_node->Insert(key, value, comparator_);
  if (!success) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  
  // 检查是否需要分裂
  if (leaf_node->GetSize() >= leaf_max_size_) {
    auto [new_leaf, split_key] = Split(leaf_node);  // 使用结构化绑定
    
    // 检查 Split 是否成功
    if (new_leaf == nullptr) {
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
      return false;  // 分裂失败
    }
    
    InsertIntoParent(leaf_node,  split_key, new_leaf, transaction);
    
    // Unpin新创建的叶子页面
    buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);
  }
  
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *node) -> std::pair<N*, KeyType> {
  page_id_t new_page_id;
  auto *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  
  if (new_page == nullptr) {
    return {nullptr, KeyType()};
  }
  
  auto *new_node = new (new_page->GetData()) N();
  KeyType split_key;
  
  if (node->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);
    auto *new_leaf = reinterpret_cast<LeafPage *>(new_node);
    
    new_leaf->Init(new_page_id, node->GetParentPageId(), leaf_max_size_);
    new_leaf->SetNextPageId(leaf_node->GetNextPageId());
    leaf_node->SetNextPageId(new_page_id);
    
    // 叶子节点分裂：分裂键由 MoveHalfTo 返回
    split_key = leaf_node->MoveHalfTo(new_leaf);
    
  } else {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *new_internal = reinterpret_cast<InternalPage *>(new_node);
    
    new_internal->Init(new_page_id, node->GetParentPageId(), internal_max_size_);
    
    // 内部节点分裂：分裂键由 MoveHalfTo 返回
    split_key = internal_node->MoveHalfTo(new_internal);
    
    // 更新被移动到新节点的子页面的父指针
    for (int i = 0; i < new_internal->GetSize(); i++) {
      page_id_t child_page_id = new_internal->ValueAt(i);
      auto *child_page = buffer_pool_manager_->FetchPage(child_page_id);
      if (child_page != nullptr) {
        auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
        child_node->SetParentPageId(new_page_id);
        buffer_pool_manager_->UnpinPage(child_page_id, true);
      }
    }
  }
  
  return {new_node, split_key};
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                     Transaction *transaction) {
  // 如果old_node是根节点，需要创建新的根节点
  if (old_node->IsRootPage()) {
    page_id_t new_root_id;
    Page *new_root_page = buffer_pool_manager_->NewPage(&new_root_id);
    if (new_root_page == nullptr) {
      return;
    }
    
    auto *new_root = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root->Init(new_root_id, INVALID_PAGE_ID, internal_max_size_);
    
    // 设置子节点
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    
    // 更新父指针
    old_node->SetParentPageId(new_root_id);
    new_node->SetParentPageId(new_root_id);
    
    root_page_id_ = new_root_id;
    UpdateRootPageId(1);
    
    buffer_pool_manager_->UnpinPage(new_root_id, true);
    return;
  }

  // 否则插入到现有的父节点
  page_id_t parent_id = old_node->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  if (parent_page == nullptr) {
    return;
  }
  
  auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  
  // 插入新的键值对到父节点
  parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

  new_node->SetParentPageId(parent_id);
  
  // 如果父节点满了，需要分裂
  if (parent->GetSize() >= internal_max_size_) {
    auto [new_parent, split_key] = Split(parent);  // 使用结构化绑定
    if (new_parent != nullptr) {
      // 注意：这里传递的是 split_key，不是 new_parent->KeyAt(1)!
      InsertIntoParent(parent, split_key, new_parent, transaction);
      buffer_pool_manager_->UnpinPage(new_parent->GetPageId(), true);
    }
  }

  buffer_pool_manager_->UnpinPage(parent_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // 如果是根节点，特殊处理
  if (node->IsRootPage()) {
    AdjustRoot(node);
    return;
  }

  // 判断节点是否下溢
  // 注意：GetMinSize()返回最小键数，但内部节点的size是孩子数量
  bool is_underflow;
  if (node->IsLeafPage()) {
    // 叶子节点：size是键值对数量
    is_underflow = (node->GetSize() < node->GetMinSize());
  } else {
    // 内部节点：size是孩子数量，最小孩子数 = 最小键数 + 1
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    int min_children = internal_node->GetMinSize() + 1;
    is_underflow = (internal_node->GetSize() < min_children);
  }
  
  // 如果不下溢，直接返回（但需要更新父节点key）
  if (!is_underflow) {
    UpdateParentKey(node);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    return;
  }

  // 获取父节点
  auto *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (parent_page == nullptr) {
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    return;
  }
  
  auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  
  // 找到node在父节点中的索引
  // 注意：父节点是内部节点，size表示value的数量，有效键在index 1..size-1
  int node_index = -1;
  for (int i = 0; i < parent->GetSize(); i++) {
    // parent->ValueAt(i) 获取第i个孩子节点的page_id
    if (parent->ValueAt(i) == node->GetPageId()) {
      node_index = i;
      break;
    }
  }
  
  if (node_index == -1) {
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    return;
  }
  
  // 尝试从左兄弟借
  if (node_index > 0) {
    page_id_t left_sibling_id = parent->ValueAt(node_index - 1);
    auto *left_page = buffer_pool_manager_->FetchPage(left_sibling_id);
    auto *left_sibling = reinterpret_cast<N *>(left_page->GetData());
    
    // 判断左兄弟是否有富余
    bool left_has_surplus;
    if (left_sibling->IsLeafPage()) {
      // 叶子节点：直接比较键数
      left_has_surplus = (left_sibling->GetSize() > left_sibling->GetMinSize());
    } else {
      // 内部节点：需要计算最小孩子数
      auto *internal_left = reinterpret_cast<InternalPage *>(left_sibling);
      int min_children = internal_left->GetMinSize() + 1;
      left_has_surplus = (internal_left->GetSize() > min_children);
    }
    
    if (left_has_surplus) {
      // 从左兄弟重分配
      Redistribute(left_sibling, node, parent, node_index - 1, true);
      buffer_pool_manager_->UnpinPage(left_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
      return;
    }
    buffer_pool_manager_->UnpinPage(left_page->GetPageId(), false);
  }
  
  // 尝试从右兄弟借
  if (node_index < parent->GetSize() - 1) {
    page_id_t right_sibling_id = parent->ValueAt(node_index + 1);
    auto *right_page = buffer_pool_manager_->FetchPage(right_sibling_id);
    auto *right_sibling = reinterpret_cast<N *>(right_page->GetData());
    
    // 判断右兄弟是否有富余
    bool right_has_surplus;
    if (right_sibling->IsLeafPage()) {
      // 叶子节点：直接比较键数
      right_has_surplus = (right_sibling->GetSize() > right_sibling->GetMinSize());
    } else {
      // 内部节点：需要计算最小孩子数
      auto *internal_right = reinterpret_cast<InternalPage *>(right_sibling);
      int min_children = internal_right->GetMinSize() + 1;
      right_has_surplus = (internal_right->GetSize() > min_children);
    }
    
    if (right_has_surplus) {
      // 从右兄弟重分配
      Redistribute(node, right_sibling, parent, node_index, false);
      buffer_pool_manager_->UnpinPage(right_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
      return;
    }
    buffer_pool_manager_->UnpinPage(right_page->GetPageId(), false);
  }
  
  // 需要合并
  if (node_index > 0) {
    // 与左兄弟合并
    page_id_t left_sibling_id = parent->ValueAt(node_index - 1);
    auto *left_page = buffer_pool_manager_->FetchPage(left_sibling_id);
    auto *left_sibling = reinterpret_cast<N *>(left_page->GetData());
    
    // 检查是否可以合并
    // 对于内部节点，合并条件：left_size + node_size <= max_children
    // max_children = max_size + 1 (因为size是孩子数)
    bool can_coalesce;
    if (node->IsLeafPage()) {
      // 叶子节点：直接比较键数
      can_coalesce = (left_sibling->GetSize() + node->GetSize() <= leaf_max_size_);
    } else {
      // 内部节点：需要比较孩子数
      // max_children = internal_max_size_ + 1
      can_coalesce = (left_sibling->GetSize() + node->GetSize() <= internal_max_size_ + 1);
    }
    
    if (can_coalesce) {
      Coalesce(left_sibling, node, parent, node_index - 1, transaction);
      buffer_pool_manager_->UnpinPage(left_page->GetPageId(), true);
    } else {
      // 不能合并，这可能是个错误状态
      buffer_pool_manager_->UnpinPage(left_page->GetPageId(), false);
    }
  } else if (node_index < parent->GetSize() - 1) {
    // 与右兄弟合并
    page_id_t right_sibling_id = parent->ValueAt(node_index + 1);
    auto *right_page = buffer_pool_manager_->FetchPage(right_sibling_id);
    auto *right_sibling = reinterpret_cast<N *>(right_page->GetData());
    
    // 检查是否可以合并
    bool can_coalesce;
    if (node->IsLeafPage()) {
      can_coalesce = (node->GetSize() + right_sibling->GetSize() <= leaf_max_size_);
    } else {
      can_coalesce = (node->GetSize() + right_sibling->GetSize() <= internal_max_size_ + 1);
    }
    
    if (can_coalesce) {
      Coalesce(node, right_sibling, parent, node_index, transaction);
      buffer_pool_manager_->UnpinPage(right_page->GetPageId(), true);
    } else {
      buffer_pool_manager_->UnpinPage(right_page->GetPageId(), false);
    }
  }
  
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}


INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node, BPlusTreePage *parent, int index,
                             Transaction *transaction) {
  auto *internal_parent = reinterpret_cast<InternalPage *>(parent);
  
  if (node->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);
    auto *leaf_neighbor = reinterpret_cast<LeafPage *>(neighbor_node);
    
    // 将node的所有元素移动到neighbor_node
    for (int i = 0; i < leaf_node->GetSize(); i++) {
      leaf_neighbor->Insert(leaf_node->KeyAt(i), leaf_node->ValueAt(i), comparator_);
    }
    
    // 更新叶子节点的链表指针
    leaf_neighbor->SetNextPageId(leaf_node->GetNextPageId());
    // 注意：你的叶子节点没有SetPrevPageId方法，所以这行要去掉
  } else {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *internal_neighbor = reinterpret_cast<InternalPage *>(neighbor_node);
    
    // 首先插入父节点的分隔key
    KeyType parent_key = internal_parent->KeyAt(index + 1);
    
    if (neighbor_node->GetPageId() == node->GetPageId()) {
      // 与右兄弟合并（neighbor是右兄弟）
      internal_neighbor->Insert(parent_key, internal_node->ValueAt(0), comparator_);
      
      // 然后将右兄弟的所有键值对移动到左兄弟
      for (int i = 0; i < internal_node->GetSize(); i++) {
        if (i > 0) {
          internal_neighbor->Insert(internal_node->KeyAt(i), internal_node->ValueAt(i), comparator_);
        }
      }
    } else {
      // 与左兄弟合并（neighbor是左兄弟）
      internal_neighbor->Insert(parent_key, internal_node->ValueAt(0), comparator_);
      
      // 然后将node的所有键值对移动到左兄弟
      for (int i = 0; i < internal_node->GetSize(); i++) {
        if (i > 0) {
          internal_neighbor->Insert(internal_node->KeyAt(i), internal_node->ValueAt(i), comparator_);
        }
      }
    }
    
    // 更新所有被移动的子页面的父指针
    for (int i = 0; i < internal_node->GetSize(); i++) {
      page_id_t child_page_id = internal_node->ValueAt(i);
      auto *child_page = buffer_pool_manager_->FetchPage(child_page_id);
      if (child_page != nullptr) {
        auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
        child_node->SetParentPageId(internal_neighbor->GetPageId());
        buffer_pool_manager_->UnpinPage(child_page_id, true);
      }
    }
  }
  
  // 从父节点中删除对应的key和孩子
  internal_parent->RemoveAt(index);
  
  // 如果父节点太小，递归处理
  bool parent_underflow;
  if (internal_parent->IsLeafPage()) {
    parent_underflow = (internal_parent->GetSize() < internal_parent->GetMinSize());
  } else {
    int min_children = internal_parent->GetMinSize() + 1;
    parent_underflow = (internal_parent->GetSize() < min_children);
  }
  
  if (parent_underflow && !internal_parent->IsRootPage()) {
    CoalesceOrRedistribute(internal_parent, transaction);
  } else if (internal_parent->IsRootPage() && internal_parent->GetSize() == 1) {
    AdjustRoot(internal_parent);
  } else {
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  }
  
  // 删除node页面
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->DeletePage(node->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, BPlusTreePage *parent, 
                                 int index, bool is_from_left) {
  auto *internal_parent = reinterpret_cast<InternalPage *>(parent);
  
  if (node->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);
    auto *leaf_neighbor = reinterpret_cast<LeafPage *>(neighbor_node);
    
    if (is_from_left) {
      // 从左兄弟借最后一个元素
      int last_index = leaf_neighbor->GetSize() - 1;
      KeyType borrowed_key = leaf_neighbor->KeyAt(last_index);
      ValueType borrowed_value = leaf_neighbor->ValueAt(last_index);
      
      leaf_neighbor->RemoveAt(last_index);
      
      // 手动在叶子节点最前面插入
      // 1. 增加大小
      leaf_node->IncreaseSize(1);
      
      // 2. 将现有元素向后移动
      for (int i = leaf_node->GetSize() - 1; i > 0; i--) {
        leaf_node->GetItem(i) = leaf_node->GetItem(i - 1);
      }
      
      // 3. 在位置0插入新元素
      // 对于叶子节点，MappingType是pair<KeyType, RID>
      leaf_node->GetItem(0).first = borrowed_key;
      leaf_node->GetItem(0).second = borrowed_value;
      
      // 更新父节点中的key
      internal_parent->SetKeyAt(index + 1, leaf_node->KeyAt(0));
    } else {
      // 从右兄弟借第一个元素
      KeyType borrowed_key = leaf_neighbor->KeyAt(0);
      ValueType borrowed_value = leaf_neighbor->ValueAt(0);
      
      leaf_neighbor->RemoveAt(0);
      
      // 在叶子节点中插入到最后面 - 使用Insert方法
      leaf_node->Insert(borrowed_key, borrowed_value, comparator_);
      
      // 更新父节点中的key
      internal_parent->SetKeyAt(index + 1, leaf_neighbor->KeyAt(0));
    }
  } else {
    // 内部节点的重分配
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *internal_neighbor = reinterpret_cast<InternalPage *>(neighbor_node);
    
    if (is_from_left) {
      // 从左兄弟借
      int last_key_index = internal_neighbor->GetSize() - 2; // 最后一个有效键的索引
      int last_child_index = internal_neighbor->GetSize() - 1; // 最后一个孩子的索引
      
      KeyType borrowed_key = internal_neighbor->KeyAt(last_key_index);
      page_id_t borrowed_child_id = internal_neighbor->ValueAt(last_child_index);
      
      // 从邻居节点删除
      internal_neighbor->RemoveAt(last_key_index);
      
      // 获取父节点中对应的key
      KeyType parent_key = internal_parent->KeyAt(index + 1);
      
      // 将父节点的key下移到当前节点
      // 1. 先将当前节点所有元素右移一位
      for (int i = internal_node->GetSize(); i > 0; i--) {
        if (i < internal_node->GetSize()) {
          internal_node->GetItem(i) = internal_node->GetItem(i - 1);
        }
      }
      
      // 2. 在位置0插入借来的孩子（键为无效）
      // 对于内部节点，MappingType是pair<KeyType, page_id_t>
      internal_node->GetItem(0).first = KeyType();  // 无效键
      internal_node->GetItem(0).second = borrowed_child_id;
      
      // 3. 更新位置1的键（如果存在）
      if (internal_node->GetSize() > 0) {
        internal_node->GetItem(1).first = parent_key;
      }
      
      internal_node->IncreaseSize(1);
      
      // 4. 将借来的key上移到父节点
      internal_parent->SetKeyAt(index + 1, borrowed_key);
      
      // 更新被移动孩子的父指针
      auto *child_page = buffer_pool_manager_->FetchPage(borrowed_child_id);
      if (child_page != nullptr) {
        auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
        child_node->SetParentPageId(internal_node->GetPageId());
        buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
      }
    } else {
      // 从右兄弟借
      KeyType borrowed_key = internal_neighbor->KeyAt(1);
      page_id_t borrowed_child_id = internal_neighbor->ValueAt(0);
      
      // 从邻居节点删除第一个有效键和第一个孩子
      internal_neighbor->RemoveAt(0);
      
      // 获取父节点中对应的key
      KeyType parent_key = internal_parent->KeyAt(index + 1);
      
      // 将父节点的key下移到当前节点
      // 使用Insert方法插入到最后
      internal_node->Insert(parent_key, borrowed_child_id, comparator_);
      
      // 将借来的key上移到父节点
      internal_parent->SetKeyAt(index + 1, borrowed_key);
      
      // 更新被移动孩子的父指针
      auto *child_page = buffer_pool_manager_->FetchPage(borrowed_child_id);
      if (child_page != nullptr) {
        auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
        child_node->SetParentPageId(internal_node->GetPageId());
        buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // 如果根节点是叶子节点且为空，整棵树为空
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    return;
  }

  // 如果根节点是内部节点且只有一个子节点，让子节点成为新的根节点
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    auto *internal_root = reinterpret_cast<InternalPage *>(old_root_node);
    root_page_id_ = internal_root->ValueAt(0);
    
    // 更新新根节点的父指针
    auto *new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    auto *new_root = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
    new_root->SetParentPageId(INVALID_PAGE_ID);
    
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
  }
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateParentKey(BPlusTreePage *node) {
  if (node->IsRootPage() || !node->IsLeafPage()) {
    return;
  }
  
  auto *leaf_node = reinterpret_cast<LeafPage *>(node);
  
  if (leaf_node->GetSize() == 0) {
    return;
  }
  
  page_id_t parent_page_id = leaf_node->GetParentPageId();
  if (parent_page_id == INVALID_PAGE_ID) {
    return;
  }
  
  auto *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  if (parent_page == nullptr) {
    return;
  }
  
  auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  
  // 找到叶子节点在父节点中的索引
  int child_index = -1;
  for (int i = 0; i < parent->GetSize(); i++) {
    if (parent->ValueAt(i) == leaf_node->GetPageId()) {
      child_index = i;
      break;
    }
  }
  
  if (child_index > 0) {
    // 更新父节点中的键为叶子节点的第一个键
    parent->SetKeyAt(child_index, leaf_node->KeyAt(0));
  }
  
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindSibling(BPlusTreePage *node, BPlusTreePage **sibling, bool *is_prev) -> bool {
  auto *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (parent_page == nullptr) {
    return false;
  }
  
  auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  
  int index = FindIndexInParent(node);
  if (index == -1) {
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
    return false;
  }
  
  // 尝试找前一个兄弟节点
  if (index > 0) {
    page_id_t sibling_id = parent->ValueAt(index - 1);
    auto *sibling_page = buffer_pool_manager_->FetchPage(sibling_id);
    if (sibling_page != nullptr) {
      *sibling = reinterpret_cast<BPlusTreePage *>(sibling_page->GetData());
      *is_prev = true;
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
      return true;
    }
  }
  
  // 尝试找后一个兄弟节点
  if (index < parent->GetSize() - 1) {
    page_id_t sibling_id = parent->ValueAt(index + 1);
    auto *sibling_page = buffer_pool_manager_->FetchPage(sibling_id);
    if (sibling_page != nullptr) {
      *sibling = reinterpret_cast<BPlusTreePage *>(sibling_page->GetData());
      *is_prev = false;
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
      return true;
    }
  }
  
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindIndexInParent(BPlusTreePage *node) -> int {
  auto *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  
  for (int i = 0; i < parent->GetSize(); i++) {
    if (parent->ValueAt(i) == node->GetPageId()) {
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
      return i;
    }
  }
  
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */

 INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  std::lock_guard<std::mutex> lock(root_latch_);

  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  std::lock_guard<std::mutex> lock(root_latch_);
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}


/*****************************************************************************
 * DEBUG METHODS 
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

// 基础类的显式实例化
template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;



}  // namespace bustub