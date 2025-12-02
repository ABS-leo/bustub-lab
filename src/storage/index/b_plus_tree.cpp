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
  
  // 注意：这里我们不再unpin页面，因为迭代器会管理页面的生命周期
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

  // 找到兄弟节点
  BPlusTreePage *sibling;
  bool is_prev;
  if (!FindSibling(node, &sibling, &is_prev)) {
    return;
  }

  auto *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (parent_page == nullptr) {
    return;
  }
  
  auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  
  int node_index = FindIndexInParent(node);
  if (node_index == -1) {
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
    return;
  }
  
  // 如果可以和兄弟节点合并
  if (node->GetSize() + sibling->GetSize() <= 
      (node->IsLeafPage() ? leaf_max_size_ : internal_max_size_)) {
    if (is_prev) {
      Coalesce(reinterpret_cast<N*>(sibling), node, parent, node_index, transaction);
    } else {
      Coalesce(node, reinterpret_cast<N*>(sibling), parent, node_index - 1, transaction);
    }
  } else {
    // 否则重分配
    Redistribute(reinterpret_cast<N*>(sibling), node, parent, node_index);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
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
  } else {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *internal_neighbor = reinterpret_cast<InternalPage *>(neighbor_node);
    
    // 首先插入父节点的分隔key
    KeyType parent_key = internal_parent->KeyAt(index);
    internal_neighbor->Insert(parent_key, internal_node->ValueAt(0), comparator_);
    
    // 将node的剩余元素移动到neighbor_node
    for (int i = 1; i < internal_node->GetSize(); i++) {
      internal_neighbor->Insert(internal_node->KeyAt(i), internal_node->ValueAt(i), comparator_);
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
  
  // 从父节点中删除对应的key
  internal_parent->RemoveAt(index);
  
  // 如果父节点太小，递归处理
  if (internal_parent->GetSize() < internal_parent->GetMinSize() && !internal_parent->IsRootPage()) {
    CoalesceOrRedistribute(internal_parent, transaction);
  }
  
  // 删除node页面
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->DeletePage(node->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, BPlusTreePage *parent, int index) {
  // 确保是叶子节点
  if (!node->IsLeafPage()) {
    return;
  }
  
  auto *internal_parent = reinterpret_cast<InternalPage *>(parent);
  auto *leaf_node = reinterpret_cast<LeafPage *>(node);
  auto *leaf_neighbor = reinterpret_cast<LeafPage *>(neighbor_node);
  
  // 只处理叶子节点的重分配
  if (index > 0) {
    // 从左兄弟借
    int last_index = leaf_neighbor->GetSize() - 1;
    KeyType borrowed_key = leaf_neighbor->KeyAt(last_index);
    ValueType borrowed_value = leaf_neighbor->ValueAt(last_index);
    
    leaf_neighbor->RemoveAt(last_index);
    leaf_node->Insert(borrowed_key, borrowed_value, comparator_);
    internal_parent->SetKeyAt(index, leaf_node->KeyAt(0));
  } else {
    // 从右兄弟借  
    KeyType borrowed_key = leaf_neighbor->KeyAt(0);
    ValueType borrowed_value = leaf_neighbor->ValueAt(0);
    
    leaf_neighbor->RemoveAt(0);
    leaf_node->Insert(borrowed_key, borrowed_value, comparator_);
    internal_parent->SetKeyAt(index + 1, leaf_neighbor->KeyAt(0));
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // 如果根节点是叶子节点且为空，整棵树为空
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
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