/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() : buffer_pool_manager_(nullptr), leaf_(nullptr), index_(0), page_id_(INVALID_PAGE_ID) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf, int index)
    : buffer_pool_manager_(bpm), leaf_(leaf), index_(index), page_id_(leaf->GetPageId()) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  // 如果迭代器持有页面，需要取消固定
  if (leaf_ != nullptr && buffer_pool_manager_ != nullptr) {
    buffer_pool_manager_->UnpinPage(page_id_, false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  // 如果当前页面为空或者索引超出范围且没有下一个页面，则到达末尾
  return leaf_ == nullptr || 
         (index_ >= leaf_->GetSize() && leaf_->GetNextPageId() == INVALID_PAGE_ID);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  if (leaf_ == nullptr || index_ >= leaf_->GetSize()) {
    throw std::runtime_error("Dereferencing invalid iterator");
  }
  return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (leaf_ == nullptr) {
    return *this;
  }

  // 移动到下一个元素
  index_++;

  // 如果当前页面已经遍历完，移动到下一个叶子页面
  if (index_ >= leaf_->GetSize()) {
    page_id_t next_page_id = leaf_->GetNextPageId();
    
    // 取消固定当前页面
    buffer_pool_manager_->UnpinPage(page_id_, false);
    
    if (next_page_id == INVALID_PAGE_ID) {
      // 没有下一个页面，设置为结束状态
      leaf_ = nullptr;
      index_ = 0;
      page_id_ = INVALID_PAGE_ID;
    } else {
      // 获取下一个叶子页面
      auto *next_page = buffer_pool_manager_->FetchPage(next_page_id);
      leaf_ = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(next_page->GetData());
      index_ = 0;
      page_id_ = next_page_id;
    }
  }

  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  // 两个迭代器相等当且仅当它们指向相同的页面和相同的索引
  // 或者都是结束迭代器
  if (leaf_ == nullptr && itr.leaf_ == nullptr) {
    return true;  // 都是结束迭代器
  }
  if (leaf_ == nullptr || itr.leaf_ == nullptr) {
    return false; // 一个结束一个没结束
  }
  return page_id_ == itr.page_id_ && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool {
  return !(*this == itr);
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub