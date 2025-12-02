//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager_instance.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every frame is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock lock(latch_);

  // First, try to get a free frame
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // If no free frames, try to evict a page
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;  // No frames available
    }

    // If the evicted page is dirty, flush it to disk
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }

    // Remove the evicted page from page table
    page_table_->Remove(pages_[frame_id].GetPageId());
  }

  // Allocate a new page id
  *page_id = AllocatePage();

  // Reset the page metadata
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;

  // Add to page table and replacer
  page_table_->Insert(*page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock lock(latch_);

  frame_id_t frame_id;

  // If page is already in buffer pool
  if (page_table_->Find(page_id, frame_id)) {
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }

  // If page is not in buffer pool, need to load it from disk
  // First, get a frame
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;  // No frames available
    }

    // If the evicted page is dirty, flush it to disk
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }

    page_table_->Remove(pages_[frame_id].GetPageId());
  }

  // Reset the page and load data from disk
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;

  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

  // Update metadata
  page_table_->Insert(page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;  // Page not found
  }

  if (pages_[frame_id].GetPinCount() <= 0) {
    return false;  // Pin count already 0
  }

  pages_[frame_id].pin_count_--;

  // Update dirty flag
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }

  // If pin count becomes 0, mark as evictable
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;  // Page not found
  }

  // Write page to disk regardless of dirty flag
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock lock(latch_);

  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].GetPageId() != INVALID_PAGE_ID && pages_[i].IsDirty()) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;  // Page not found, considered successfully deleted
  }

  // If page is pinned, cannot delete
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }

  // Remove from page table and replacer
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);

  // Reset page metadata and add frame back to free list
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;

  free_list_.push_back(frame_id);

  // Deallocate the page
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
