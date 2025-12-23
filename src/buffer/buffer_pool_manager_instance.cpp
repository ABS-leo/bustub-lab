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

// 构造函数：初始化缓冲池管理器实例
// 参数：
//   - pool_size: 缓冲池大小（帧的数量）
//   - disk_manager: 磁盘管理器，用于读写磁盘页面
//   - replacer_k: LRU-K算法中的K值
//   - log_manager: 日志管理器（本项目中未使用）
BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // 为缓冲池分配连续的内存空间
  pages_ = new Page[pool_size_];
  
  // 初始化可扩展哈希表，用于维护page_id到frame_id的映射
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  
  // 初始化LRU-K替换器
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // 初始时，所有帧都处于空闲状态，加入空闲列表
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }
}

// 析构函数：释放缓冲池资源
BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;       // 释放Page数组
  delete page_table_;    // 释放哈希表
  delete replacer_;      // 释放替换器
}

// NewPgImp: 创建新页面并存入缓冲池
// 参数：page_id - 输出参数，返回分配的新页面ID
// 返回值：指向新页面的指针，失败返回nullptr
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock lock(latch_);  // 线程安全：使用互斥锁保护整个函数

  // 第一步：尝试获取空闲帧
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    // 有空闲帧：从空闲列表头部取出
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // 无空闲帧：尝试驱逐一个页面来腾出帧
    if (!replacer_->Evict(&frame_id)) {
      return nullptr; // 无可驱逐帧，返回nullptr
    }

    // 如果被驱逐的页面是脏页，需要先写回磁盘
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }

    // 从页表中移除被驱逐页面的映射关系
    page_table_->Remove(pages_[frame_id].GetPageId());
  }

  // 第二步：分配新的页面ID
  *page_id = AllocatePage();

  // 第三步：重置页面的元数据和内存内容
  pages_[frame_id].ResetMemory();         // 清空页面数据
  pages_[frame_id].page_id_ = *page_id;   // 设置页面ID
  pages_[frame_id].pin_count_ = 1;        // 设置pin_count为1（当前被固定）
  pages_[frame_id].is_dirty_ = false;     // 新页面初始为干净页

  // 第四步：更新管理数据结构
  page_table_->Insert(*page_id, frame_id);      // 建立页面ID到帧ID的映射
  replacer_->RecordAccess(frame_id);            // 记录该帧被访问
  replacer_->SetEvictable(frame_id, false);     // 设置为不可驱逐（因为刚被固定）

  // 第五步：返回页面指针
  return &pages_[frame_id];
}

// FetchPgImp: 从缓冲池获取指定页面（如果不在缓冲池则从磁盘加载）
// 参数：page_id - 要获取的页面ID
// 返回值：指向页面的指针，失败返回nullptr
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock lock(latch_);  // 线程安全

  frame_id_t frame_id;
  
  // 第一步：检查页面是否已在缓冲池中
  if (page_table_->Find(page_id, frame_id)) {
    // 命中缓存：更新访问信息和引用计数
    pages_[frame_id].pin_count_++;                  // 增加引用计数
    replacer_->RecordAccess(frame_id);              // 记录访问
    replacer_->SetEvictable(frame_id, false);       // 设置为不可驱逐
    return &pages_[frame_id];                      // 返回页面指针
  }

  // 第二步：页面不在缓冲池，需要从磁盘加载
  // 首先获取一个帧（与NewPage逻辑相同）
  if (!free_list_.empty()) {
    // 有空闲帧
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // 无空闲帧，尝试驱逐
    if (!replacer_->Evict(&frame_id)) {
      return nullptr; // 无可驱逐帧
    }

    // 处理被驱逐的脏页
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }

    // 从页表中移除被驱逐页面
    page_table_->Remove(pages_[frame_id].GetPageId());
  }

  // 第三步：重置页面元数据并从磁盘加载数据
  pages_[frame_id].ResetMemory();          // 清空页面数据
  pages_[frame_id].page_id_ = page_id;    // 设置页面ID
  pages_[frame_id].pin_count_ = 1;        // 设置引用计数
  pages_[frame_id].is_dirty_ = false;     // 初始为干净页
  
  // 从磁盘读取页面数据
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

  // 第四步：更新管理数据结构
  page_table_->Insert(page_id, frame_id);      // 建立映射
  replacer_->RecordAccess(frame_id);           // 记录访问
  replacer_->SetEvictable(frame_id, false);    // 设置为不可驱逐

  return &pages_[frame_id];  // 返回页面指针
}

// UnpinPgImp: 减少页面的引用计数，更新脏标志
// 参数：
//   - page_id: 页面ID
//   - is_dirty: 页面是否被修改
// 返回值：操作成功返回true，失败返回false
auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock lock(latch_);  // 线程安全

  frame_id_t frame_id;
  
  // 第一步：检查页面是否在缓冲池中
  if (!page_table_->Find(page_id, frame_id)) {
    return false; // 页面不在缓冲池
  }

  // 第二步：安全性检查：引用计数不能为负数
  if (pages_[frame_id].GetPinCount() <= 0) {
    return false; // 引用计数异常
  }

  // 第三步：减少引用计数
  pages_[frame_id].pin_count_--;

  // 第四步：更新脏标志（单向设置：只能从false变为true）
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }

  // 第五步：如果引用计数归零，标记为可驱逐
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true; // 操作成功
}

// FlushPgImp: 将指定页面强制写回磁盘（无论是否为脏页）
// 参数：page_id - 要刷新的页面ID
// 返回值：操作成功返回true，失败返回false
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock lock(latch_);  // 线程安全

  frame_id_t frame_id;
  
  // 检查页面是否在缓冲池中
  if (!page_table_->Find(page_id, frame_id)) {
    return false; // 页面不在缓冲池
  }

  // 将页面数据写回磁盘（忽略脏标志）
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  
  // 重置脏标志
  pages_[frame_id].is_dirty_ = false;

  return true; // 刷新成功
}

// FlushAllPgsImp: 将缓冲池中所有脏页写回磁盘
void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock lock(latch_);  // 线程安全

  // 遍历所有帧
  for (size_t i = 0; i < pool_size_; ++i) {
    // 如果帧中有有效页面且为脏页
    if (pages_[i].GetPageId() != INVALID_PAGE_ID && pages_[i].IsDirty()) {
      // 写回磁盘
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
      
      // 重置脏标志
      pages_[i].is_dirty_ = false;
    }
  }
}

// DeletePgImp: 删除指定页面（从缓冲池和磁盘中移除）
// 参数：page_id - 要删除的页面ID
// 返回值：删除成功返回true，失败返回false
auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock lock(latch_);  // 线程安全

  frame_id_t frame_id;
  
  // 第一步：检查页面是否在缓冲池中
  if (!page_table_->Find(page_id, frame_id)) {
    return true; // 页面不存在，视为删除成功
  }

  // 第二步：检查页面是否被固定（pin_count > 0）
  if (pages_[frame_id].GetPinCount() > 0) {
    return false; // 页面正在被使用，不能删除
  }

  // 第三步：从页表和替换器中移除页面
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);

  // 第四步：重置帧状态并加入空闲列表
  pages_[frame_id].ResetMemory();                // 清空页面数据
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;   // 设置无效页面ID
  pages_[frame_id].pin_count_ = 0;               // 重置引用计数
  pages_[frame_id].is_dirty_ = false;            // 重置脏标志
  
  free_list_.push_back(frame_id);                // 帧回归空闲列表

  // 第五步：回收磁盘页面（由上层管理）
  DeallocatePage(page_id);

  return true; // 删除成功
}

// AllocatePage: 分配新的页面ID（单调递增）
// 返回值：新的页面ID
auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  return next_page_id_++;
}

}  // namespace bustub
