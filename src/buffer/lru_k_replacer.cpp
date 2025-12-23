//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include <limits>

namespace bustub {

// 构造函数：初始化LRU-K替换器
// 参数：
//   - num_frames: 替换器管理的最大帧数（等于缓冲池大小）
//   - k: 算法中的K值，表示考虑的访问历史长度
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

// 析构函数：清理资源
LRUKReplacer::~LRUKReplacer() = default;

// Evict: 驱逐具有最大后退K距离的可驱逐帧
// 返回值：如果成功驱逐返回true，否则返回false
// 输出参数：frame_id 存储被驱逐帧的ID
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock lock(latch_);  // 线程安全：使用互斥锁保护整个函数

  if (curr_size_ == 0) {
    return false;  // 没有可驱逐的帧
  }

  frame_id_t candidate = -1;  // 候选帧ID
  size_t max_k_distance = 0;  // 最大后退K距离
  size_t earliest_timestamp = std::numeric_limits<size_t>::max();  // 最早时间戳

  // 遍历所有帧，选择最佳驱逐候选
  for (const auto &[fid, frame_info] : frame_table_) {
    if (!frame_info.is_evictable_) {
      continue;  // 跳过不可驱逐的帧
    }

    size_t k_distance;      // 后退K距离
    size_t frame_earliest;  // 关键时间戳

    // 根据访问次数判断属于哪个"逻辑队列"
    if (frame_info.history_.size() < k_) {
      // 访问不足K次：属于"历史队列"，后退K距离设为无穷大
      k_distance = std::numeric_limits<size_t>::max();
      frame_earliest = frame_info.history_.front();  // 比较第一次访问时间
    } else {
      // 访问达到K次：属于"缓存队列"，计算实际后退K距离
      k_distance = current_timestamp_ - frame_info.history_.front();
      frame_earliest = frame_info.history_.front();  // 比较第K次访问时间
    }

    // 两级比较策略：
    // 1. 优先选择后退K距离更大的帧
    // 2. 距离相同时，选择访问时间更早的帧
    if (k_distance > max_k_distance || 
        (k_distance == max_k_distance && frame_earliest < earliest_timestamp)) {
      candidate = fid;
      max_k_distance = k_distance;
      earliest_timestamp = frame_earliest;
    }
  }

  if (candidate == -1) {
    return false;  // 没有找到合适的候选帧
  }

  *frame_id = candidate;            // 输出被驱逐帧ID
  frame_table_.erase(candidate);    // 从帧表中移除
  curr_size_--;                     // 减少可驱逐帧计数
  return true;                      // 驱逐成功
}

// RecordAccess: 记录帧被访问的时间戳
// 参数：frame_id - 被访问的帧ID
void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);  // 线程安全

  // 检查帧ID有效性
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    return;
  }

  current_timestamp_++;  // 全局时间戳递增

  // 如果帧不存在于帧表中，创建新条目
  if (frame_table_.find(frame_id) == frame_table_.end()) {
    frame_table_[frame_id] = FrameInfo();
  }

  auto &frame_info = frame_table_[frame_id];
  
  // 添加当前时间戳到访问历史
  frame_info.history_.push_back(current_timestamp_);
  
  // 维护访问历史长度不超过K：只保留最近K次访问的时间戳
  // 队列头部保存第K次访问时间（如果访问次数≥K）
  if (frame_info.history_.size() > k_) {
    frame_info.history_.pop_front();
  }
}

// SetEvictable: 设置帧的可驱逐状态，并更新可驱逐帧计数
// 参数：
//   - frame_id: 帧ID
//   - set_evictable: true表示设置为可驱逐，false表示不可驱逐
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock lock(latch_);  // 线程安全

  auto it = frame_table_.find(frame_id);
  if (it == frame_table_.end()) {
    return;  // 帧不存在，直接返回
  }

  auto &frame_info = it->second;
  
  // 更新可驱逐帧计数：
  // 1. 从可驱逐变为不可驱逐：计数减1
  // 2. 从不可驱逐变为可驱逐：计数加1
  if (frame_info.is_evictable_ && !set_evictable) {
    curr_size_--;
  } else if (!frame_info.is_evictable_ && set_evictable) {
    curr_size_++;
  }
  
  frame_info.is_evictable_ = set_evictable;  // 设置新的可驱逐状态
}

// Remove: 从替换器中完全移除帧（通常在页面被删除时调用）
// 参数：frame_id - 要移除的帧ID
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);  // 线程安全

  auto it = frame_table_.find(frame_id);
  if (it == frame_table_.end()) {
    return;  // 帧不存在，直接返回
  }

  // 安全检查：不可驱逐的帧不能直接移除
  if (!it->second.is_evictable_) {
    return;  // 应该抛出异常，但为了兼容性静默返回
  }

  frame_table_.erase(it);  // 从帧表中移除
  curr_size_--;            // 减少可驱逐帧计数
}

// Size: 返回当前可驱逐帧的数量
// 返回值：可驱逐帧的数量
auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock lock(latch_);  // 线程安全
  return curr_size_;
}

}  // namespace bustub
