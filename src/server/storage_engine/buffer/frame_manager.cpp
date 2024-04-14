#include "include/storage_engine/buffer/frame_manager.h"

FrameManager::FrameManager(const char *tag) : allocator_(tag)
{}

RC FrameManager::init(int pool_num)
{
  int ret = allocator_.init(false, pool_num);
  if (ret == 0) {
    return RC::SUCCESS;
  }
  return RC::NOMEM;
}

RC FrameManager::cleanup()
{
  if (frames_.count() > 0) {
    return RC::INTERNAL;
  }
  frames_.destroy();
  return RC::SUCCESS;
}

Frame *FrameManager::alloc(int file_desc, PageNum page_num)
{
  FrameId frame_id(file_desc, page_num);
  std::lock_guard<std::mutex> lock_guard(lock_);
  Frame *frame = get_internal(frame_id);
  if (frame != nullptr) {
    return frame;
  }

  frame = allocator_.alloc();
  if (frame != nullptr) {
    ASSERT(frame->pin_count() == 0, "got an invalid frame that pin count is not 0. frame=%s",
        to_string(*frame).c_str());
    frame->set_page_num(page_num);
    frame->pin();
    frames_.put(frame_id, frame);
  }
  return frame;
}

Frame *FrameManager::get(int file_desc, PageNum page_num)
{
  FrameId frame_id(file_desc, page_num);
  std::lock_guard<std::mutex> lock_guard(lock_);
  return get_internal(frame_id);
}

/**
 * @brief 页帧驱逐：将 pin_count 为 0 的 frame 从队列中驱除
 * 场景：内存缓冲区已满，需要进行页面置换
  * @param evict_action 表示如何处理脏数据的frame，一般是要刷回磁盘，可参考FileBufferPool::allocate_frame方法中的相关逻辑
 */
int FrameManager::evict_frames(int count, std::function<RC(Frame *frame)> evict_action)
{
  // 对于FrameLruCache的使用，要通过加锁保证线程安全，但注意避免死锁
  std::lock_guard<std::mutex> lock_guard(lock_);

  int evicted = 0;
  
  auto fetcher = [this, &evicted, count, evict_action](const FrameId &frame_id, Frame *frame) -> bool {
    if (frame->can_evict()) {
      if (evict_action(frame) == RC::SUCCESS) {
        frames_.remove(frame_id);
        // 需要调用FrameAllocator的接口彻底释放该Frame
        allocator_.free(frame);
        evicted++;
      }
    }
    return evicted < count;
  };
  frames_.foreach(fetcher);

  return evicted;
}

Frame *FrameManager::get_internal(const FrameId &frame_id)
{
  Frame *frame = nullptr;
  (void)frames_.get(frame_id, frame);
  if (frame != nullptr) {
    frame->pin();
  }
  return frame;
}

/**
 * @brief 查找目标文件的frame
 * FramesCache中选出所有与给定文件描述符(file_desc)相匹配的Frame对象，并将它们添加到列表中
 */
std::list<Frame *> FrameManager::find_list(int file_desc)
{
  std::lock_guard<std::mutex> lock_guard(lock_);

  std::list<Frame *> frames;
  auto fetcher = [&frames, file_desc](const FrameId &frame_id, Frame *const frame) -> bool {
    if (file_desc == frame_id.file_desc()) {
      frame->pin();
      frames.push_back(frame);
    }
    return true;
  };
  frames_.foreach (fetcher);
  return frames;
}

RC FrameManager::free(int file_desc, PageNum page_num, Frame *frame)
{
  FrameId frame_id(file_desc, page_num);

  std::lock_guard<std::mutex> lock_guard(lock_);
  return free_internal(frame_id, frame);
}

RC FrameManager::free_internal(const FrameId &frame_id, Frame *frame)
{
  Frame *frame_source = nullptr;
  [[maybe_unused]] bool found = frames_.get(frame_id, frame_source);
  ASSERT(found && frame == frame_source && frame->pin_count() == 1,
         "failed to free frame. found=%d, frameId=%s, frame_source=%p, frame=%p, pinCount=%d, lbt=%s",
         found, to_string(frame_id).c_str(), frame_source, frame, frame->pin_count(), lbt());

  frame->unpin();
  frames_.remove(frame_id);
  allocator_.free(frame);
  return RC::SUCCESS;
}