//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"
#include "common/logger.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
//  throw NotImplementedException(
//      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
//      "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::FindEmptyFrame(frame_id_t *frame_id) -> bool{
  frame_id_t select_id;
  if(!free_list_.empty()){
    select_id = free_list_.front();
    free_list_.pop_front();
  }else if(replacer_->Size() != 0){
    replacer_->Evict(&select_id);
    if(pages_[select_id].is_dirty_){
      disk_manager_->WritePage(pages_[select_id].page_id_,pages_[select_id].data_);
    }
    /*清除page_table中的记录->保持一个原则，只有在frame中的page才会有记录，不然就是没有*/
    page_table_->Remove(pages_[select_id].page_id_);
    /* 重置page中的data和meta data */
    pages_[select_id].is_dirty_ = false;
    pages_[select_id].pin_count_ = 0;
    pages_[select_id].page_id_ = INVALID_PAGE_ID;
    pages_[select_id].ResetMemory();
  }else{
      return false;
  }
  *frame_id = select_id;
  return true;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  if(free_list_.empty() && replacer_->Size() == 0){
    return nullptr;
  }
  // 选出空闲的frame
  frame_id_t select_frame_id;
  FindEmptyFrame(&select_frame_id);
  // 选出下一个page_id
  page_id_t select_page_id;
  select_page_id = AllocatePage();
  // 准备好page
  pages_[select_frame_id].page_id_ = select_page_id;
  pages_[select_frame_id].is_dirty_ = false;
  pages_[select_frame_id].pin_count_ = 1;//这地方不确定
  // 为新的page准备好LRU和page_table
  page_table_->Insert(select_page_id,select_frame_id);
  replacer_->RecordAccess(select_frame_id);
  replacer_->SetEvictable(select_frame_id,false);

  *page_id = select_page_id;
  return &pages_[select_frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  bool flag = page_table_->Find(page_id,frame_id);
  //这就是已经在buffer pool中的情况
  if(flag && pages_[frame_id].page_id_ == page_id){
    replacer_->RecordAccess(frame_id);
    pages_[frame_id].pin_count_++;
    LOG_DEBUG("Fetch Page %d in frame %d",page_id,frame_id);
    return &pages_[frame_id];
  }
  //如果不在buffer pool中，且如果没有空闲frame，就返回nullptr
  if(!flag && (replacer_->Size() == 0 && free_list_.empty())){
    LOG_DEBUG("Can not Fetch Page %d",page_id);
    return nullptr;
  }
  //选出空闲的frame
  FindEmptyFrame(&frame_id);
  //准备好page
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;
  disk_manager_->ReadPage(page_id,pages_[frame_id].data_);
  //准备好LRU和page_table
  page_table_->Insert(page_id,frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id,false);
  LOG_DEBUG("Fetch Page %d in frame %d",page_id,frame_id);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  bool flag = page_table_->Find(page_id,frame_id);
  //特殊情况处理
  if(!flag || pages_[frame_id].pin_count_ == 0){
    return false;
  }
  //处理page
  pages_[frame_id].pin_count_--;
  pages_[frame_id].is_dirty_ = is_dirty;

  if(pages_[frame_id].pin_count_ == 0){
    replacer_->SetEvictable(page_id,true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  bool flag = page_table_->Find(page_id,frame_id);
  if(!flag){
    return false;
  }
  if(pages_[frame_id].page_id_ != page_id){
    throw std::exception();
  }

  disk_manager_->WritePage(page_id,pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for(size_t i = 0;i < pool_size_;i++){
    if(pages_[i].page_id_ != INVALID_PAGE_ID){
      disk_manager_->WritePage(pages_[i].page_id_,pages_[i].data_);
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  bool flag = page_table_->Find(page_id,frame_id);
  if(!flag){
    return true;
  }
  //防止bug
  if(pages_[frame_id].page_id_ != page_id){
    throw std::exception();
  }
  //如果是pin的就不能删除
  if(pages_[frame_id].pin_count_ != 0){
    return false;
  }
  //根据要求操作
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  free_list_.emplace_back(frame_id);
  //reset page
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].ResetMemory();
  //call DeallocatePage()
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
