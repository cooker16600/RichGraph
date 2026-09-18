#pragma once

#include "core/SSTable.h"
#include <fstream>
#include <iostream>
#include <string>
#include <queue>
#include <stdlib.h>
#include <optional>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <mutex>
#include <fstream>
#include <ctime>
#include <algorithm>
#include <functional>
#include <memory>

#include "core/utils.h"
#include "core/types.h"
#include "core/cache/BufferManager.h"
#include "core/graph/edge.h"
#include "core/graph/edge_record.h"
#include "core/version_set.h"
#include "util/livegraph/futex.hpp"
#include "util/concurrent_queue.h"
#include "util/atomic.hpp"
#include "core/cache/SSTDataManager.h"
#include "core/superversion.h"
#include "core/del_record_manage.h"
#include "core/fixed_property_layout.h"
#include "core/storage_internal.h"
#include "core/io/file_io.h"

using Futex = livegraph::Futex;


namespace lsmgraph {
class SuperVersion;

struct Anchor{
  VertexId_t begin;
  size_t range_size;

  Anchor () {}

  Anchor (VertexId_t _begin, size_t _range_size) 
          : begin(_begin), range_size(_range_size) {}

  void print() {
    std::cout << " Anchor: begin=" << begin
              << " range_size=" << range_size
              << std::endl;
  }
};



struct WayAnchor{
  VertexId_t begin;
  VertexId_t end;
  EdgeOffset_t offset;    // edge offset

  VertexId_t index_start_offset;    // index id
  size_t index_num;       // index num
  size_t range_size;      // edge_offset range size
  std::string path;
  FileId_t fid;
  bool is_input_0;
  std::vector<Index>* index_ptr;

  WayAnchor () {}

  WayAnchor (VertexId_t _begin, VertexId_t _end, 
             EdgeOffset_t _offset,
             VertexId_t _index_start_offset,
             size_t _index_num,
             size_t _range_size,
             std::string _path,
             FileId_t _fid,
             bool _is_input_0,
             std::vector<Index>* index_ptr_)
             :begin(_begin), 
              end(_end), 
              offset(_offset), 
              index_start_offset(_index_start_offset),
              index_num(_index_num),
              range_size(_range_size),  
              path(_path),
              is_input_0(_is_input_0),
              fid(_fid),
              index_ptr(index_ptr_) {}

  void print() {
    std::cout << " WayAnchor: begin=" << begin
              << " end=" << end
              << " offset=" << offset
              << " _index_start_offset=" << index_start_offset
              << " _index_num=" << index_num
              << " range_size=" << (range_size/EDGEBODY_SIZE)
              << " _fid=" << fid
              << " is_input_0=" << is_input_0
              << std::endl;
  }
};

class Way{
public:
    Way(SSTableCache *c,
        BufferManager& buffer_manager,
        bool is_input_0):
            remain_edge_cnt_(c->header.size),
            total_edge_cnt_(c->header.size),
            total_index_cnt_(c->header.index_size),
            index_start_offset_(0),
            edge_start_offset_(0),
            path_(c->path),
            fid_(c->header.timeStamp),
            buffer_manager_(buffer_manager),
            is_input_0_(is_input_0),
            index_cp(&c->indexes){
             }

    Way(BufferManager& buffer_manager,
        size_t index_num,
        EdgeOffset_t index_offset,
        size_t edge_num,
        std::string& path,
        FileId_t fid,
        bool is_input_0,
        std::vector<Index>* index_cp_):
            total_index_cnt_(index_num),
            index_start_offset_(index_offset),
            remain_edge_cnt_(edge_num),
            total_edge_cnt_(edge_num),
            path_(path),
            fid_(fid),
            buffer_manager_(buffer_manager),
            is_input_0_(is_input_0),
            index_cp(index_cp_) {
             }

    void Init () {
        property_num_ = GetActiveSubPropertyNum();

        for(int i = 0; i < property_num_; i++){
          prop_read_cnt_.push_back(0);
          prop_buffer_ptr_.push_back(0);
        }

        p_file_fds.reserve(property_num_);
        for(int i = 0; i < property_num_; i++) {
          p_file_fds.push_back(-1);
        }
        assert(total_index_cnt_ <= MAX_INDEX_NUM);

        e_file_fd = open(path_.c_str(), O_RDONLY);
        assert(e_file_fd >= 0);
        for(int i = 0; i < property_num_; i++) {
          p_file_fds[i] = open((path_+"_p_"+std::to_string(i)).c_str(), O_RDONLY);
          assert(p_file_fds[i] >= 0);
        }
        


        index_ptr_ = index_start_offset_;

        assert(total_index_cnt_ > 0);
        edge_start_offset_ = (*index_cp)[index_ptr_].offset;
        body_buffer_ = new EdgeBody_t[BODY_BUFFER_SIZE];
        FillBodyBuffer();
        assert(total_edge_cnt_ > 0);
        int now_edge_cnt = edge_start_offset_ / EDGEBODY_SIZE;
        property_start_offset_.push_back(
            GetSubPropertyOffsetByEdgeOrdinal(now_edge_cnt, 0));
        
        for(int i = 1; i < property_num_; i++){
          property_start_offset_.push_back(
              GetSubPropertyOffsetByEdgeOrdinal(now_edge_cnt, i));
        }

        property_buffer_.resize(property_num_, std::vector<char>(PROPERTY_BUFFER_SIZE));

        for(int i = 0; i < property_num_; i++){
          FillPropertyBuffer(i);
        }
    }

    void check_vaild(){
      std::cout<<"---Way Block---\n";
      std::cout<<"index_cnt:"<<total_index_cnt_<<"--\n";
      std::cout<<"index_start_offset:"<<index_start_offset_<<"--\n";
      std::cout<<"edge_cnt:"<<total_edge_cnt_<<"--\n";
    }

    void FreeBuffer() {
        if(body_buffer_ != nullptr) {
          delete[] body_buffer_;
          body_buffer_ = nullptr;
        }
        if (e_file_fd != -1) {
          close(e_file_fd);
          e_file_fd = -1;
          for(size_t i = 0; i < p_file_fds.size(); i++) {
            close(p_file_fds[i]);
            p_file_fds[i] = -1;
          }
        }
      has_released = true;
    }

    bool IsReleased(){
      return has_released;
    }

    ~Way() {
        if (e_file_fd != -1) {
          close(e_file_fd);
          for(size_t i = 0; i < p_file_fds.size(); i++) {
            close(p_file_fds[i]);
          }
        }
        
    }

    auto NextEdgeRecord() -> std::optional<EdgeRecord> {
        if(EdgeBodyOffset() >= (*index_cp)[index_ptr_+1].offset) {
            // need to change src vertex
            index_ptr_++;
            if(index_ptr_ >= index_start_offset_ + total_index_cnt_-1) {
                return std::nullopt;
            }
        }
        int old_ptr = body_ptr_;
        auto edge_body = CurEdgeBody();

        bool restore = false;

        VertexId_t old_dst = edge_body->get_dst();
        SequenceNumber_t old_seq = edge_body->get_seq();
        Marker_t old_maker = edge_body->get_marker();
        bool old_is_out = edge_body->get_is_out();
        uint8_t old_edge_type = edge_body->get_edge_type();
        EdgePropertyOffset_t old_offset = edge_body->get_Offset();

        remain_edge_cnt_--;
        if(body_ptr_ == BODY_BUFFER_SIZE-1) {
            FillBodyBuffer();
            restore = true;
        } else {
            body_ptr_++;
        }

        if(remain_edge_cnt_ == 0) {
          for(int i = 0; i < property_num_; i++) {
            lseek(p_file_fds[i], 0L, SEEK_END);
          }
        }

        std::vector<Slice>props;


        for(int i = 0; i < property_num_; i++){
           size_t prop_len;
           // 当前阶段所有子属性都是定长，属性长度由统一布局模块给出。
           prop_len = GetSubPropertyFixedLength(i);
          // Refill from the current property's first byte when it crosses the
          // in-memory buffer boundary.
          if(prop_buffer_ptr_[i] + prop_len > PROPERTY_BUFFER_SIZE) {
              FillPropertyBuffer(i);
          }
          assert(prop_len < 100000);
          auto prop = Slice(property_buffer_[i].data() + prop_buffer_ptr_[i], prop_len);
          props.push_back(prop);
          prop_read_cnt_[i] += prop_len;
          prop_buffer_ptr_[i] += prop_len;
        }
        EdgeRecord er((*index_cp)[index_ptr_].key, old_dst, old_seq, props,
                      old_maker, old_is_out, old_edge_type);
        return std::optional<EdgeRecord>(er);
    }

    FileId_t GetFildId() {
        return fid_; 
    }

    bool Is_input_0() {
      return is_input_0_;
    }

private:
    auto CurSrcVertex() const -> VertexId_t{
        assert(index_ptr_ >= 0);
        return (*index_cp)[index_ptr_].key;
    }

    auto EdgeBodyOffset() -> EdgeOffset_t {
        return edge_start_offset_ 
                  + (total_edge_cnt_-remain_edge_cnt_)*EDGEBODY_SIZE;
    }

    auto CurEdgeBody() const -> EdgeBody_t* {
        assert(body_ptr_ < BODY_BUFFER_SIZE);
        return body_buffer_ +body_ptr_;
    }



    void FillBodyBuffer() {
        auto body_cnt = std::min(BODY_BUFFER_SIZE, remain_edge_cnt_);
        lseek(e_file_fd, 
              (total_edge_cnt_-remain_edge_cnt_)*EDGEBODY_SIZE
                + edge_start_offset_, 
              SEEK_SET);
        auto read_bytes = read(e_file_fd, body_buffer_, body_cnt*EDGEBODY_SIZE);
        assert(read_bytes == body_cnt*EDGEBODY_SIZE);
        body_ptr_ = 0;

    }

    void FillPropertyBuffer(int id) {
        lseek(p_file_fds[id], prop_read_cnt_[id] + property_start_offset_[id], SEEK_SET);
        auto read_bytes = read(p_file_fds[id], 
                               property_buffer_[id].data(), 
                               PROPERTY_BUFFER_SIZE);
        assert(read_bytes != 0);
        prop_buffer_ptr_[id] = 0;
    }
private:
    int e_file_fd = -1;
    std::vector<int> p_file_fds;
    int property_num_ = 0;

    size_t index_ptr_;  // 指向buffer中待合并的第一个元素

    EdgeBody_t *body_buffer_ = nullptr;
    size_t body_ptr_;

    std::vector<std::vector<char>>property_buffer_ ;
    std::vector<size_t> prop_buffer_ptr_;

    const size_t total_edge_cnt_;
    const size_t total_index_cnt_;

    EdgeOffset_t edge_start_offset_;
    std::vector<EdgeOffset_t> property_start_offset_;
    const EdgeOffset_t index_start_offset_;

    size_t remain_edge_cnt_;
    std::vector<size_t> prop_read_cnt_;

    std::string path_;
    FileId_t fid_;
    BufferManager& buffer_manager_;

    bool is_input_0_ = false;
    bool has_released = false;
    std::vector<Index>* index_cp; // property file fds
}; // Way


// Writes a multi-way merge into one or more size-bounded SSTs. A compaction
// creates one writer and may flush each output file in multiple buffer-sized
// chunks.
class SSTableWriter {
public:
    SSTableWriter(std::vector<std::vector<SSTableCache*>*>& fileMetaCache,
                  LevelIndex *vid_to_levelIndex,
            #ifdef MMAP_LEVEL_INDEX
                  MulLevelIndexSharedArray& vid_to_mullevelIndex,
            #elif defined(MMAP_DIFF_SIZE_LEVEL_INDEX)
                  MulLevelIndexArrayWrapper* vid_to_mullevelIndex,
            #elif defined(MMAP_COLD_HOT_LEVEL_INDEX)
                  MulLevelIndexWrapper* vid_to_mullevelIndex,
            #else
                  LazyMulLevelIndexArray *vid_to_mullevelIndex,
            #endif
                  Futex *vertex_futexes,
                  RWLock_t *vertex_rwlocks,
                  Level_t *vertex_max_level,
                  size_t vertex_lock_count,
                  FileId_t min_level_0_fid,
                  int level,
                  BufferManager& buffer_manager,
                  std::vector<std::mutex*>& tablecache_mutex,
                  VersionEdit& version_edit,
                  SSTDataManager& sstdata_manager,
                  std::map<FileId_t, bool>& sst_is_vaild_to_ins_lf,
                  std::mutex* lf_mutex,
                  bool can_be_rewrite = true):
                                    level_(level),
                                    fileMetaCache_(fileMetaCache),
                                    vid_to_levelIndex_(vid_to_levelIndex),
                                    vid_to_mullevelIndex_(vid_to_mullevelIndex),
                                    vertex_futexes_(vertex_futexes),
                                    vertex_rwlocks_(vertex_rwlocks),
                                    vertex_max_level_(vertex_max_level),
                                    vertex_lock_count_(vertex_lock_count),
                                    min_level_0_fid_(min_level_0_fid),
                                    buffer_manager_(buffer_manager),
                                    tablecache_mutex_(tablecache_mutex),
                                    version_edit_(version_edit),
                                    sstdata_manager_(sstdata_manager),
                                    sst_is_vaild_to_ins_lf_(sst_is_vaild_to_ins_lf),
                                    lf_mutex_(lf_mutex),
                                    can_be_rewrite_(can_be_rewrite),
                                    property_num_(GetActiveSubPropertyNum()) {

        edge_body_buffer_ = buffer_manager_.GetEdgeBodyBuffer();
        edge_index_buffer_ = buffer_manager_.GetMaxIndexBuffer();
        assert(property_num_ > 0);
        p_file_content_.resize(property_num_, std::vector<char>(PROPERTY_BUFFER_SIZE));
        p_ptr_.reserve(property_num_);
        p_file_fds.reserve(property_num_);
        for(int i = 0; i < property_num_; i++) {
            p_file_size_.push_back(0);
            p_ptr_.push_back(0);
            p_file_fds.push_back(-1);
        }                              
    }

    void Init(const std::string& efile_name, const std::string &pfile_name, 
              VertexId_t first_src, const uint64_t timestamp) {
        timestamp_ = timestamp;
        edge_ptr_ = 0;
        edge_cnt_ = 0;
        index_cnt_ = 0;
        for(int i = 0; i < property_num_; i++) {
            p_file_size_[i] = 0;
            p_ptr_[i] = 0;
        }
        cur_src_vtx_ = INVALID_VERTEX_ID;
        first_src_ = first_src;

        e_file_fd = -1;
        for(int i = 0; i < property_num_; i++){
          p_file_fds[i] = -1;
        }
        path_ = std::move(efile_name);
        e_file_fd = open(path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        assert(e_file_fd > 0);
        for(int i = 0; i < property_num_; i++){
          p_file_fds[i] = open(
              (pfile_name + "_" + std::to_string(i)).c_str(),
              O_WRONLY | O_CREAT | O_TRUNC,
              0644);
          assert(p_file_fds[i] > 0);
        }
    }

    ~SSTableWriter() {
        buffer_manager_.FreeEdgeBodyBuffer(edge_body_buffer_);
        buffer_manager_.FreeMaxIndexBuffer(edge_index_buffer_);

      p_file_content_.clear(); // 清空 vector
    };


    auto Write(EdgeRecord &edge_record) -> bool {
        auto const src = edge_record.src_;
        if(src != cur_src_vtx_) {
            if(CurSize() >= MAX_EFILE_SiZE) {
                WriteEnd();
                return false;
            }

            cur_src_vtx_ = src;
            edge_index_buffer_[index_cnt_++] = Index{src, (EdgeOffset_t)(edge_cnt_ * EDGEBODY_SIZE)};
            assert(index_cnt_ < MAX_INDEX_NUM);
        }
        newest_edge = std::max(newest_edge, edge_record.seq_);
        EdgePropertyOffset_t *Offset; 
        Offset = new EdgePropertyOffset_t[property_num_];
        for(int i = 0; i < property_num_; i++){

          const auto fixed_len = static_cast<size_t>(GetSubPropertyFixedLength(i));
          if(p_ptr_[i] + fixed_len >= PROPERTY_BUFFER_SIZE) {
              auto write_bytes = write(p_file_fds[i], p_file_content_[i].data(), p_ptr_[i]);
              p_ptr_[i] = 0;
          }
          assert(p_ptr_[i] + fixed_len < PROPERTY_BUFFER_SIZE);
          

          


          // 定长写入策略：
          // 1) 若源属性更短，尾部补 0；
          // 2) 若源属性更长，截断到定长；
          // 3) 偏移严格按定长累计，便于统一随机访问公式。
          WritePaddedSubPropertySlot(
              p_file_content_[i].data() + p_ptr_[i],
              edge_record.props_[i],
              i);
          
          

          
          p_ptr_[i] += fixed_len;
          assert(p_ptr_[i] < PROPERTY_BUFFER_SIZE);
          Offset[i] = p_file_size_[i];
          p_file_size_[i] += fixed_len;
        }
        EdgeBody_t edgebody(edge_record.dst_, edge_record.seq_, Offset[0],
                            edge_record.marker_, edge_record.is_out_,
                            edge_record.edge_type_);
        edge_body_buffer_[edge_ptr_++] = edgebody;

        if(edge_ptr_ >= BODY_BUFFER_SIZE - 1) { // 为哨兵留一个
            auto write_bytes = write(e_file_fd, edge_body_buffer_, edge_ptr_*EDGEBODY_SIZE);
            edge_ptr_ = 0;
        }

        edge_cnt_++;
        delete[] Offset;
        Offset = nullptr;
        return true;
    }

    void change_sst_state(FileId_t sst_id, bool state){
      std::lock_guard<std::mutex> lock(*lf_mutex_);
      if(state){
        sst_is_vaild_to_ins_lf_.insert({sst_id, true});
      } else {
        auto it = sst_is_vaild_to_ins_lf_.find(sst_id);
        if(it != sst_is_vaild_to_ins_lf_.end()){
          sst_is_vaild_to_ins_lf_.erase(it);
        }
      }
    }

    void WriteEnd() {
        assert(edge_ptr_ + 1 < BODY_BUFFER_SIZE);
        EdgePropertyOffset_t *Offset; 
        Offset = new EdgePropertyOffset_t[property_num_];
        for(int i = 0; i < property_num_; i++){
          Offset[i] = p_file_size_[i];
        }
        
        EdgeBody_t edgebody(INVALID_VERTEX_ID, 0, Offset[0], 0, true, 0);
        edge_body_buffer_[edge_ptr_++] = edgebody;
        auto write_bytes = write(e_file_fd, edge_body_buffer_, edge_ptr_*EDGEBODY_SIZE);

        assert(index_cnt_ + 1 < MAX_INDEX_NUM);
        edge_index_buffer_[index_cnt_++] = 
            Index{INVALID_VERTEX_ID, (EdgeOffset_t)(edge_cnt_ * EDGEBODY_SIZE)}; // index哨兵
        write_bytes = write(e_file_fd, edge_index_buffer_, 
                            index_cnt_*sizeof(Index));

        SSTableCache *temp_filemeta_cache = new SSTableCache(sstdata_manager_, newest_edge);

      #ifdef MULTI_LEVEL_VERSION
        version_edit_.AddDownLevelFile(temp_filemeta_cache);
      #endif

        temp_filemeta_cache->path = path_;


        temp_filemeta_cache->indexes.assign(edge_index_buffer_, edge_index_buffer_ + index_cnt_);
        
        
        temp_filemeta_cache->header.size = edge_cnt_ + 1;
        temp_filemeta_cache->header.index_size = index_cnt_;
        temp_filemeta_cache->header.timeStamp = timestamp_;
        temp_filemeta_cache->header.minKey = first_src_;
        temp_filemeta_cache->header.maxKey = cur_src_vtx_;
        const Header persisted_header = temp_filemeta_cache->header;
        write_bytes = write(e_file_fd,
                            &persisted_header,
                            sizeof(persisted_header));
        assert(write_bytes == sizeof(Header));
        assert(write_bytes != -1);
        for(int i = 0; i < property_num_; i++){
          write_bytes = write(p_file_fds[i], p_file_content_[i].data(), p_ptr_[i]);
          assert(write_bytes != -1);
          fsync(p_file_fds[i]);
          close(p_file_fds[i]);
        }   

        fsync(e_file_fd);
        close(e_file_fd);
        
        int dir_fd = open(GetCurrentDbPath().data(), O_DIRECTORY | O_RDONLY);
        fsync(dir_fd);
        close(dir_fd);
  
        sstdata_manager_.put_data(timestamp_, temp_filemeta_cache->header.size,
                              reinterpret_cast<uintptr_t>(temp_filemeta_cache), newest_edge);
        if(can_be_rewrite_){
          change_sst_state(timestamp_, true);
        }

        // Legacy non-multiversion indexes are mutated in place and therefore
        // require synchronization with concurrent readers.
        if (FLAGS_support_mulversion== false) {
          for(int32_t i = 0; i < index_cnt_ - 1; ++i) {
            // 需要重置level层的索引为无效值，并更新level+1层的索引
            int index_id = edge_index_buffer_[i].key * LEVEL_INDEX_SIZE + level_;
            if (level_ > 0) {
              FileId_t input0_fid = vid_to_levelIndex_[index_id-1].get_fileID();
              if (input_0_fidset_.find(input0_fid) != input_0_fidset_.end()) { 
                  // TODO(correctness): Protect legacy level-index mutations
                  // from concurrent readers.
                  vid_to_levelIndex_[index_id-1].set_fileID(INVALID_File_ID);
              }
            }
            LevelIndex& findex = vid_to_levelIndex_[index_id];
            FileId_t fid = timestamp_;
            // TODO(correctness): Protect legacy level-index mutations from
            // concurrent readers.
            findex.set_fileID(fid);
            findex.set_offset(edge_index_buffer_[i].offset);
            findex.set_next_offset(edge_index_buffer_[i+1].offset);
            write_max(&vertex_max_level_[edge_index_buffer_[i].key], Level_t(level_+2));
          }
        } else {
#if defined(MMAP_DIFF_SIZE_LEVEL_INDEX) || defined(MMAP_COLD_HOT_LEVEL_INDEX)
          FileId_t fid = timestamp_;
          // 批量申请空间
  #ifdef MMAP_DIFF_SIZE_LEVEL_INDEX
          std::vector<std::vector<std::pair<VertexId_t, 
                                  char*>>> level_index_free_ids(MAX_LEVEL);
  #elif defined(MMAP_COLD_HOT_LEVEL_INDEX)
          std::vector<std::pair<VertexId_t, char*>> level_index_free_ids;
          int used_ptr = 0;
  #endif


  #ifdef MMAP_DIFF_SIZE_LEVEL_INDEX
          for(int32_t i = 0; i < index_cnt_ - 1; ++i) {
    #ifdef OPT_MERGE_MULTI_LEVEL
            // 这个在diff size下可能需要修改
            for (int levelID = 0; levelID < LEVEL_INDEX_SIZE; levelID++) {
              if (levelID == level_ - 1 || levelID == level_) {
                continue;
              }
              if (findex.getCopyFlag(levelID)) {
                findex.set_fileID(levelID, INVALID_File_ID);
                findex.setCopyFlag(levelID, 0);
              }
            }
    #endif
            write_max(&vertex_max_level_[edge_index_buffer_[i].key], 
                      Level_t(level_+2));
            update_level_index(edge_index_buffer_[i].key, 
                                  level_+1 /*compact to this level*/,
                                  fid,
                                  edge_index_buffer_[i].offset,
                                  edge_index_buffer_[i+1].offset,
                                  min_level_0_fid_+1
                                  ,level_index_free_ids);
          }
          // 剩余的需要回收
          for (int i = 0; i < level_index_free_ids.size(); i++) {
            for (auto& id : level_index_free_ids[i]) {
              vid_to_mullevelIndex_->recycle_array_id_by_array_id(i + 1, 
                                                                  id.first
                                                                  );
            }
          }
          level_index_free_ids.clear();

  #elif defined(MMAP_COLD_HOT_LEVEL_INDEX)
          for(int32_t i = 0; i < index_cnt_ - 1; ++i) {
    #ifdef OPT_MERGE_MULTI_LEVEL
            // 这个在diff size下可能需要修改
            for (int levelID = 0; levelID < LEVEL_INDEX_SIZE; levelID++) {
              if (levelID == level_ - 1 || levelID == level_) {
                continue;
              }
              if (findex.getCopyFlag(levelID)) {
                findex.set_fileID(levelID, INVALID_File_ID);
                findex.setCopyFlag(levelID, 0);
              }
            }
    #endif
            write_max(&vertex_max_level_[edge_index_buffer_[i].key], 
                      Level_t(level_+2));
            update_level_index_by_hot(edge_index_buffer_[i].key, 
                                  level_+1 /*compact to this level*/,
                                  fid,
                                  edge_index_buffer_[i].offset,
                                  edge_index_buffer_[i+1].offset,
                                  min_level_0_fid_+1
                                  ,level_index_free_ids
                                  ,used_ptr);
          }
          // 剩余的需要回收
          for (int i = used_ptr; i < level_index_free_ids.size(); i++) {
            vid_to_mullevelIndex_->recycle_array_id_by_array_id(0, 
                                      level_index_free_ids[i].first,
                                      HOT_LEVEL_NUM, false);
          }
          level_index_free_ids.clear();
  #endif
#else
          for(int32_t i = 0; i < index_cnt_ - 1; ++i) {
            VertexRWLock(edge_index_buffer_[i].key).WriteLock();
            MulLevelIndex& findex = 
                               (*vid_to_mullevelIndex_)[edge_index_buffer_[i].key];

#ifdef OPT_MERGE_MULTI_LEVEL
            for (int levelID = 0; levelID < LEVEL_INDEX_SIZE; levelID++) {
              if (levelID == level_ - 1 || levelID == level_) {
                continue;
              }
              if (findex.getCopyFlag(levelID)) {
                findex.set_fileID(levelID, INVALID_File_ID);
                findex.setCopyFlag(levelID, 0);
              }
            }
#endif
            // 注意: 需要重置level层的索引为无效值，并更新level+1层的索引
            //   重置的条件是: 需要保证上一层文件的fid在本次压缩的集合中，否则表示其并为参与  
            if (level_ > 0 && can_be_rewrite_) {
              FileId_t input0_fid = findex.get_fileID(level_-1);
              // Files within a nonzero level have disjoint key ranges.
             if (input_0_fidset_.find(input0_fid) != input_0_fidset_.end()) {
                  findex.set_fileID(level_-1, INVALID_File_ID);
             }
            }
            FileId_t fid = timestamp_;
            if (level_ == 0 && can_be_rewrite_) {
              findex.set_min_level_0_fid(min_level_0_fid_+1);
            }
            if(can_be_rewrite_){
              findex.set_fileID(level_, fid);
              findex.set_offset(level_, edge_index_buffer_[i].offset);
              findex.set_next_offset(level_, edge_index_buffer_[i+1].offset);
              write_max(&vertex_max_level_[edge_index_buffer_[i].key], Level_t(level_+2));
              
            }
            VertexRWLock(edge_index_buffer_[i].key).WriteUnlock();
          }
#endif
      }


      // Serialize publication into the destination level.
      std::lock_guard<std::mutex> lock(*tablecache_mutex_[level_+1]);
      assert(fileMetaCache_.size() > level_+1);
      fileMetaCache_[level_+1]->push_back(temp_filemeta_cache);
      std::sort(fileMetaCache_[level_+1]->begin(), 
                fileMetaCache_[level_+1]->end(), 
                [](const SSTableCache *a, const SSTableCache *b){
                  return (a->header).minKey < (b->header).minKey; // Sort minkey from small to large 
      });
      delete[] Offset;
      Offset = nullptr;
    }

#ifdef MMAP_DIFF_SIZE_LEVEL_INDEX
    void print_level_index(VertexId_t vid) {
      std::cout << "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++" << std::endl;
      int level_num = vid_to_mullevelIndex_->get_level_num_by_vid(vid);
      if (level_num > 0) {
        char* ptr = vid_to_mullevelIndex_->get_level_index_ptr_by_vid(vid);
        MulLevelIndexWithDiffSize level_index = MulLevelIndexWithDiffSize(ptr, level_num);
        level_index.print();
      }
      std::cout << "-----------------------------------------------------------" << std::endl;
    }

    void update_level_index(VertexId_t vid, 
                            int this_level_id /*compact to this level*/,
                            FileId_t fid,
                            uint32_t offset,
                            uint32_t next_offset,
                            FileId_t min_level_0_fid
                            ,std::vector<std::vector<std::pair<VertexId_t, 
                                char*>>>& level_index_free_ids) {
      int level_num = vid_to_mullevelIndex_->get_level_num_by_vid(vid);

      int batch_size = 200;

      if (level_num == 0) { 
        // 之前一个level也没有，则构建一个新的，然后本层的信息填进去即可
        VertexId_t array_id= 0;
        level_num = 1; // 申请的level > 0
        char* ptr = nullptr;

        if (batch_size <= 1) {
          // way1: 
          ptr = vid_to_mullevelIndex_->
            get_new_level_index_ptr_by_vid_and_level_num(vid, level_num, 
                                                         array_id);
        } else {
          // way2
          // 申请空间，先检查free list
          if (level_index_free_ids[level_num-1].size() == 0) {
            // 填充free list
            vid_to_mullevelIndex_->get_batch_new_level_index_ptr_by_level_num(
                level_index_free_ids[level_num-1], level_num, batch_size);
          }
          // 获得id和地址
          auto& pari = level_index_free_ids[level_num-1].back();
          array_id = pari.first;
          ptr = pari.second;
          level_index_free_ids[level_num-1].pop_back();
        }

        if (ptr != nullptr) {
          MulLevelIndexWithDiffSize level_index = MulLevelIndexWithDiffSize(ptr, level_num);
          level_index.set_level_data(0, this_level_id, fid, offset, next_offset);
          if (this_level_id == 1) {
            level_index.set_min_level_0_fid(min_level_0_fid);
          }
          VertexRWLock(vid).WriteLock();
          vid_to_mullevelIndex_->set_level_num_by_vid(vid, level_num);
          vid_to_mullevelIndex_->set_array_id_by_vid(vid, array_id);
          VertexRWLock(vid).WriteUnlock();
        }
      } else { 
        char* ptr = vid_to_mullevelIndex_->get_level_index_ptr_by_vid(vid);
        MulLevelIndexWithDiffSize level_index = MulLevelIndexWithDiffSize(ptr, level_num);
        bool pre_level_exit = false;
        bool this_level_exit = false;
        bool ignor_pre_level = false;
        for (int i = 0; i < level_num; i++) {
          if (this_level_id - 1 == level_index.get_level_id(i)) {
            pre_level_exit = true;
            if (input_0_fidset_.find(level_index.get_fileID(i)) 
                == input_0_fidset_.end()) {
              ignor_pre_level = true;
            }
          } else if (this_level_id == level_index.get_level_id(i)) {
            this_level_exit = true;
          }
        }
        int new_level_num = level_num 
            + (pre_level_exit == false && this_level_exit == false)  // L-0和L-1都没有时
            - (pre_level_exit == true && this_level_exit == true
               && ignor_pre_level == false);   // l-1-n上下两层都有时合并


        if (new_level_num != level_num) {
          // 需要更新大小
          VertexId_t array_id = 0;
          char* ptr = nullptr;

          if (batch_size <= 1) {
            // way1
            ptr = vid_to_mullevelIndex_->
                get_new_level_index_ptr_by_vid_and_level_num(vid, new_level_num, 
                                                             array_id);
          } else {
            // way2
            if (level_index_free_ids[new_level_num-1].size() == 0) {
              // 填充free list
              vid_to_mullevelIndex_->get_batch_new_level_index_ptr_by_level_num(
                level_index_free_ids[new_level_num-1], new_level_num, batch_size);
            }
            // 获得id和地址
            auto& pari = level_index_free_ids[new_level_num-1].back();
            array_id = pari.first;
            ptr = pari.second;
            level_index_free_ids[new_level_num-1].pop_back();
          }

          MulLevelIndexWithDiffSize new_level_index 
            = MulLevelIndexWithDiffSize(ptr, new_level_num);
          // copy old-index to new-index
          int new_level_index_id = 0;
          for (int i = 0; i < level_num; i++) {
            if (this_level_id - 1 == level_index.get_level_id(i)) {
              continue;
            } else if (this_level_id == level_index.get_level_id(i)) {
              // update 本层
              new_level_index.set_level_data(new_level_index_id++, 
                                         this_level_id, fid, offset, next_offset);
            } else {
              // copy othres index
              memcpy(new_level_index.get_level_data_ptr(new_level_index_id++),
                     level_index.get_level_data_ptr(i),
                     sizeof(MulLevelIndexWithDiffSize::LevelData));
            }
          }
          assert(new_level_index_id <= new_level_num);
          if (new_level_index_id < new_level_num) {
            assert(pre_level_exit == false && this_level_exit == false);
            // 属于增加的情况(可能发生在L0-L1压缩，并且这个点在L1没有索引，在其它层有索引)
            new_level_index.set_level_data(new_level_index_id++, 
                                       this_level_id, fid, offset, next_offset);
          }
          if (this_level_id == 1) {
            new_level_index.set_min_level_0_fid(min_level_0_fid);
          } else {
            new_level_index.set_min_level_0_fid(level_index.get_min_level_0_fid());
          }
          // 加锁
          VertexRWLock(vid).WriteLock();
          // 回收旧索引项，添加新的数组索引项
          vid_to_mullevelIndex_->recycle_array_id(vid, level_num);
          vid_to_mullevelIndex_->set_level_num_by_vid(vid, new_level_num);
          vid_to_mullevelIndex_->set_array_id_by_vid(vid, array_id);
          // 解锁
          VertexRWLock(vid).WriteUnlock();
        } else {
          // 加锁
          VertexRWLock(vid).WriteLock();
          //  修改索引内容
          // 不需要更新大小, 仅仅更新本层的Index内容信息就行了
          for (int i = 0; i < level_num; i++) {
            if (this_level_id - 1 == level_index.get_level_id(i) 
                && this_level_exit == false) {
              assert(pre_level_exit == true 
                     && (this_level_exit == false || ignor_pre_level == true));
              // Initialize an index entry for a previously empty level.
              level_index.set_level_data(i, this_level_id, 
                                         fid, offset, next_offset);
              break; // 一定只需要修改一项
            } else if (this_level_id == level_index.get_level_id(i)) {
              // update 本层
              level_index.set_level_data(i, this_level_id, 
                                         fid, offset, next_offset);
              break; // 一定只需要修改一项
            } else {
              // 不需要动
            }
          }
          if (this_level_id == 1) {
            level_index.set_min_level_0_fid(min_level_0_fid);
          }
          // 解锁
          VertexRWLock(vid).WriteUnlock();
        }
      }
    }
#endif


#ifdef MMAP_COLD_HOT_LEVEL_INDEX
    void update_level_index_by_hot(VertexId_t vid, 
                            int this_level_id /*compact to this level*/,
                            FileId_t fid,
                            uint32_t offset,
                            uint32_t next_offset,
                            FileId_t min_level_0_fid
                            ,std::vector<std::pair<VertexId_t, char*>>&
                              level_index_free_ids,
                            int& used_ptr) {
      int batch_size = 32;

      VertexId_t old_array_id= 0;
      int old_level_num = 0;
      char* ptr = vid_to_mullevelIndex_->\
          get_level_index_ptr_by_vid(vid, old_level_num, old_array_id);
      MulLevelIndexWithDiffSize level_index 
          = MulLevelIndexWithDiffSize(ptr, old_level_num);

      if (old_level_num != COLD_LEVEL_NUM && old_level_num != HOT_LEVEL_NUM) {
        throw std::runtime_error("Error level_num="+std::to_string(old_level_num));
      }
      
      // 遍历每一个index, 将this_level_id填进去-空位置/或者自己原本的位置
      // 如果: pre_level_id 有数据且它的fid in set 则需要置空
      // 最后看看数量，如果变热/变冷，则需要迁移

      VertexRWLock(vid).WriteLock();

      int real_level_num = 0;
      bool is_add = false;
      for (int i = 0; i < old_level_num; i++) {
        if (level_index.get_fileID(i) != INVALID_File_ID) {
          if (this_level_id == level_index.get_level_id(i)) {
            if (is_add == false) {
              level_index.set_level_data(i, this_level_id, fid, 
                                        offset, next_offset);
              is_add = true;
              real_level_num++;
            } else {
              level_index.set_level_data(i, 0, INVALID_File_ID, 
                                        INVALID_OFFSET, INVALID_OFFSET);
            }
          } else if (level_index.get_level_id(i) == this_level_id - 1 
            && input_0_fidset_.find(level_index.get_fileID(i)) 
               != input_0_fidset_.end()) {
            if (is_add == false) {
              level_index.set_level_data(i, this_level_id, fid, 
                                        offset, next_offset);
              is_add = true;
              real_level_num++;
            } else {
              level_index.set_level_data(i, 0, INVALID_File_ID, 
                                        INVALID_OFFSET, INVALID_OFFSET);
            }
          } else {
            real_level_num++;
          }
        } else {
          if (is_add == false) {
              level_index.set_level_data(i, this_level_id, fid, 
                                        offset, next_offset);
            is_add = true;
            real_level_num++;
          }
        }
      }

      real_level_num += (!is_add);

      if (old_level_num == COLD_LEVEL_NUM && real_level_num > COLD_LEVEL_NUM) {
        // cold to hot
        VertexId_t array_id= INVALID_VERTEX_ID;

        char* ptr = nullptr;

        if (batch_size > 1) {
          if (used_ptr == batch_size || level_index_free_ids.size() == 0) {
            level_index_free_ids.clear();
            used_ptr = 0;
            vid_to_mullevelIndex_->get_batch_new_level_index_ptr_by_level_num(
                level_index_free_ids, batch_size);
            assert(level_index_free_ids.size() == batch_size);
          }
          // 获得id和地址
          assert(level_index_free_ids.size() >= 1);
          auto& pari = level_index_free_ids[used_ptr++];
          array_id = pari.first;
          ptr = pari.second;
        } else {
          ptr = vid_to_mullevelIndex_->
                get_new_level_index_ptr_by_vid_and_level_num(vid, HOT_LEVEL_NUM, 
                                                             array_id);
        }

        // get a new hot-index
        MulLevelIndexWithDiffSize new_level_index 
            = MulLevelIndexWithDiffSize(ptr, HOT_LEVEL_NUM);
        new_level_index.init(HOT_LEVEL_NUM);
        // copy to hot_index
        int new_id = 0;
        for (int i = 0; i < old_level_num; i++) {
          if (level_index.get_fileID(i) != INVALID_File_ID) {
            memcpy(new_level_index.get_level_data_ptr(new_id++),
                    level_index.get_level_data_ptr(i),
                    sizeof(MulLevelIndexWithDiffSize::LevelData));
          }
        }
        if (new_id != real_level_num) {
          assert(is_add == false);
          new_level_index.set_level_data(new_id++, 
                                       this_level_id, fid, offset, next_offset);
        }
        assert(new_id == real_level_num);

        // 加入 hash
        vid_to_mullevelIndex_->insert_hot_hash(vid, array_id);


      } else if (old_level_num == HOT_LEVEL_NUM 
                 && real_level_num <= COLD_LEVEL_NUM) {
        // hot to cold
        VertexId_t array_id= INVALID_VERTEX_ID;
        char* ptr = vid_to_mullevelIndex_->
            get_new_level_index_ptr_by_vid_and_level_num(vid, COLD_LEVEL_NUM, 
                                                         array_id);
        // get a new hot-index
        MulLevelIndexWithDiffSize new_level_index 
            = MulLevelIndexWithDiffSize(ptr, HOT_LEVEL_NUM);
        new_level_index.init(COLD_LEVEL_NUM);
        // copy to hot_index
        int new_id = 0;
        for (int i = 0; i < old_level_num; i++) {
          if (level_index.get_fileID(i) != INVALID_File_ID) {
            memcpy(new_level_index.get_level_data_ptr(new_id++),
                    level_index.get_level_data_ptr(i),
                    sizeof(MulLevelIndexWithDiffSize::LevelData));
          }
        }
        assert(new_id == real_level_num);

        vid_to_mullevelIndex_->recycle_array_id_by_array_id(vid, old_array_id, 
                                                            HOT_LEVEL_NUM, 
                                                            true);
      } else {
        assert(is_add == true);
      }

      VertexRWLock(vid).WriteUnlock();


    }
#endif

    bool IsClear() {
      return edge_ptr_ == 0;
    }

    void InsertInput0Fid(FileId_t fid) {
        input_0_fidset_.insert(fid);
    }

private:
    int e_file_fd;
    std::vector<int> p_file_fds;

    EdgeBody_t* edge_body_buffer_;
    uint64_t edge_ptr_ = 0;
    uint64_t edge_cnt_ = 0;

    Index *edge_index_buffer_;
    uint64_t index_cnt_ = 0;

    std::vector<std::vector<char>>p_file_content_;
    std::vector<EdgePropertyOffset_t> p_ptr_;
    // Cumulative property bytes written across all buffered chunks.
    std::vector<EdgePropertyOffset_t>p_file_size_;

    VertexId_t cur_src_vtx_ = INVALID_VERTEX_ID;

    VertexId_t first_src_;
    uint64_t timestamp_;

    std::string path_;
    int level_;
    std::vector<std::vector<SSTableCache*>*>& fileMetaCache_; // Each level has some
    LevelIndex *vid_to_levelIndex_;
  #ifdef MMAP_LEVEL_INDEX
    MulLevelIndexSharedArray vid_to_mullevelIndex_;
  #elif defined(MMAP_DIFF_SIZE_LEVEL_INDEX)
    MulLevelIndexArrayWrapper *vid_to_mullevelIndex_;
  #elif defined(MMAP_COLD_HOT_LEVEL_INDEX)
    MulLevelIndexWrapper* vid_to_mullevelIndex_;
  #else
    LazyMulLevelIndexArray *vid_to_mullevelIndex_; // sparse vid to mullevel_index
  #endif
    Futex *vertex_futexes_;
    RWLock_t *vertex_rwlocks_;
    Level_t *vertex_max_level_;
    size_t vertex_lock_count_ = 1;
    FileId_t min_level_0_fid_;


  

    BufferManager& buffer_manager_;
    std::vector<std::mutex*>& tablecache_mutex_;

    std::unordered_set<int> input_0_fidset_;

    VersionEdit& version_edit_;

    SSTDataManager& sstdata_manager_;

    std::map<FileId_t, bool>& sst_is_vaild_to_ins_lf_;
    std::mutex* lf_mutex_;

    bool can_be_rewrite_;
    int property_num_;

    RWLock_t& VertexRWLock(VertexId_t vid) {
      return vertex_rwlocks_[static_cast<size_t>(vid % vertex_lock_count_)];
    }

    public:
    SequenceNumber_t newest_edge = 0;

    auto CurSize() const -> size_t {
        // Reserve space for the sentinel.
        return HEADER_SIZE+ (edge_cnt_+1)*EDGEBODY_SIZE
               + (index_cnt_+1)*sizeof(Index);
    }
};  // end SSTableWriter

class Compaction {
public:
	  using PreparationCallback =
	      std::function<std::shared_ptr<void>(std::string*)>;
	  Compaction(std::vector<std::vector<SSTableCache*>*>& _fileMetaCache,
	             std::string& _dataDir,
	             uint64_t& _currentTime,
	             std::mutex* level_0_mux_,
	             VersionSet *l0_versionset,
	             std::atomic<SequenceNumber_t>& global_version_id,
	             SSTDataManager& sstdata_manager,
             DelRecordManage& del_record_manager,
             SuperVersion& sv,
             std::mutex* lf_mutex,
             std::map<FileId_t, bool>& sst_is_vaild_to_ins_lf,
             const std::vector<uint32_t>& sub_property_lengths)
                      : state(false),
                      fileMetaCache_(_fileMetaCache), 
                      dataDir(_dataDir), 
                      currentTime(_currentTime),
                      l0_versionset_(l0_versionset),
                      global_version_id_(global_version_id),
                      sv_(sv),
                      sstdata_manager_(sstdata_manager),
                      del_record_manager_(del_record_manager),
                      sub_property_lengths_(sub_property_lengths),
                      buffer_manager(1, MAX_INDEX_NUM,        // max_indexBuffer_
                                     0, MAX_INDEX_NUM,        // indexBuffer
                                     1, BODY_BUFFER_SIZE,     // edgeBodyBuffer_
                                     sub_property_lengths_.size(), PROPERTY_BUFFER_SIZE), // propertyBuffer_
                      large_job_(0),
                      max_job_num_(0),
                      lf_mutex_(lf_mutex),
                      sst_is_vaild_to_ins_lf_(sst_is_vaild_to_ins_lf){
    for (int level = 0; level < MAX_LEVEL; level++) {
      compact_pointer_[level] = MAX_GLOBAL_SEQ;
    }
    max_subcompactions_ = FLAGS_max_subcompactions;
    tablecache_mutex_.emplace_back(level_0_mux_);
    for (int i = 1; i < MAX_LEVEL; i++) {
      tablecache_mutex_.emplace_back(new std::mutex());
    }
  }

  void init (Futex *_vertex_futexes, LevelIndex *_vid_to_levelIndex,
             RWLock_t *_vertex_rwlocks,
             Level_t* _vertex_max_level,
             size_t _vertex_lock_count,
          #ifdef MMAP_LEVEL_INDEX
             MulLevelIndexSharedArray& _vid_to_mullevelIndex){
          #elif defined(MMAP_DIFF_SIZE_LEVEL_INDEX)
             MulLevelIndexArrayWrapper* _vid_to_mullevelIndex){
          #elif defined(MMAP_COLD_HOT_LEVEL_INDEX)
             MulLevelIndexWrapper* _vid_to_mullevelIndex){
          #else
             LazyMulLevelIndexArray *_vid_to_mullevelIndex) {
          #endif
    vertex_futexes = _vertex_futexes;
    vertex_rwlocks_ = _vertex_rwlocks;
    vertex_max_level_ = _vertex_max_level;
    vertex_lock_count_ = _vertex_lock_count;
    vid_to_levelIndex = _vid_to_levelIndex;
    vid_to_mullevelIndex_ = _vid_to_mullevelIndex; // vid to mullevel_index: v_num * 1
  
  #ifdef MMAP_LEVEL_INDEX
  }
#elif defined(MMAP_DIFF_SIZE_LEVEL_INDEX)
  }
#elif defined(MMAP_COLD_HOT_LEVEL_INDEX)
  }
#else
  }
#endif

  bool GetState() {
    return state.load(std::memory_order_acquire);
  }

  bool SetState(bool old_state, bool new_state) {
    return state.compare_exchange_strong(old_state, new_state);
  }

  uint32_t LargeJob() const {
    return large_job_;
  }

  uint32_t l0_auto_compaction_limit() const {
    return l0_auto_compaction_limit_;
  }

  void SetL0AutoCompactionLimit(uint32_t limit) {
    l0_auto_compaction_limit_ = std::max<uint32_t>(1, limit);
  }

  void SetL1AutoCompactionLimit(uint32_t limit) {
    l1_auto_compaction_limit_ = std::max<uint32_t>(1, limit);
  }

  // Called once before a compaction reads any input property file.  The
  // returned lifetime token is retained until the whole compaction finishes.
  // RichGraph uses it to drain property deltas and block new publications.
  void SetPreparationCallback(PreparationCallback callback) {
    preparation_callback_ = std::move(callback);
  }

  int LevelMaxSize(int level) const {
    if (level == 0) {
      return static_cast<int>(l0_auto_compaction_limit_);
    }
    if (level == 1 && l1_auto_compaction_limit_ > 0) {
      return static_cast<int>(l1_auto_compaction_limit_);
    }
    return getLevelMaxSize(level);
  }

  ~Compaction() {
    // TODO(correctness): Retire obsolete files after their final reader
    // releases them instead of deferring all retirement to shutdown.
    RealRemoveFile();
    for (int i = 1; i < MAX_LEVEL; i++) {
      delete tablecache_mutex_[i];
    }
    if (FLAGS_richgraph_verbose) {
      std::cout << "~compaction completed" << std::endl;
    }

#ifdef WRITE_STALL_TEST
    std::cout << "~Compaction" << std::endl;
    auto now = std::chrono::system_clock::now();
    std::time_t now_c = std::chrono::system_clock::to_time_t(now);
    std::tm* now_tm = std::localtime(&now_c);
    char filename[100];
    std::strftime(filename, sizeof(filename), "timings_%Y%m%d_%H%M%S.filenum_microseconds", now_tm);

    std::ofstream outFile(filename);
    if (outFile.is_open()) {
        for (const auto& [fnum, time] : timings) {
            outFile << fnum << "\t" << time << std::endl;
        }
        outFile.close();
    } else {
        std::cerr << "Unable to open file for writing." << std::endl;
    }
    std::cout << " write to: " << filename << std::endl;
#endif
  }

  bool PickCompactioFile(const int level, const bool is_grow);
  void GetRange(const std::vector<SSTableCache*>& inputs,
                VertexId_t& smallest, VertexId_t& largest);
  void GetRange2(const std::vector<SSTableCache*>& inputs1,
                  const std::vector<SSTableCache*>& inputs2,
                  VertexId_t& smallest, VertexId_t& largest);
  void GetOverlappingInputs(const int level, const VertexId_t begin,
                            const VertexId_t end,
                            std::vector<SSTableCache*>* inputs);
  void SetupOtherInputs(const int pre_level, const bool is_grow);
  void BackgroundCompaction();
  void RemoveCache(const int level, const int inputs_index, bool free_space=true);
  void RemoveFile();
  void RealRemoveFile();
  void DirectAdjustLevel(const int level);
  void UpdataLevelIndex(SSTableCache* it, const int level);
  void GenSubcompactionBoundaries(std::vector<WayAnchor>& all_wayAnchors,
                                  std::vector<VertexId_t>& boundaries);
  void PreCompaction(const int level, std::vector<Way>& ways);

  // 具体压缩策略
  void DoCompactionWork_all_priorty(const int level, std::vector<Way>& ways);
  void ProcessCompactionWork(const int level, 
                             const int inputs_0_size, 
                             const int inputs_1_size,
                             std::vector<EdgeRecord>& edge_cache,
                             std::vector<Way>& ways);
  void DoSubCompactionWork(const int level);
  void DoCompactionWorkTwoWay(const int level,
                              const int inputs_0_size,
                              const int inputs_1_size,
                              std::vector<EdgeRecord>& edge_cache,
                              std::vector<Way>& ways);
  void DoGeneralCompactionWork(const int level);
  void ProcessMultiWaysCompaction(std::vector<Way>& ways, const int level);
  void ProcessMultiWaysCompactionOpt(const int level,
                                     std::vector<WayAnchor>& all_wayAnchors,
                                     int anchor_begin,
                                     int anchor_end);

  void MaybeScheduleCompaction();
  bool ForceCompactAllL0ToL1();

  void BottomCompactionWork(const int level,
                              const int inputs_0_size,
                              const int inputs_1_size,
                              std::vector<EdgeRecord>& edge_cache,
                              std::vector<Way>& ways);
  
  void MergeLazyFileBeforeCompaction();

  void Merge_Lazy_file(LazyUpdate* lazyupdate, int sub_property_id);
  void Merge_Lazy_file_of_SST(int level, SSTableCache* sstable, std::vector<SSTableCache*>&new_inputs, bool need_delete_file);
  void Do_Merge_Work_Before_Compaction(int level, bool need_delete_file);
  void change_sst_state(FileId_t sst_id, bool state);
  void clean();
private:
  RWLock_t& VertexRWLock(VertexId_t vid) {
    return vertex_rwlocks_[static_cast<size_t>(vid % vertex_lock_count_)];
  }

#if !defined(MMAP_LEVEL_INDEX) && !defined(MMAP_DIFF_SIZE_LEVEL_INDEX) && \
    !defined(MMAP_COLD_HOT_LEVEL_INDEX)
  MulLevelIndex& MutableMulLevelIndex(VertexId_t vid) {
    return (*vid_to_mullevelIndex_)[vid];
  }
#endif

  std::vector<std::vector<SSTableCache*>*>& fileMetaCache_; // Each level has some
  LevelIndex *vid_to_levelIndex;
  #ifdef MMAP_LEVEL_INDEX
    MulLevelIndexSharedArray vid_to_mullevelIndex_;
  #elif defined(MMAP_DIFF_SIZE_LEVEL_INDEX)
    MulLevelIndexArrayWrapper *vid_to_mullevelIndex_;
  #elif defined(MMAP_COLD_HOT_LEVEL_INDEX)
    MulLevelIndexWrapper* vid_to_mullevelIndex_;
  #else
    LazyMulLevelIndexArray *vid_to_mullevelIndex_; // sparse vid to mullevel_index
  #endif
  Futex *vertex_futexes;
  RWLock_t *vertex_rwlocks_;
  Level_t *vertex_max_level_;
  size_t vertex_lock_count_;
  FileId_t min_level_0_fid_;
  std::string& dataDir;
  uint64_t& currentTime;
  SequenceNumber_t MAX_GLOBAL_SEQ = std::numeric_limits<SequenceNumber_t>::max();
  std::vector<Way> ways;
  std::vector<EdgeRecord> edge_cache;
  int cache_max_size = 0;
  const std::vector<uint32_t>& sub_property_lengths_;
  BufferManager buffer_manager;
  uint32_t max_subcompactions_;
  std::vector<std::mutex*> tablecache_mutex_;
  std::set<FileId_t> merged_fids_[2];
  grape::BlockingQueue<FileId_t> delete_fids_set_;
  std::vector<SSTableCache*> delete_filemeta_set_;
  // Each compaction reads inputs from "level_" and "level_+1"
  std::vector<SSTableCache*> inputs_[2];  // The two sets of inputs
  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  SequenceNumber_t compact_pointer_[MAX_LEVEL];
  VertexId_t last_merge_max_key_ = 0;
  std::atomic<bool> state;
  VersionSet *l0_versionset_;
  std::atomic<SequenceNumber_t>& global_version_id_;

  VersionEdit version_edit_;
  SSTDataManager& sstdata_manager_;
  DelRecordManage& del_record_manager_;
  SuperVersion& sv_;

  std::atomic<uint32_t> large_job_;
  std::atomic<uint32_t> max_job_num_;
  uint32_t l0_auto_compaction_limit_ = 4;
  uint32_t l1_auto_compaction_limit_ = 0;

  std::map<FileId_t, bool>& sst_is_vaild_to_ins_lf_;
  std::mutex* lf_mutex_;

  PreparationCallback preparation_callback_;

  std::map<std::pair<FileId_t, int>, std::vector<LazyFile*>>lf_need_del;

#ifdef WRITE_STALL_TEST
  std::vector<std::pair<uint32_t, double>> timings;
#endif
};

} // lsmgraph namespace
