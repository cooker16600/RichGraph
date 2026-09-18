#pragma once

#include <cstdint>
#include <memory>
#include <variant>
#include <atomic>
#include <string>
#include "core/types.h"
#include "core/graph/edge.h"
#include "core/SSTable.h"
#include "core/MemTable.h"
#include "core/graph/edge.h"
#include "core/cache/BlockManager.h"
#include "core/cache/SSTDataCache.h"
#include "core/flags.h"
#include "core/utils.h"
#include "core/superversion.h"
#include "core/fixed_property_layout.h"
#include "util/rocksdb/heap.h"
#include "util/livegraph/futex.hpp"
#include <map>
#include <queue>
#include <algorithm>
#include <functional>
#include "core/edge_iterator_base.h"
#include "core/del_record_manage.h"
#include "core/SSTEdgeIterator.h"
#include "core/property_delta_overlay_iterator.h"

namespace lsmgraph {

    using PropertyDeltaViewResolver = std::function<
        std::shared_ptr<const PropertyDeltaReadView>(FileId_t, int)>;

    class EdgeIteratorNoDelete;

    class EdgeIteratorTraverse;

    class EdgeIteratorTraverseOpt; // 利用数组/bitmap/set来避免多路归并 

    class EdgeIteratorDynamic;

    using EdgeIterator = EdgeIteratorDynamic;
#ifndef NO_VIRTUAL

    class EdgeIteratorNoDelete {
    public:
      EdgeIteratorNoDelete(const VertexId_t src,
                           MemTable *newmemTable,
                           std::vector<std::vector<SSTableCache *> *> &fileMetaCache,
                           SSTDataManager &sstdata_manager,
                           LevelIndex *vid_to_levelIndex,
                           SuperVersion *sv,
                           DelRecordManage *del_record_manager,
                           int max_level,
                           SequenceNumber_t seq = MAX_SEQ_ID,
                           int property_id = -1)
              : seq_(seq), sv_(sv), del_record_manager_(del_record_manager) {
        if (max_level < 0) {
          return;
        }


#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_level_0_t1 = std::chrono::steady_clock::now();
#endif
        // sstable
        // level-0
        if (max_level >= 1) {
          // 需要满足 fileID >= min_level_0_fid
          FileId_t min_level_0_fid = sv_->findex.get_min_level_0_fid();
          for (auto sst_it: *(sv_->get_version()->GetLevel0Files())) {
            if (seq_ <= sst_it->seq_) {
              continue;
            }
            if (sst_it->header.timeStamp >= min_level_0_fid
                && src <= sst_it->header.maxKey
                && src >= sst_it->header.minKey) {
              find_iterator_sst(sst_it, src, sstdata_manager, property_id);
            }
          }
        }
#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_level_0_t2 = std::chrono::steady_clock::now();
        std::chrono::duration<double> iterate_level_0_span = std::chrono::duration_cast<std::chrono::duration<double>>(
            iterate_level_0_t2 - iterate_level_0_t1);
        write_add(&iterate_level_0, iterate_level_0_span.count());
#endif

#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_level_1_t1 = std::chrono::steady_clock::now();
#endif
        // level>0, from levelindex
        if (max_level >= 2) {
          find_iterator_sst_by_levelindex(src, sstdata_manager,
                                          vid_to_levelIndex, max_level, property_id);
        }
#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_level_1_t2 = std::chrono::steady_clock::now();
        std::chrono::duration<double> iterate_level_1_span = std::chrono::duration_cast<std::chrono::duration<double>>(
            iterate_level_1_t2 - iterate_level_1_t1);
        write_add(&iterate_level_1, iterate_level_1_span.count());
#endif

#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_memtable_t1 = std::chrono::steady_clock::now();
#endif
        for (auto tb: sv_->get_memtable()) {
          if (seq_ <= tb->GetStartTime()) {
            continue;
          }
          it = std::shared_ptr<EdgeIteratorBase>(
                  new NeighBors::MemEdgeIterator(tb->get_vertex_adj(src),
                                                 tb->GetFid()));
          if (it->valid()) {
            it_array.insert(it_array.begin(), it);
          }
        }
#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_memtable_t2 = std::chrono::steady_clock::now();
        std::chrono::duration<double> iterate_memtable_span = std::chrono::duration_cast<std::chrono::duration<double>>(
            iterate_memtable_t2 - iterate_memtable_t1);
        write_add(&iterate_memtable, iterate_memtable_span.count());
#endif

#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_find_first_t1 = std::chrono::steady_clock::now();
#endif
        if (!it_array.empty()) {
          findFirstValid();
        } else {
          it = nullptr;
        }
#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_find_first_t2 = std::chrono::steady_clock::now();
        std::chrono::duration<double> iterate_find_first_span = std::chrono::duration_cast<std::chrono::duration<double>>(
            iterate_find_first_t2 - iterate_find_first_t1);
        write_add(&iterate_find_first, iterate_find_first_span.count());
#endif
      }

      void find_iterator_sst_by_levelindex(const VertexId_t src,
                                           SSTDataManager &sstdata_manager,
                                           LevelIndex *vid_to_levelIndex,
                                           int max_level, int property_id) {
        uint32_t fileID = 0;
        uint32_t offset = 0;
        uint32_t next_offset = 0;

        MulLevelIndex &findex = sv_->findex;

#ifndef MMAP_DIFF_SIZE_LEVEL_INDEX 
        for (int levelID = 0; levelID < max_level - 1; levelID++) {
#else   // 默认的和hot/cold都需要检测每一个level
        for (int levelID = 0; levelID < findex.get_level_num(); levelID++) {
#endif
          if (FLAGS_support_mulversion == false) {
            int index_id = src * LEVEL_INDEX_SIZE + levelID;
            LevelIndex &findex = vid_to_levelIndex[index_id];
            fileID = findex.get_fileID();

            if (fileID == INVALID_File_ID) {
              continue;
            }

            offset = findex.get_offset();
            next_offset = findex.get_next_offset();
            assert(next_offset >= offset);
          } else {
            fileID = findex.get_fileID(levelID);

            if (fileID == INVALID_File_ID) {
              continue;
            }

            offset = findex.get_offset(levelID);
            next_offset = findex.get_next_offset(levelID);
            assert(next_offset >= offset);
          }

          uint32_t adj_size = (next_offset - offset) / EDGEBODY_SIZE;
          if (FLAGS_OPEN_SSTDATA_CACHE == true) {
            SSTDataCache *sstcache = sstdata_manager.get_data(fileID);
            assert(sstcache != nullptr);
            std::shared_ptr<EdgeIteratorBase> it_temp = std::shared_ptr<EdgeIteratorBase>(
                    new SSTEdgeIterator(
                            (EdgeBody_t*)(sstcache->GetEdgeData() + offset),
                            sstcache->GetPropertyData(property_id),
                            adj_size,
                            fileID,
                            0,
                            GetSubPropertyOffsetByEdgeOrdinal(
                                GetEdgeOrdinalFromBodyOffset(offset), property_id),
                            GetSubPropertyFixedLength(property_id)));
            if (it_temp->valid()) {
#ifdef USE_MADVISE
            utils::use_madvise(sstcache->GetEdgeData(), offset, adj_size);
#endif
              it_array.emplace_back(it_temp);
            }
          } else {
            std::cout << " ToDo get_edges..." << std::endl;
          }
        }

      }

      Status find_iterator_sst(SSTableCache *sst_it, VertexId_t src,
                               SSTDataManager &sstdata_manager, int property_id) {

        int pos = sst_it->get(src);

        if (pos < 0) {
          return Status::kNotFound;
        }

        uint32_t offset = (sst_it->indexes)[pos].offset;
        uint32_t next_offset = (sst_it->indexes)[pos + 1].offset;

        if (FLAGS_OPEN_SSTDATA_CACHE == true) {
          SSTDataCache *sstcache = sstdata_manager.get_data(
                  sst_it->header.timeStamp);
          assert(sstcache != nullptr);
          uint32_t adj_size = (next_offset - offset) / EDGEBODY_SIZE;
          assert(next_offset >= offset);

          std::shared_ptr<EdgeIteratorBase> it_temp = std::shared_ptr<EdgeIteratorBase>(
                  new SSTEdgeIterator(
                          (EdgeBody_t*)(sstcache->GetEdgeData() + offset),
                          sstcache->GetPropertyData(property_id),
                          adj_size,
                          sst_it->header.timeStamp,
                          0,
                          GetSubPropertyOffsetByEdgeOrdinal(
                              GetEdgeOrdinalFromBodyOffset(offset), property_id),
                          GetSubPropertyFixedLength(property_id)));
          if (it_temp->valid()) {
#ifdef USE_MADVISE
            utils::use_madvise(sstcache->GetEdgeData(), offset, adj_size);
#endif
            it_array.emplace_back(it_temp);
          }
        } else {
          std::cout << " ToDo get_edges..." << std::endl;
        }
        return Status::kOk;
      }

      void init() {

      }

      bool valid() {
        return !(it == nullptr) && it->valid();
      }

      // 检查entry是否合法
      bool check_entry_valid() {
        assert(it != nullptr && it->valid());
#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_check_entry_valid_t1 = std::chrono::steady_clock::now();
#endif
      int64_t temp_seq = it->sequence();
        if (temp_seq > seq_) {  // 读不到的版本，太新了
          return false;
        }
#ifdef DEL_EDGE_SEPARATE
        if (!curr_have_map_) {
          if (it->marker() == true && it->sequence() < seq_) {
            it->next();
            if (!it->IsMemTable()) { // 如果不是memtable
              assert(it != nullptr); // 下一条是被删除的记录 注意对于memtable的不是
              it->next();
            }
            return it->valid() && check_entry_valid();
          }
        } else {
          SequenceNumber_t eid = it->sequence();
          SequenceNumber_t del_time;
          if (del_record_manager_->get_time(
                  curr_deleted_edge_map_, eid, del_time)
              && del_time < seq_) {
            it->next();
            if (!it->IsMemTable()) { // 如果不是memtable
              assert(it != nullptr); // 下一条是被删除的记录 注意对于memtable的不是
              it->next();
            }
            return it->valid() && check_entry_valid();
          } else {
            return true;
          }
        }
#endif
        return true;
      }

      // 找到第一个合法指针
      void findFirstValid() {
        if (it_array.size() == 0) {
          it = nullptr;
          return;
        } else {
          it = nullptr;
          do {
            it = it_array.back();
            it_array.pop_back();
#ifdef DEL_EDGE_SEPARATE
            curr_have_map_ = del_record_manager_->find_eidmap(it->get_fid(),
                                                              curr_deleted_edge_map_);
#endif
            // 找到第一个合法的指针，并且值合法
            while (valid() && !check_entry_valid()) {
              if (!valid()) {
                break;
              }
              it->next();
            }
          } while ((!valid() && !it_array.empty()));
          return;
        }
      }

      void next() {
        while (true) {
          if (!valid()) {
            return;
          }
          it->next();

          if (!valid()) {
            findFirstValid();
          }

          if (!valid() || check_entry_valid()) {
            break;
          }
        }
      }

      VertexId_t dst_id() {
        return it->dst_id();
      }

      SequenceNumber_t sequence() {
        return it->sequence();
      }

      Marker_t marker() {
        return it->marker();
      }

      EdgeProperty_t edge_data(int id) {
        return it->edge_data(id);
      }

      bool empty() {
        return it->empty();
      }

      size_t size() {
        return it->size();
      }

      bool compare(const std::shared_ptr<EdgeIteratorBase> r1,
                   const std::shared_ptr<EdgeIteratorBase> r2) {
        if (r1->dst_id() == r2->dst_id()) {
          return r1->sequence() > r2->sequence();
        }
        return r1->dst_id() < r2->dst_id();
      }

      // clear all points
      ~EdgeIteratorNoDelete() {
        it_array.clear();
      }

    private:
      std::shared_ptr<EdgeIteratorBase> it = nullptr;
      std::vector<std::shared_ptr<EdgeIteratorBase>> it_array;
      SuperVersion *sv_;
      SequenceNumber_t seq_;
      EidToTimeMap *curr_deleted_edge_map_;
      DelRecordManage *del_record_manager_;
      bool curr_have_map_;
    };

    // Merges sources from newest to oldest and returns only the newest record
    // for each topology key. The source order is property buffer, memtable,
    // then each delta/SST pair.
    class EdgeIteratorDynamic {
      public:
      // Fast path for one prelocated CSR range. The snapshot guard keeps its
      // backing file alive for the iterator lifetime.
      EdgeIteratorDynamic(std::shared_ptr<EdgeIteratorBase> single_it,
                          SequenceNumber_t seq,
                          bool is_out,
                          uint8_t edge_type,
                          std::shared_ptr<const void> snapshot_guard)
          : it(std::move(single_it)),
            sv_(nullptr),
            seq_(seq),
            is_out_(is_out),
            edge_type_(edge_type),
            snapshot_guard_(std::move(snapshot_guard)) {
        while (valid() && !check_entry_valid()) {
          it->next();
        }
      }

      // Lightweight CSR merge over iterators prepared by the caller, usually
      // source-local memory iterators plus one CSR disk iterator.
      EdgeIteratorDynamic(std::vector<std::shared_ptr<EdgeIteratorBase>> prepared_iters,
                          SuperVersion* sv,
                          SequenceNumber_t seq,
                          bool is_out,
                          uint8_t edge_type,
                          std::shared_ptr<const void> snapshot_guard)
          : it(nullptr),
            it_array(std::move(prepared_iters)),
            sv_(sv),
            seq_(seq),
            is_out_(is_out),
            edge_type_(edge_type),
            snapshot_guard_(std::move(snapshot_guard)) {
        hold_superversion_snapshot();
        if (!it_array.empty()) {
          findFirstValid();
        }
      }

      EdgeIteratorDynamic(const VertexId_t src,
        SSTDataManager &sstdata_manager,
        LevelIndex *vid_to_levelIndex,
        SuperVersion *sv,
        int max_level,
        SequenceNumber_t seq = MAX_SEQ_ID,
        int property_id = -1,
        bool is_out = true,
        uint8_t edge_type = 0,
        PropertyDeltaViewResolver delta_view_resolver = {})
          : seq_(seq), sv_(sv), is_out_(is_out),
            edge_type_(edge_type),
            delta_view_resolver_(std::move(delta_view_resolver)) {
        if (max_level < 0) {
          return;
        }
        hold_superversion_snapshot();
        if (sv_ == nullptr || sv_snapshot_ == nullptr) {
          return;
        }
        

        // memproperty
        if (FLAGS_enable_memproperty) {
          for (auto mp: sv_->get_memproperty()) {
            if (mp == nullptr || seq_ <= mp->GetStartTime()) {
              continue;
            }
            it = std::shared_ptr<EdgeIteratorBase>(new NeighBors::MemEdgeIterator(mp->get_vertex_adj(src), mp->GetFid(), mp->newest_edge));
            
            if (it->valid()) {
              it_array.emplace_back(it);
            }
          }
        }

        // memtable
        for (auto tb: sv_->get_memtable()) {
          if (seq_ <= tb->GetStartTime()) {
            continue;
          }
          it = std::shared_ptr<EdgeIteratorBase>(new NeighBors::MemEdgeIterator(tb->get_vertex_adj(src), tb->GetFid(), tb->newest_edge));
          if (it->valid()) {
            const auto delta_view = ResolveDeltaView(tb->GetFid(), property_id);
            if (delta_view != nullptr) {
              it = std::make_shared<PropertyDeltaOverlayIterator>(
                  src, it, delta_view);
            }
            it_array.emplace_back(it);
          }
        }

        // level-0
        if (max_level >= 1) {
          // 需要满足 fileID >= min_level_0_fid
          FileId_t min_level_0_fid = sv_->findex.get_min_level_0_fid();
          for (auto sst_it: *(sv_->get_version()->GetLevel0Files())) {
            if (seq_ <= sst_it->seq_) {
              continue;
            }
            if (sst_it->header.timeStamp >= min_level_0_fid
                && src <= sst_it->header.maxKey
                && src >= sst_it->header.minKey) {
              find_iterator_sst(sst_it, src, sstdata_manager, property_id);
            }
          }
        }

        // level>0, from levelindex
        if (max_level >= 2) {
          find_iterator_sst_by_levelindex(src, sstdata_manager,
          vid_to_levelIndex, max_level, property_id);
        }

        if (!it_array.empty()) {
          findFirstValid();
        } else {
          it = nullptr;
        }
      }

      void find_LF(FileId_t fid, VertexId_t src, SSTDataManager &sstdata_manager){
        LazyFile* lf = sstdata_manager.get_LazyFile(fid);

        int pos = lf->get(src);

        if (pos < 0) {
          return;
        }
        int begin = lf->indexs[pos].offset;
        int end = lf->indexs[pos+1].offset;

        std::shared_ptr<EdgeIteratorBase> it_temp = std::shared_ptr<EdgeIteratorBase>(new LFEdgeIterator(lf->file_ptr + begin, lf->file_ptr + lf->Get_Reseted_Property_Offset(), end - begin, fid, lf->newest_edge));
        if(it_temp->valid()){
          it_array.emplace_back(it_temp);
        }

      }

      Status find_iterator_sst(SSTableCache *sst_it, VertexId_t src,
        SSTDataManager &sstdata_manager, int property_id) {


        int pos = sst_it->get(src);

        if (pos < 0) {
          return Status::kNotFound;
        }
        // Apply newer lazy-file records before the SST base data.

        auto& store_cp = sv_->get_lazyfile_store();

        auto iter = store_cp.find(std::make_pair(sst_it->header.timeStamp, property_id));

        if(iter != store_cp.end()){
          for(auto lf: iter->second){
            find_LF(lf->fid_, src, sstdata_manager);
          }
        }


        uint32_t offset = (sst_it->indexes)[pos].offset;
        uint32_t next_offset = (sst_it->indexes)[pos + 1].offset;
        if (FLAGS_OPEN_SSTDATA_CACHE == true) {
          SSTDataCache *sstcache = sstdata_manager.get_data(
          sst_it->header.timeStamp);
          assert(sstcache != nullptr);
          const auto delta_view = ResolveDeltaView(
              sst_it->header.timeStamp, property_id);
          const std::byte* property_data = delta_view == nullptr
              ? reinterpret_cast<const std::byte*>(
                    sstcache->GetPropertyData(property_id))
              : delta_view->BaseDataOr(
                    sstcache->GetPropertyData(property_id));
          uint32_t adj_size = (next_offset - offset) / EDGEBODY_SIZE;
          assert(next_offset >= offset);

          std::shared_ptr<EdgeIteratorBase> it_temp = std::shared_ptr<EdgeIteratorBase>(
          new SSTEdgeIterator(
            (EdgeBody_t*)(sstcache->GetEdgeData() + offset),
            const_cast<char*>(reinterpret_cast<const char*>(property_data)),
            adj_size,
            sst_it->header.timeStamp,
            sst_it->newest_edge,
            GetSubPropertyOffsetByEdgeOrdinal(
                GetEdgeOrdinalFromBodyOffset(offset), property_id),
            GetSubPropertyFixedLength(property_id)));
          if (delta_view != nullptr) {
            it_temp = std::make_shared<PropertyDeltaOverlayIterator>(
                src, it_temp, delta_view);
          }
          if (it_temp->valid()) {
          #ifdef USE_MADVISE
          utils::use_madvise(sstcache->GetEdgeData(), offset, adj_size);
          #endif
          it_array.emplace_back(it_temp);
          }
        } else {
          std::cout << " ToDo get_edges..." << std::endl;
        }
        return Status::kOk;
      } 

      void find_iterator_sst_by_levelindex(const VertexId_t src,
        SSTDataManager &sstdata_manager,
        LevelIndex *vid_to_levelIndex,
        int max_level, int property_id) {


          uint32_t fileID = 0;
          uint32_t offset = 0;
          uint32_t next_offset = 0;

          MulLevelIndex &findex = sv_->findex;

          for (int levelID = 0; levelID < max_level - 1; levelID++) {

            fileID = findex.get_fileID(levelID);

            if (fileID == INVALID_File_ID) {
            continue;
            }

            
            // Apply newer lazy-file records before the SST base data.

            auto& store_cp = sv_->get_lazyfile_store();
            
            auto iter = store_cp.find(std::make_pair(fileID, property_id));

            if(iter != store_cp.end()){
              for(auto lf: iter->second){
                find_LF(lf->fid_, src, sstdata_manager);
              }
            }


            offset = findex.get_offset(levelID);
            next_offset = findex.get_next_offset(levelID);
            assert(next_offset >= offset);
            

            uint32_t adj_size = (next_offset - offset) / EDGEBODY_SIZE;
            if (FLAGS_OPEN_SSTDATA_CACHE == true) {
              SSTDataCache *sstcache = sstdata_manager.get_data(fileID);
              assert(sstcache != nullptr);
              const auto delta_view = ResolveDeltaView(fileID, property_id);
              const std::byte* property_data = delta_view == nullptr
                  ? reinterpret_cast<const std::byte*>(
                        sstcache->GetPropertyData(property_id))
                  : delta_view->BaseDataOr(
                        sstcache->GetPropertyData(property_id));
              std::shared_ptr<EdgeIteratorBase> it_temp = std::shared_ptr<EdgeIteratorBase>(
              new SSTEdgeIterator(
              (EdgeBody_t*)(sstcache->GetEdgeData() + offset),
              const_cast<char*>(reinterpret_cast<const char*>(property_data)),
              adj_size,
              fileID,
              sstcache->newest_edge,
              GetSubPropertyOffsetByEdgeOrdinal(
                  GetEdgeOrdinalFromBodyOffset(offset), property_id),
              GetSubPropertyFixedLength(property_id)));
              if (delta_view != nullptr) {
                it_temp = std::make_shared<PropertyDeltaOverlayIterator>(
                    src, it_temp, delta_view);
              }
              if (it_temp->valid()) {
                #ifdef USE_MADVISE
                utils::use_madvise(sstcache->GetEdgeData(), offset, adj_size);
                #endif
                it_array.emplace_back(it_temp);
              }
            } else {
              std::cout << " ToDo get_edges..." << std::endl;
            }
          }
        }

        ~EdgeIteratorDynamic() {
          auto held_snapshot = sv_snapshot_;
          it.reset();
          it_array.clear();
          // Release the thread-local SuperVersion promptly so a scan cannot
          // pin retired memtables and stall writers. Multiple database shards
          // share this TLS object, so clear it only when it still references
          // the snapshot held by this iterator.
          if (sv_ != nullptr
              && sv_->version_memtable_memproperty_lazyfile == held_snapshot) {
            sv_->version_memtable_memproperty_lazyfile = nullptr;
          }
          sv_snapshot_.reset();
        }

        void init() {

        }
  
        bool valid() {
          return !(it == nullptr) && it->valid();
        }

        // 检查entry是否合法
        bool check_entry_valid() {
          assert(it != nullptr && it->valid());

          if (it->is_out() != is_out_) {
            return false;
          }
          if (it->edge_type() != edge_type_) {
            return false;
          }

        if(seq_ > it->newest_edge){
            return true;
          }
          int64_t temp_seq = it->sequence();
          if (temp_seq > seq_) {  // 读不到的版本，太新了
            return false;
          }
          return true;
        }

        // 找到第一个合法指针
        void findFirstValid() {
          if (it_array.size() == 0) {
            it = nullptr;
            return;
          } else {
            it = nullptr;
            do {
              it = it_array.back();
              it_array.pop_back();
  #ifdef DEL_EDGE_SEPARATE
              curr_have_map_ = del_record_manager_->find_eidmap(it->get_fid(),
                                                                curr_deleted_edge_map_);
  #endif
              // 找到第一个合法的指针，并且值合法
              while (valid() && !check_entry_valid()) {
                it->next();
              }
            } while ((!valid() && !it_array.empty()));
            return;
          }
        }

      void next() {
        while (true) {
          if (!valid()) {
            return;
          }
          it->next();

          if (!valid()) {
            findFirstValid();
          }

          bool valid_flag = valid();
          if(!valid_flag){
            break;
          }
          bool check_entry_valid_flag = check_entry_valid();
          if(valid_flag && check_entry_valid_flag){
            break;
          }
        }
      }

      VertexId_t dst_id() {
        return it->dst_id();
      }

      SequenceNumber_t sequence() {
        return it->sequence();
      }

      Marker_t marker() {
        return it->marker();
      }

      EdgeProperty_t edge_data(int id) {
        return it->edge_data(id);
      }

      bool empty() {
        return it->empty();
      }

      size_t size() {
        return it->size();
      }

      bool compare(const std::shared_ptr<EdgeIteratorBase> r1,
                   const std::shared_ptr<EdgeIteratorBase> r2) {
        if (r1->dst_id() == r2->dst_id()) {
          return r1->sequence() > r2->sequence();
        }
        return r1->dst_id() < r2->dst_id();
      }



      private:
      std::shared_ptr<const PropertyDeltaReadView> ResolveDeltaView(
          FileId_t fid, int property_id) const {
        if (!delta_view_resolver_ || property_id < 0) return nullptr;
        return delta_view_resolver_(fid, property_id);
      }

      void hold_superversion_snapshot() {
        if (sv_ != nullptr) {
          sv_snapshot_ = sv_->version_memtable_memproperty_lazyfile;
        }
      }

      std::shared_ptr<EdgeIteratorBase> it = nullptr;
      std::vector<std::shared_ptr<EdgeIteratorBase>> it_array;
      SuperVersion *sv_;
      std::shared_ptr<VersionAndMemTableAndMemPropertyAndLf> sv_snapshot_;
      SequenceNumber_t seq_;
      bool is_out_ = true;
      uint8_t edge_type_ = 0;
      std::shared_ptr<const void> snapshot_guard_;
      PropertyDeltaViewResolver delta_view_resolver_;
  };
#else

    class EdgeIteratorWrapper {
    public:
      enum IteratorType {
        MEM_ITR,
        SST_ITR,
      };

      explicit EdgeIteratorWrapper() : type_(SST_ITR), sst_it_(nullptr) {}

      explicit EdgeIteratorWrapper(const NeighBors *edges, FileId_t fid = INVALID_File_ID) : type_(MEM_ITR), mem_it_
              (std::make_shared<NeighBors::MemEdgeIterator>(edges, fid)) {}

      EdgeIteratorWrapper(EdgeBody_t *body_data,
                          char *property_data,
                          size_t body_num,
                          FileId_t fid = INVALID_File_ID) :
              type_(SST_ITR), sst_it_(std::make_shared<SSTEdgeIterator>(body_data, property_data,
                                                                        body_num,
                                                                        fid)) {}

      EdgeIteratorWrapper(EdgeIteratorWrapper &&rhs) noexcept: type_(rhs.type_), sst_it_(std::move(rhs.sst_it_)) {}

      ~EdgeIteratorWrapper() {}


      inline bool valid() const {
        switch (type_) {
          case MEM_ITR:
            return mem_it_->valid();
          case SST_ITR:
            return sst_it_->valid();
        }
      }

      inline void next() {
        switch (type_) {
          case MEM_ITR:
            mem_it_->next();
            break;
          case SST_ITR:
            sst_it_->next();
            break;
        }
      }

      inline VertexId_t dst_id() const {
        switch (type_) {
          case MEM_ITR:
            return mem_it_->dst_id();
          case SST_ITR:
            return sst_it_->dst_id();
        }
      }

      inline SequenceNumber_t sequence() const {
        switch (type_) {
          case MEM_ITR:
            return mem_it_->sequence();
          case SST_ITR:
            return sst_it_->sequence();
        }
      }

      inline Marker_t marker() const {
        switch (type_) {
          case MEM_ITR:
            return mem_it_->marker();
          case SST_ITR:
            return sst_it_->marker();
        }
      }

      inline bool empty() const {
        switch (type_) {
          case MEM_ITR:
            return mem_it_->empty();
          case SST_ITR:
            return sst_it_->empty();
        }
      }

      inline size_t size() const {
        switch (type_) {
          case MEM_ITR:
            return mem_it_->size();
          case SST_ITR:
            return sst_it_->size();
        }
      }

      inline EdgeProperty_t edge_data() {
        switch (type_) {
          case MEM_ITR:
            return mem_it_->edge_data();
          case SST_ITR:
            return sst_it_->edge_data();
        }
      }

      inline FileId_t get_fid() {
        switch (type_) {
          case MEM_ITR:
            return mem_it_->get_fid();
          case SST_ITR:
            return sst_it_->get_fid();
        }
      }

      inline bool IsMemTable() {
        switch (type_) {
          case MEM_ITR:
            return true;
          case SST_ITR:
            return false;
        }
      }

    private:
      IteratorType type_;
      union {
        std::shared_ptr<SSTEdgeIterator> sst_it_;
        std::shared_ptr<NeighBors::MemEdgeIterator> mem_it_;
      };
    };

    class EdgeIteratorNoDelete {
    public:
      EdgeIteratorNoDelete(const VertexId_t src,
                           MemTable *newmemTable,
                           std::vector<std::vector<SSTableCache *> *> &fileMetaCache,
                           SSTDataManager &sstdata_manager,
                           LevelIndex *vid_to_levelIndex,
                           SuperVersion *sv,
                           DelRecordManage *del_record_manager,
                           int max_level,
                           SequenceNumber_t seq = MAX_SEQ_ID)
              : it(nullptr), flag(false), seq_(seq), sv_(sv), del_record_manager_(del_record_manager) {
        if (max_level < 0) {
          return;
        }

#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_memtable_t1 = std::chrono::steady_clock::now();
#endif
        for (auto tb: sv_->get_memtable()) {
          if (seq_ <= tb->GetStartTime()) {
            continue;
          }
          EdgeIteratorWrapper temp_itr(tb->get_vertex_adj(src),
                                       tb->GetFid());
          if (temp_itr.valid()) {
            it_array.emplace_back(std::move(temp_itr));
          }
        }
#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_memtable_t2 = std::chrono::steady_clock::now();
    std::chrono::duration<double> iterate_memtable_span = std::chrono::duration_cast<std::chrono::duration<double>>(
        iterate_memtable_t2 - iterate_memtable_t1);
    write_add(&iterate_memtable, iterate_memtable_span.count());
#endif

#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_level_0_t1 = std::chrono::steady_clock::now();
#endif
        // sstable
        // level-0
        if (max_level >= 1) {
          // 需要满足 fileID >= min_level_0_fid
          FileId_t min_level_0_fid = sv_->findex.get_min_level_0_fid();
          for (auto sst_it: *(sv_->get_version()->GetLevel0Files())) {
            if (sst_it->header.timeStamp >= min_level_0_fid
                && src <= sst_it->header.maxKey
                && src >= sst_it->header.minKey) {
              find_iterator_sst(sst_it, src, sstdata_manager);
            }
          }
        }
#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_level_0_t2 = std::chrono::steady_clock::now();
    std::chrono::duration<double> iterate_level_0_span = std::chrono::duration_cast<std::chrono::duration<double>>(
        iterate_level_0_t2 - iterate_level_0_t1);
    write_add(&iterate_level_0, iterate_level_0_span.count());
#endif

#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_level_1_t1 = std::chrono::steady_clock::now();
#endif
        // level>0, from levelindex
        if (max_level >= 2) {
          find_iterator_sst_by_levelindex(src, sstdata_manager,
                                          vid_to_levelIndex, max_level);
        }
#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_level_1_t2 = std::chrono::steady_clock::now();
    std::chrono::duration<double> iterate_level_1_span = std::chrono::duration_cast<std::chrono::duration<double>>(
        iterate_level_1_t2 - iterate_level_1_t1);
    write_add(&iterate_level_1, iterate_level_1_span.count());
#endif

#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_find_first_t1 = std::chrono::steady_clock::now();
#endif
        if (!it_array.empty()) {
          flag = true;
          findFirstValid();
        } else {
          it = nullptr;
        }
#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_find_first_t2 = std::chrono::steady_clock::now();
    std::chrono::duration<double> iterate_find_first_span = std::chrono::duration_cast<std::chrono::duration<double>>(
        iterate_find_first_t2 - iterate_find_first_t1);
    write_add(&iterate_find_first, iterate_find_first_span.count());
#endif
      }

      void find_iterator_sst_by_levelindex(const VertexId_t src,
                                           SSTDataManager &sstdata_manager,
                                           LevelIndex *vid_to_levelIndex,
                                           int max_level) {
        uint32_t fileID = 0;
        uint32_t offset = 0;
        uint32_t next_offset = 0;

        MulLevelIndex &findex = sv_->findex;

        for (int levelID = 0; levelID < max_level - 1; levelID++) {
          if (FLAGS_support_mulversion == false) {
            int index_id = src * LEVEL_INDEX_SIZE + levelID;
            LevelIndex &findex = vid_to_levelIndex[index_id];
            fileID = findex.get_fileID();

            if (fileID == INVALID_File_ID) {
              continue;
            }

            offset = findex.get_offset();
            next_offset = findex.get_next_offset();
            assert(next_offset >= offset);
          } else {
            fileID = findex.get_fileID(levelID);

            if (fileID == INVALID_File_ID) {
              continue;
            }

            offset = findex.get_offset(levelID);
            next_offset = findex.get_next_offset(levelID);
            assert(next_offset >= offset);
          }

          uint32_t adj_size = (next_offset - offset) / EDGEBODY_SIZE;
          if (FLAGS_OPEN_SSTDATA_CACHE == true) {
            SSTDataCache *sstcache = sstdata_manager.get_data(fileID);
            assert(sstcache != nullptr);
            EdgeIteratorWrapper temp_itr((EdgeBody_t *) (sstcache->GetEdgeData() + offset),
                                         sstcache->GetPropertyData(),
                                         adj_size,
                                         fileID);
            if (temp_itr.valid()) {
              it_array.emplace_back(std::move(temp_itr));
            }
          } else {
            std::cout << " ToDo get_edges..." << std::endl;
          }
        }

      }

      Status find_iterator_sst(SSTableCache *sst_it, VertexId_t src,
                               SSTDataManager &sstdata_manager) {

        int pos = sst_it->get(src);

        if (pos < 0) {
          return Status::kNotFound;
        }

        uint32_t offset = (sst_it->indexes)[pos].offset;
        uint32_t next_offset = (sst_it->indexes)[pos + 1].offset;

        if (FLAGS_OPEN_SSTDATA_CACHE == true) {
          SSTDataCache *sstcache = sstdata_manager.get_data(
                  sst_it->header.timeStamp);
          assert(sstcache != nullptr);
          uint32_t adj_size = (next_offset - offset) / EDGEBODY_SIZE;
          assert(next_offset >= offset);

          EdgeIteratorWrapper temp_itr((EdgeBody_t *) (sstcache->GetEdgeData() + offset),
                                       sstcache->GetPropertyData(),
                                       adj_size,
                                       sst_it->header.timeStamp);
          if (temp_itr.valid()) {
            it_array.emplace_back(std::move(temp_itr));
          }
        } else {
          std::cout << " ToDo get_edges..." << std::endl;
        }
        return Status::kOk;
      }

      void init() {

      }

      bool valid() {
        return it != nullptr && it->valid();
      }

      // 检查entry是否合法
      bool check_entry_valid() {
        assert(it != nullptr && it->valid());
        if (it->sequence() > seq_) {  // 读不到的版本，太新了
          return false;
        }
#ifdef DEL_EDGE_SEPARATE
        if (!curr_have_map_) {
          if (it->marker() && it->sequence() < seq_) {
            it->next();
            if (!it->IsMemTable()) { // 如果不是memtable
              assert(it != nullptr); // 下一条是被删除的记录 注意对于memtable的不是
              it->next();
            }
            return it->valid() && check_entry_valid();
          }
        } else {
          SequenceNumber_t eid = it->sequence();
          SequenceNumber_t del_time;
          if (del_record_manager_->get_time(
                  curr_deleted_edge_map_, eid, del_time)
              && del_time < seq_) {
            it->next();
            if (!it->IsMemTable()) { // 如果不是memtable
              assert(it != nullptr); // 下一条是被删除的记录 注意对于memtable的不是
              it->next();
            }
            return it->valid() && check_entry_valid();
          } else {
            return true;
          }
        }
#endif
        return true;
      }

      // 找到第一个合法指针
      void findFirstValid() {
        if (it_array.size() == 0) {
          it = nullptr;
          return;
        } else {
          it = nullptr;
          do {
            it = &it_array.back();
            it_array.pop_back();
#ifdef DEL_EDGE_SEPARATE
            curr_have_map_ = del_record_manager_->find_eidmap(it->get_fid(),
                                                              curr_deleted_edge_map_);
#endif
            // 找到第一个合法的指针，并且值合法
            while (valid() && !check_entry_valid()) {
              if (!valid()) {
                break;
              }
              it->next();
            }
          } while ((!valid() && !it_array.empty()));
          return;
        }
      }

      void next() {
        while (true) {
          if (!valid()) {
            return;
          }
          it->next();

          if (!valid()) {
            findFirstValid();
          }

          if (!valid() || check_entry_valid()) {
            break;
          }
        }
      }

      VertexId_t dst_id() {
        return it->dst_id();
      }

      SequenceNumber_t sequence() {
        return it->sequence();
      }

      Marker_t marker() {
        return it->marker();
      }

      EdgeProperty_t edge_data() {
        return it->edge_data();
      }

      bool empty() {
        return it->empty();
      }

      size_t size() {
        return it->size();
      }

      bool compare(const std::shared_ptr<EdgeIteratorBase> r1,
                   const std::shared_ptr<EdgeIteratorBase> r2) {
        if (r1->dst_id() == r2->dst_id()) {
          return r1->sequence() > r2->sequence();
        }
        return r1->dst_id() < r2->dst_id();
      }

      // clear all points
      ~EdgeIteratorNoDelete() {
        it_array.clear();
      }

    private:
      bool flag;
      EdgeIteratorWrapper *it;
      std::vector<EdgeIteratorWrapper> it_array;
      SuperVersion *sv_;
      SequenceNumber_t seq_;
      EidToTimeMap *curr_deleted_edge_map_;
      DelRecordManage *del_record_manager_;
      bool curr_have_map_;
    };

#endif

    // 利用暴力找最小值合并所有迭代器
    class EdgeIteratorTraverse {
    public:
      EdgeIteratorTraverse(const VertexId_t src,
                           MemTable *newmemTable,
                           std::vector<std::vector<SSTableCache *> *> &fileMetaCache,
                           SSTDataManager &sstdata_manager,
                           LevelIndex *vid_to_levelIndex,
                           SuperVersion *sv,
                           DelRecordManage *del_record_manager,
                           int max_level,
                           SequenceNumber_t seq = MAX_SEQ_ID, int property_id = -1)
              : seq_(seq), sv_(sv) {
        if (FLAGS_support_mulversion == true) {
#ifdef DEBUG_COST
          std::chrono::steady_clock::time_point iterate_memtable_t1 = std::chrono::steady_clock::now();
#endif
          if (max_level < 0) {
            return;
          }


          for (auto tb: sv_->get_memtable()) {
            NeighBors *neighbors = tb->get_vertex_adj(src);
            if (neighbors != nullptr) {
              it = std::shared_ptr<EdgeIteratorBase>(
                      new NeighBors::MemEdgeIterator(neighbors, tb->GetFid()));
              it_array.emplace_back(it);
            }
          }
#ifdef DEBUG_COST
          std::chrono::steady_clock::time_point iterate_memtable_t2 = std::chrono::steady_clock::now();
          std::chrono::duration<double> iterate_memtable_span = std::chrono::duration_cast<std::chrono::duration<double>>(
              iterate_memtable_t2 - iterate_memtable_t1);
          write_add(&iterate_memtable, iterate_memtable_span.count());
#endif

#ifdef DEBUG_COST
          std::chrono::steady_clock::time_point iterate_level_0_t1 = std::chrono::steady_clock::now();
#endif
          if (max_level >= 1) {
            // 需要满足 fileID >= min_level_0_fid
            FileId_t min_level_0_fid = sv_->findex.get_min_level_0_fid();
            for (auto sst_it: *(sv_->get_version()->GetLevel0Files())) {
              if (sst_it->header.timeStamp >= min_level_0_fid
                  && src <= sst_it->header.maxKey
                  && src >= sst_it->header.minKey) {
                find_iterator_sst(sst_it, src, sstdata_manager, max_level, property_id);
              }
            }
          }
#ifdef DEBUG_COST
          std::chrono::steady_clock::time_point iterate_level_0_t2 = std::chrono::steady_clock::now();
          std::chrono::duration<double> iterate_level_0_span = std::chrono::duration_cast<std::chrono::duration<double>>(
              iterate_level_0_t2 - iterate_level_0_t1);
          write_add(&iterate_level_0, iterate_level_0_span.count());
#endif
        } else {
          // memtable
          NeighBors *neighbors = newmemTable->get_vertex_adj(src);
          if (neighbors != nullptr) {
            it = std::shared_ptr<EdgeIteratorBase>(
                    new NeighBors::MemEdgeIterator(neighbors, newmemTable->GetFid()));
            if (it->valid()) {
              it_array.emplace_back(it);
            }
          }
          // sstable
          for (auto sst_it: *fileMetaCache[0]) {
            if (src <= sst_it->header.maxKey
                && src >= sst_it->header.minKey) {
              find_iterator_sst(sst_it, src, sstdata_manager, max_level, property_id);
            }
          }
        }

#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_level_1_t1 = std::chrono::steady_clock::now();
#endif
        // level>0, from levelindex
        if (max_level >= 2) {
          find_iterator_sst_by_levelindex(src, sstdata_manager, vid_to_levelIndex, max_level, property_id
          );
        }
#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_level_1_t2 = std::chrono::steady_clock::now();
        std::chrono::duration<double> iterate_level_1_span = std::chrono::duration_cast<std::chrono::duration<double>>(
            iterate_level_1_t2 - iterate_level_1_t1);
        write_add(&iterate_level_1, iterate_level_1_span.count());
#endif

#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_find_first_t1 = std::chrono::steady_clock::now();
#endif
        ptr_num_ = it_array.size();
        if (ptr_num_ > 0) {

          find_smallest();
          if (it != nullptr) {
            findFirstValid();
          }
        } else {
          it = nullptr;
        }
#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_find_first_t2 = std::chrono::steady_clock::now();
        std::chrono::duration<double> iterate_find_first_span = std::chrono::duration_cast<std::chrono::duration<double>>(
            iterate_find_first_t2 - iterate_find_first_t1);
        write_add(&iterate_find_first, iterate_find_first_span.count());
#endif
      }

      void find_iterator_sst_by_levelindex(const VertexId_t src,
                                           SSTDataManager &sstdata_manager,
                                           LevelIndex *vid_to_levelIndex,
                                           int max_level, int property_id
      ) {
        uint32_t fileID = 0;
        uint32_t offset = 0;
        uint32_t next_offset = 0;

        MulLevelIndex &findex = sv_->findex;

        for (int levelID = 0; levelID < max_level - 1; levelID++) {
          if (FLAGS_support_mulversion == false) {
            int index_id = src * LEVEL_INDEX_SIZE + levelID;
            LevelIndex &findex = vid_to_levelIndex[index_id];
            fileID = findex.get_fileID();

            if (fileID == INVALID_File_ID) {
              continue;
            }

            offset = findex.get_offset();
            next_offset = findex.get_next_offset();
            assert(next_offset >= offset);
          } else {
            fileID = findex.get_fileID(levelID);

            if (fileID == INVALID_File_ID) {
              continue;
            }

            offset = findex.get_offset(levelID);
            next_offset = findex.get_next_offset(levelID);
            assert(next_offset >= offset);
          }

          uint32_t adj_size = (next_offset - offset) / EDGEBODY_SIZE;
          assert(next_offset > offset);
          assert(adj_size > 0);
          if (FLAGS_OPEN_SSTDATA_CACHE == true) {
            SSTDataCache *sstcache = sstdata_manager.get_data(fileID);
            assert(sstcache != nullptr);
            std::shared_ptr<EdgeIteratorBase> it_temp = std::shared_ptr<EdgeIteratorBase>(
                    new SSTEdgeIterator(
                            (EdgeBody_t*)(sstcache->GetEdgeData() + offset),
                            sstcache->GetPropertyData(property_id),
                            adj_size,
                            fileID,
                            0,
                            GetSubPropertyOffsetByEdgeOrdinal(
                                GetEdgeOrdinalFromBodyOffset(offset), property_id),
                            GetSubPropertyFixedLength(property_id)));
            it_array.emplace_back(it_temp);
          } else {
            std::cout << " ToDo get_edges..." << std::endl;
          }
        }

      }

      Status find_iterator_sst(SSTableCache *sst_it, VertexId_t src,
                               SSTDataManager &sstdata_manager, int max_level,
                              int property_id) {


        int pos = sst_it->get(src);


        if (pos < 0) {
          return Status::kNotFound;
        }

        uint32_t offset = (sst_it->indexes)[pos].offset;
        uint32_t next_offset = (sst_it->indexes)[pos + 1].offset;

        if (FLAGS_OPEN_SSTDATA_CACHE == true) {
          SSTDataCache *sstcache = sstdata_manager.get_data(
                  sst_it->header.timeStamp);
          assert(sstcache != nullptr);
          uint32_t adj_size = (next_offset - offset) / EDGEBODY_SIZE;
          assert(next_offset > offset);
          assert(adj_size > 0);

          std::shared_ptr<EdgeIteratorBase> it_temp = std::shared_ptr<EdgeIteratorBase>(
                  new SSTEdgeIterator(
                          (EdgeBody_t*)(sstcache->GetEdgeData() + offset),
                          sstcache->GetPropertyData(property_id),
                          adj_size,
                          sst_it->header.timeStamp,
                          0,
                          GetSubPropertyOffsetByEdgeOrdinal(
                              GetEdgeOrdinalFromBodyOffset(offset), property_id),
                          GetSubPropertyFixedLength(property_id)));
          it_array.emplace_back(it_temp);
        } else {
          std::cout << " ToDo get_edges..." << std::endl;
        }
        return Status::kOk;
      }

      void init() {

      }

      bool valid() {
        return !(it == nullptr) && it->valid();
      }

      void findFirstValid() {
        if (it->sequence() < seq_) {
          if (it->marker() == true) { // deleted
            saved_key_ = it->dst_id();
            skipping_ = true;
          } else {
            skipping_ = false;
            return;
          }
        }
        next();
      }

      void find_smallest() {
        if (ptr_num_ == 0) {   // 是否每次判断没有必要
          it = nullptr;
          return;
        } else if (ptr_num_ == 1) {
          it = nullptr;
          if (it_array[0]->valid()) {
            it = it_array[0];
          }
          return;
        }
        it = nullptr;
        for (int i = 0; i < ptr_num_; i++) {
          std::shared_ptr<EdgeIteratorBase> child = it_array[i];
          if (child->valid()) {
            if (it == nullptr) {
              it = child;
            } else if (compare(child, it)) {
              it = child;
            }
          }
        }
      }

      void next() {
        if (!valid()) {
          return;
        }

        while (true) {
          it->next();
          find_smallest();

          if (it == nullptr) {
            return;
          }

          // check valid
          if (it->sequence() < seq_) {
            if (skipping_ == true && it->dst_id() == saved_key_) {
            } else {
              if (it->marker() == true) { // deleted
                saved_key_ = it->dst_id();
                skipping_ = true;
              } else {
                skipping_ = false;
                break;
              }
            }
          } else {
            if (it->dst_id() == saved_key_) {
            } else {
              saved_key_ = it->dst_id();
            }
          }
        }
      }

      VertexId_t dst_id() {
        return it->dst_id();
      }

      SequenceNumber_t sequence() {
        return it->sequence();
      }

      Marker_t marker() {
        return it->marker();
      }

      EdgeProperty_t edge_data(int id) {
        return it->edge_data(id);
      }

      bool empty() {
        return it->empty();
      }

      size_t size() {
        return it->size();
      }

      bool compare(const std::shared_ptr<EdgeIteratorBase> r1,
                   const std::shared_ptr<EdgeIteratorBase> r2) {
        if (r1->dst_id() == r2->dst_id()) {
          return r1->sequence() > r2->sequence();
        }
        return r1->dst_id() < r2->dst_id();
      }

      // clear all points
      ~EdgeIteratorTraverse() {
        it_array.clear();
      }

    private:
      std::shared_ptr<EdgeIteratorBase> it = nullptr;
      std::vector<std::shared_ptr<EdgeIteratorBase>> it_array;
      VertexId_t saved_key_ = INVALID_VERTEX_ID;
      bool skipping_ = false;
      SequenceNumber_t seq_;
      SuperVersion *sv_;
      short ptr_num_ = 0;
    };

    // 利用记录第一次出现的顶点来处理删除
    class EdgeIteratorTraverseOpt {
    public:
      EdgeIteratorTraverseOpt(const VertexId_t src,
                           MemTable *newmemTable,
                           std::vector<std::vector<SSTableCache *> *> &fileMetaCache,
                           SSTDataManager &sstdata_manager,
                           LevelIndex *vid_to_levelIndex,
                           SuperVersion *sv,
                           DelRecordManage *del_record_manager,
                           int max_level,
                           SequenceNumber_t seq = MAX_SEQ_ID,
                           int property_id = -1)
              : seq_(seq), sv_(sv) {
        if (max_level < 0) {
          return;
        }


#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_level_0_t1 = std::chrono::steady_clock::now();
#endif
        // sstable
        // level-0
        if (max_level >= 1) {
          // 需要满足 fileID >= min_level_0_fid
          FileId_t min_level_0_fid = sv_->findex.get_min_level_0_fid();
          for (auto sst_it: *(sv_->get_version()->GetLevel0Files())) {
            if (seq_ <= sst_it->seq_) {
              continue;
            }
            if (sst_it->header.timeStamp >= min_level_0_fid
                && src <= sst_it->header.maxKey
                && src >= sst_it->header.minKey) {
              find_iterator_sst(sst_it, src, sstdata_manager, property_id);
            }
          }
        }
#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_level_0_t2 = std::chrono::steady_clock::now();
        std::chrono::duration<double> iterate_level_0_span = std::chrono::duration_cast<std::chrono::duration<double>>(
            iterate_level_0_t2 - iterate_level_0_t1);
        write_add(&iterate_level_0, iterate_level_0_span.count());
#endif

#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_level_1_t1 = std::chrono::steady_clock::now();
#endif
        // level>0, from levelindex
        if (max_level >= 2) {
          find_iterator_sst_by_levelindex(src, sstdata_manager,
                                          vid_to_levelIndex, max_level, property_id);
        }
#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_level_1_t2 = std::chrono::steady_clock::now();
        std::chrono::duration<double> iterate_level_1_span = std::chrono::duration_cast<std::chrono::duration<double>>(
            iterate_level_1_t2 - iterate_level_1_t1);
        write_add(&iterate_level_1, iterate_level_1_span.count());
#endif

#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_memtable_t1 = std::chrono::steady_clock::now();
#endif
        for (auto tb: sv_->get_memtable()) {
          if (seq_ <= tb->GetStartTime()) {
            continue;
          }
          it = std::shared_ptr<EdgeIteratorBase>(
                  new NeighBors::MemEdgeIterator(tb->get_vertex_adj(src),
                                                 tb->GetFid()));
          if (it->valid()) {
            it_array.insert(it_array.begin(), it);
          }
        }
#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_memtable_t2 = std::chrono::steady_clock::now();
        std::chrono::duration<double> iterate_memtable_span = std::chrono::duration_cast<std::chrono::duration<double>>(
            iterate_memtable_t2 - iterate_memtable_t1);
        write_add(&iterate_memtable, iterate_memtable_span.count());
#endif

#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_find_first_t1 = std::chrono::steady_clock::now();
#endif

        if (!it_array.empty()) {
          findFirstValid();
        } else {
          it = nullptr;
        }
#ifdef DEBUG_COST
        std::chrono::steady_clock::time_point iterate_find_first_t2 = std::chrono::steady_clock::now();
        std::chrono::duration<double> iterate_find_first_span = std::chrono::duration_cast<std::chrono::duration<double>>(
            iterate_find_first_t2 - iterate_find_first_t1);
        write_add(&iterate_find_first, iterate_find_first_span.count());
#endif
      }

      void find_iterator_sst_by_levelindex(const VertexId_t src,
                                           SSTDataManager &sstdata_manager,
                                           LevelIndex *vid_to_levelIndex,
                                           int max_level, int property_id) {
        uint32_t fileID = 0;
        uint32_t offset = 0;
        uint32_t next_offset = 0;

        MulLevelIndex &findex = sv_->findex;

#ifndef MMAP_DIFF_SIZE_LEVEL_INDEX 
        for (int levelID = 0; levelID < max_level - 1; levelID++) {
#else   // 默认的和hot/cold都需要检测每一个level
        for (int levelID = 0; levelID < findex.get_level_num(); levelID++) {
#endif
          if (FLAGS_support_mulversion == false) {
            int index_id = src * LEVEL_INDEX_SIZE + levelID;
            LevelIndex &findex = vid_to_levelIndex[index_id];
            fileID = findex.get_fileID();

            if (fileID == INVALID_File_ID) {
              continue;
            }

            offset = findex.get_offset();
            next_offset = findex.get_next_offset();
            assert(next_offset >= offset);
          } else {
            fileID = findex.get_fileID(levelID);

            if (fileID == INVALID_File_ID) {
              continue;
            }

            offset = findex.get_offset(levelID);
            next_offset = findex.get_next_offset(levelID);
            assert(next_offset >= offset);
          }

          uint32_t adj_size = (next_offset - offset) / EDGEBODY_SIZE;
          if (FLAGS_OPEN_SSTDATA_CACHE == true) {
            SSTDataCache *sstcache = sstdata_manager.get_data(fileID);
            assert(sstcache != nullptr);
            std::shared_ptr<EdgeIteratorBase> it_temp = std::shared_ptr<EdgeIteratorBase>(
                    new SSTEdgeIterator(
                            (EdgeBody_t*)(sstcache->GetEdgeData() + offset),
                            sstcache->GetPropertyData(property_id),
                            adj_size,
                            fileID,
                            0,
                            GetSubPropertyOffsetByEdgeOrdinal(
                                GetEdgeOrdinalFromBodyOffset(offset), property_id),
                            GetSubPropertyFixedLength(property_id)));
            if (it_temp->valid()) {
#ifdef USE_MADVISE
            utils::use_madvise(sstcache->GetEdgeData(), offset, adj_size);
#endif
              it_array.emplace_back(it_temp);
            }
          } else {
            std::cout << " ToDo get_edges..." << std::endl;
          }
        }

      }

      Status find_iterator_sst(SSTableCache *sst_it, VertexId_t src,
                               SSTDataManager &sstdata_manager, int property_id) {

        int pos = sst_it->get(src);

        if (pos < 0) {
          return Status::kNotFound;
        }

        uint32_t offset = (sst_it->indexes)[pos].offset;
        uint32_t next_offset = (sst_it->indexes)[pos + 1].offset;

        if (FLAGS_OPEN_SSTDATA_CACHE == true) {
          SSTDataCache *sstcache = sstdata_manager.get_data(
                  sst_it->header.timeStamp);
          assert(sstcache != nullptr);
          uint32_t adj_size = (next_offset - offset) / EDGEBODY_SIZE;
          assert(next_offset >= offset);

          std::shared_ptr<EdgeIteratorBase> it_temp = std::shared_ptr<EdgeIteratorBase>(
                  new SSTEdgeIterator(
                          (EdgeBody_t*)(sstcache->GetEdgeData() + offset),
                          sstcache->GetPropertyData(property_id),
                          adj_size,
                          sst_it->header.timeStamp,
                          0,
                          GetSubPropertyOffsetByEdgeOrdinal(
                              GetEdgeOrdinalFromBodyOffset(offset), property_id),
                          GetSubPropertyFixedLength(property_id)));
          if (it_temp->valid()) {
#ifdef USE_MADVISE
            utils::use_madvise(sstcache->GetEdgeData(), offset, adj_size);
#endif
            it_array.emplace_back(it_temp);
          }
        } else {
          std::cout << " ToDo get_edges..." << std::endl;
        }
        return Status::kOk;
      }

      void init() {

      }

      bool valid() {
        return !(it == nullptr) && it->valid();
      }

      // 检查entry是否合法
      // Return only visible, non-deleted records not seen earlier in the
      // newest-to-oldest traversal.
      bool check_entry_valid() {
        assert(it != nullptr && it->valid());

        int64_t temp_seq = it->sequence();
        if (temp_seq > seq_) {  // 读不到的版本，太新了
          return false;
        }

        if (it->marker() != 0) {  // 删除边
          return false;
        }

        VertexId_t dst = it->dst_id();
        if (threadLocalSet.find(dst) != threadLocalSet.end()) {  // 已经访问过
          return false;
        }
        threadLocalSet.insert(dst);
 
        return true;
      }

      // Selects the first iterator containing a visible record.
      void findFirstValid() {
        it = nullptr;
        if (it_cnt >= it_array.size()) {
          return ;
        }

        do {
          // Newest-to-oldest order preserves tombstone semantics.
          it = it_array[it_cnt++];
          while (valid()) {
            if (check_entry_valid()) {
              return ;
            }
            it->next();
          }
        } while (it_cnt < it_array.size());

        if (it_cnt >= it_array.size()) {
          it = nullptr;
        }
      }

      void next() {
        while (true) {
          if (!valid()) {
            return;
          }
          it->next();

          if (!valid()) {
            findFirstValid();
            return ;
          } else if (check_entry_valid()) {
            return ;
          }
        }
      }

      VertexId_t dst_id() {
        return it->dst_id();
      }

      SequenceNumber_t sequence() {
        return it->sequence();
      }

      Marker_t marker() {
        return it->marker();
      }

      EdgeProperty_t edge_data(int id) {
        return it->edge_data(id);
      }

      bool empty() {
        return it->empty();
      }

      size_t size() {
        return it->size();
      }

      bool compare(const std::shared_ptr<EdgeIteratorBase> r1,
                   const std::shared_ptr<EdgeIteratorBase> r2) {
        if (r1->dst_id() == r2->dst_id()) {
          return r1->sequence() > r2->sequence();
        }
        return r1->dst_id() < r2->dst_id();
      }

      // clear all points
      ~EdgeIteratorTraverseOpt() {
        it_array.clear();
        threadLocalSet.clear();
      }

    private:
      std::shared_ptr<EdgeIteratorBase> it = nullptr;
      std::vector<std::shared_ptr<EdgeIteratorBase>> it_array;
      int it_cnt = 0;
      SuperVersion *sv_;
      SequenceNumber_t seq_;
      static thread_local std::unordered_set<VertexId_t> threadLocalSet;
      bool curr_have_map_;
    };

} // lsmgraph namespace
