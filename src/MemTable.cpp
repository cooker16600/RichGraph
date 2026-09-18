
#include <chrono>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <thread>
#include <random>
#include <memory>
#include <tbb/tbb.h>
#include <string.h>
#include <mutex>
#include <cassert>
#include <vector>
#include <set>
#include <limits>

#include "MemTable.h"
#include "io/file_io.h"
#include "edge_iterator.h"
#include "util/atomic.hpp"
#include "core/fixed_property_layout.h"
#include "core/storage_internal.h"
namespace lsmgraph {

#ifdef DEBUG_COST
    double MemTable::save2eSSTable_time = 0;
    double MemTable::put_flush_waite_time = 0;
    double MemTable::put_memtable_time = 0;
#endif

    void MemTable::put_edge(VertexId_t src,
                            VertexId_t dst,
                            const EdgeProperty_t &s,
                            Marker_t marker,
                            SequenceNumber_t seq,
                            size_t id,
                            bool is_out,
                            uint8_t edge_type) {
      #ifdef DEBUG_COST
      std::chrono::steady_clock::time_point t1 = std::chrono::steady_clock::now();
      #endif

      const size_t lock_slot = static_cast<size_t>(src % vertex_lock_count_);
      vertex_futexes_[lock_slot].lock();   // 5.7%
      write_max(&newest_edge, seq);
      if (vertex_adjs[src] == NULLPOINTER) {
        NeighBors *tempEdge_ = edge_arena.GetAnElementById(id); // 1.9%, 3.1%
        tempEdge_->put_edge(dst, seq, marker, s, is_out, edge_type);
        // 直接插入
#if VERTEX_ADJ_TYPE == 0
        vertex_adjs[src] = reinterpret_cast<uintptr_t>(tempEdge_);
#elif VERTEX_ADJ_TYPE == 1 || VERTEX_ADJ_TYPE == 2
        bool inserted
          = vertex_adjs.insert(src, reinterpret_cast<uintptr_t>(tempEdge_));
        if (inserted == false) {
          printf("error: insert fail..\n");
          exit(0);
        }
#endif
        vertex_futexes_[lock_slot].unlock(); // < 1%
        __sync_fetch_and_add(&src_vertex_num, 1);
        write_max(&vertex_max_level_[src], Level_t(0));
      } else {
        (reinterpret_cast<NeighBors *>(vertex_adjs[src]))->put_edge(
                dst, seq, marker, s, is_out, edge_type);
        vertex_futexes_[lock_slot].unlock(); // < 1%
      }

      __sync_fetch_and_add(&listLength, 1);
      assert(listLength <= max_edge_num); // An error will occur if MemTable's number of edges exceeds this number!
      #ifdef DEBUG_COST
      std::chrono::steady_clock::time_point t2 = std::chrono::steady_clock::now();
      std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
      write_add(&put_memtable_time, time_span.count());
      #endif
    }

    Status MemTable::get(VertexId_t src, VertexId_t dst, std::string *property) {
      return get(src, dst, true, 0, property);
    }

    Status MemTable::get(VertexId_t src, VertexId_t dst, bool is_out,
                         uint8_t edge_type, std::string *property) {
      auto prt = vertex_adjs[src];
      if (prt == NULLPOINTER) {
        return Status::kNotFound;
      } else {
        Status temp = (reinterpret_cast<NeighBors *>(
                prt))->get(dst, is_out, edge_type, property);
        return temp;
      }
    }

    Status MemTable::get(VertexId_t src, VertexId_t dst,
                         FileId_t &fid, SequenceNumber_t &seq) {
      auto prt = vertex_adjs[src];
      if (prt == NULLPOINTER) {
        return Status::kNotFound;
      } else {
        fid = this->fid_;
        Status temp = (reinterpret_cast<NeighBors *>(
                prt))->get(dst, seq);
        return temp;
      }
    }

    Status MemTable::get(VertexId_t src, VertexId_t dst, bool is_out,
                         uint8_t edge_type, FileId_t &fid,
                         SequenceNumber_t &seq) {
      auto prt = vertex_adjs[src];
      if (prt == NULLPOINTER) {
        return Status::kNotFound;
      }
      fid = this->fid_;
      return (reinterpret_cast<NeighBors *>(prt))
          ->get(dst, is_out, edge_type, seq);
    }

    void MemTable::get_edges(VertexId_t src, std::vector<Edge> &edges) {
      if (vertex_adjs[src] != NULLPOINTER) {
        (reinterpret_cast<NeighBors *>(vertex_adjs[src]))->get_edges(src, edges);

      }
    }

    void MemTable::save2eSSTable(const std::string &dir,
                                 uint64_t &currentTime,
                                 std::vector<SSTableCache *> *level_0_cache) {
      save2eSSTable_split(dir, currentTime, level_0_cache);

    }


    // Writes packed edge bodies, a source index, and a trailing header to the
    // topology file. Each property column is written to a separate side file.
    void MemTable::save2eSSTable_split_property(const std::string &dir,
      uint64_t &currentTime,
      std::vector<SSTableCache *> *level_0_cache,
      int property_num) {

      uint64_t temp_currentTime = this->GetFid();

      #ifdef DEBUG_COST
      std::chrono::steady_clock::time_point t1 = std::chrono::steady_clock::now();
      #endif

      uint64_t listLength = 0;
      if (remain_capacity > 0) {
        listLength = GetMaxEdgeNum() - remain_capacity;
      } else {
      listLength = GetMaxEdgeNum();
        checkIsFinishWrite();
      }


      if (FLAGS_richgraph_verbose && temp_currentTime % 10 == 0) {
        std::cout << " temp_currentTime=" << temp_currentTime << std::endl;
        std::cout << "listLength=" << listLength << std::endl;
        std::cout << "src_vertex_num1=" << src_vertex_num << std::endl;
        #if VERTEX_ADJ_TYPE == 1 || VERTEX_ADJ_TYPE == 2
        src_vertex_num = vertex_adjs.size();
        std::cout << "src_vertex_num2=" << src_vertex_num << std::endl;
        #endif
      }

      size_t write_all_edge_num = listLength;

      // build cache and buffers
      SSTableCache *cache = new SSTableCache(sstdata_manager_, newest_edge);
      cache->indexes.resize(src_vertex_num);

      uint32_t EdgeBody_size = sizeof(EdgeBody_t);
      uint32_t index_pair_size = (sizeof(VertexId_t) + 4);
      uint32_t sizeof_vid = sizeof(VertexId_t);

      // setting of parallel build sstable
      int chunk_num = FLAGS_max_subcompactions;
      int chunk_size = write_all_edge_num / chunk_num;
      std::vector<int> chunk_src_ids;
      std::vector<EdgeOffset_t> chunk_edge_offset;
      std::vector<EdgeOffset_t> chunk_index_offset;
      std::vector<EdgeOffset_t> chunk_property_offsets;

      chunk_src_ids.reserve(chunk_num + 1);
      chunk_edge_offset.reserve(chunk_num);
      chunk_index_offset.reserve(chunk_num);
                         
      chunk_property_offsets.reserve(chunk_num + 1);



      chunk_src_ids.push_back(0);
      chunk_edge_offset.push_back(0);
      chunk_index_offset.push_back(0);

      chunk_property_offsets.push_back(0);


      // get all source nodes
      VertexId_t i = 0;
      size_t edge_num = 0;
      size_t edge_num_sum = 0;
      // Tracks the accumulated bytes in property column zero; all columns use
      // fixed-width slots and therefore need no value-length pre-scan.
      size_t property_size_sum = 0;
      VertexId_t *vertex_set = new VertexId_t[src_vertex_num];

      #ifdef COUNT_MEMTABLE_USED_INFO
        size_t min_adj_size = 0xfffffff;
        size_t max_adj_size = 0;
      #endif
      VertexId_t temp_max_v_num = vertex_id_.load(std::memory_order_relaxed);

      #if VERTEX_ADJ_TYPE == 0 || VERTEX_ADJ_TYPE == 1
      for (VertexId_t id = 0; id < temp_max_v_num; ++id) {
        auto item = vertex_adjs[id];
        if (item != NULLPOINTER) {
          auto adjs = reinterpret_cast<NeighBors *>(item);
          size_t adj_num = adjs->getEdgeNum();
          if (edge_num + adj_num > chunk_size) {
            edge_num_sum += edge_num;
            chunk_src_ids.push_back(i);
            chunk_edge_offset.push_back(edge_num_sum * EdgeBody_size);
            chunk_index_offset.push_back(i * index_pair_size);
            chunk_property_offsets.push_back(property_size_sum);
            edge_num = 0;
          }
          edge_num += adj_num;
          property_size_sum += adj_num * GetSubPropertyFixedLength(0);
          
          vertex_set[i++] = id;
          if (i >= src_vertex_num) {
            break;
          }
          #ifdef COUNT_MEMTABLE_USED_INFO
          {
          min_adj_size = std::min(min_adj_size, adj_num);
          max_adj_size = std::max(max_adj_size, adj_num);
          }
          #endif
        }
      }
      #elif VERTEX_ADJ_TYPE == 2
      { // used to unorderedmap
        // uk-2002: time=0.43779sec
        std::vector<std::pair<VertexId_t, uintptr_t>> kv;
        kv.reserve(src_vertex_num);
        for(auto iterator1 = vertex_adjs.begin(); 
          iterator1 != vertex_adjs.end(); ++iterator1 ){
          if (iterator1->second == NULLPOINTER) {
            std::cout << " error.... line220" << std::endl;
          }
          kv.emplace_back(std::pair(iterator1->first, 
          iterator1->second));
        }
        std::cout << "src_vertex_num3=" << kv.size() << std::endl;

        tbb::parallel_sort(kv.begin(), kv.end(), 
        [](const std::pair<VertexId_t, uintptr_t>& x, 
          const std::pair<VertexId_t, uintptr_t>& y){
          return x.first < y.first;
          });

        for (auto& x : kv) {
          VertexId_t id = x.first;
          auto adjs = reinterpret_cast<NeighBors*>(x.second);
          size_t adj_num = adjs->getEdgeNum();
          if (edge_num + adj_num > chunk_size) {
            edge_num_sum += edge_num;
            chunk_src_ids.push_back(i);
            chunk_edge_offset.push_back(edge_num_sum * EdgeBody_size);
            chunk_index_offset.push_back(i * index_pair_size);
            chunk_property_offsets.push_back(property_size_sum);
            edge_num = 0;
          }
          edge_num += adj_num;
          property_size_sum += adj_num * GetSubPropertyFixedLength(0);
          vertex_set[i++] = id;
          #ifdef COUNT_MEMTABLE_USED_INFO
          {
            min_adj_size = std::min(min_adj_size, adj_num);
            max_adj_size = std::max(max_adj_size, adj_num);
          }
          #endif
        }
      }
      #endif

      edge_num_sum += edge_num;
      chunk_src_ids.push_back(i);
      chunk_property_offsets.push_back(property_size_sum);

      if (!(i == src_vertex_num && i > 0)) {
        printf("error: i=%ld src_vertex_num=%ld\n", i, src_vertex_num);
        exit(0);
      }
      if (edge_num_sum != listLength) {
        printf("error: edge_num_sum=%ld listLength=%ld\n",
        edge_num_sum, listLength);
        exit(0);
      }

      // count degree info
      if (temp_currentTime < 0) {
        #if VERTEX_ADJ_TYPE == 0 || VERTEX_ADJ_TYPE == 1

        // write degrees of each vertex to file
        std::string filename = "./degree/degree_" + FLAGS_dataset_name + "_" 
        + std::to_string(temp_currentTime) + ".degree";
        std::cout << "\nwrite to info of degree to: " << filename << std::endl;
        std::ofstream outFile(filename, std::ios::binary | std::ios::out);

        std::cout << "temp_max_v_num=" << temp_max_v_num << std::endl;
        for (VertexId_t id = 0; id < temp_max_v_num; ++id) {
          auto item = vertex_adjs[id];
          if (item != NULLPOINTER) {
            auto adjs = reinterpret_cast<NeighBors *>(item);
            size_t adj_num = adjs->getEdgeNum();
            outFile << adj_num << " ";
          }
        }

        outFile.close();
        #endif
      } else {
      }

      // 测试scan性能
      if (0) {
        std::cout << 
        "----------------------------------测试scan性能----------------------" 
        << std::endl;
        std::chrono::steady_clock::time_point t1 = std::chrono::steady_clock::now();
        double dst_sum = 0;
        for (int v_index = 0; v_index < src_vertex_num; v_index++) {
          VertexId_t cur_vid = vertex_set[v_index];

          NeighBors::MemEdgeIterator it = NeighBors::MemEdgeIterator(
          this->get_vertex_adj(cur_vid));
          for (; it.valid(); it.next()) {
            dst_sum += it.dst_id();
          }
        }
        std::chrono::steady_clock::time_point t2 = std::chrono::steady_clock::now();
        std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        std::cout << " scan_time=" << time_span.count() << std::endl;
        std::cout << " dst_sum=" << dst_sum << std::endl;
      }

      uint64_t real_efile_size = HEADER_SIZE 
      + EdgeBody_size * (write_all_edge_num + 1)
      + (src_vertex_num + 1) * index_pair_size;
      char *efile_buffer = new char[real_efile_size];

      char* pfile_buffers[property_num];
      for (int p_id = 0; p_id < property_num; ++p_id) {
        const auto file_size = static_cast<size_t>(write_all_edge_num) *
                               GetSubPropertyFixedLength(p_id);
        pfile_buffers[p_id] = new char[file_size];
      }

      // write head
      (cache->header).timeStamp = temp_currentTime;
      (cache->header).size = write_all_edge_num + 1; // Add an end flag
      (cache->header).index_size = src_vertex_num + 1;  // Add an end flag
      (cache->header).minKey = vertex_set[0];
      (cache->header).maxKey = vertex_set[i - 1];

      cache->seq_ = GetStartTime();

      char *index = efile_buffer + EDGEBODY_SIZE * (write_all_edge_num + 1);
      EdgeOffset_t body_offset = 0;
      char *body = efile_buffer + body_offset;
      VertexId_t last_srcId = std::numeric_limits<VertexId_t>::max();

      auto build_sstable = [&](int chunk_id) {
        char *temp_index = index + chunk_index_offset[chunk_id];
        char *temp_body = body + chunk_edge_offset[chunk_id];
        EdgeOffset_t temp_body_offset = body_offset + chunk_edge_offset[chunk_id];
        
        assert(chunk_src_ids.size() > chunk_id + 1);

        EdgePropertyOffset_t Offset_ = chunk_property_offsets[chunk_id];

        for (int v_index = chunk_src_ids[chunk_id]; v_index < chunk_src_ids[chunk_id + 1]; v_index++) {
          VertexId_t cur_vid = vertex_set[v_index];
          auto mem_ptr = this->get_vertex_adj(cur_vid);
          mem_ptr->sort();
          NeighBors::MemEdgeIterator it = NeighBors::MemEdgeIterator(
          mem_ptr);
          write_max(&vertex_max_level_[cur_vid], Level_t(1));

          // write edge index
          std::memcpy(temp_index, &cur_vid, sizeof(cur_vid));
          temp_index += sizeof(VertexId_t);
          std::memcpy(temp_index, &temp_body_offset, sizeof(temp_body_offset));
          temp_index += sizeof(EdgeOffset_t);
          cache->indexes[v_index].key = cur_vid;
          cache->indexes[v_index].offset = temp_body_offset;
          // write edge body
          for (; it.valid(); it.next()) {

            EdgeOffset_t newOffset = temp_body_offset + EdgeBody_size;
            if (newOffset > EDGEBODY_SIZE * write_all_edge_num) {
              printf("error: eBuffer Overflow in memtable to sstable!!!\n");
              exit(-1);
            }
            temp_body_offset = newOffset;
            EdgeBody_t edge_body(it.dst_id(), it.sequence(), Offset_,
                                 it.marker(), it.is_out(), it.edge_type());
            std::memcpy(temp_body, &edge_body, sizeof(edge_body));

            temp_body += EdgeBody_size;

            // 统一按定长子属性写入：
            // - 使用 `|` 作为子属性分隔符；
            // - 每个子属性写入固定槽位（不足补0，超长截断）；
            // - 这样查询时可用“边序号 + 固定长度”直接定位，无需边内偏移数组。
            const std::string property_raw = it.edge_data(-1);
            const int edge_ordinal = (temp_body - body) / EdgeBody_size - 1;

            int sub_property_id = 0;
            size_t segment_begin = 0;
            for (size_t k = 0; k <= property_raw.size() &&
                               sub_property_id < property_num; ++k) {
              if (k == property_raw.size() || property_raw[k] == '|') {
                const size_t segment_len = k - segment_begin;
                const auto slot_len = GetSubPropertyFixedLength(sub_property_id);
                char* slot_begin = pfile_buffers[sub_property_id]
                                 + edge_ordinal * slot_len;
                WritePaddedSubPropertySlot(
                    slot_begin,
                    property_raw.data() + segment_begin,
                    segment_len,
                    sub_property_id);
                ++sub_property_id;
                segment_begin = k + 1;
              }
            }
            for (; sub_property_id < property_num; ++sub_property_id) {
              const auto slot_len = GetSubPropertyFixedLength(sub_property_id);
              char* slot_begin = pfile_buffers[sub_property_id]
                               + edge_ordinal * slot_len;
              std::memset(slot_begin, 0, slot_len);
            }

            Offset_ += GetSubPropertyFixedLength(0);
          }
        }
      };
      if (FLAGS_max_subcompactions > 1) {
        std::vector<std::thread> threads;
        int threads_num = chunk_src_ids.size() - 1;
        const auto* property_layout_override =
            GetCurrentFixedPropertyLayoutOverride();
        threads.reserve(threads_num - 1);
        for (int i = 0; i < threads_num - 1; i++) {
          threads.emplace_back([&, i, property_layout_override]() {
            const ScopedFixedPropertyLayout property_layout(
                property_layout_override);
            build_sstable(i);
          });
        }
        build_sstable(threads_num - 1);
          for (int i = 0; i < threads_num - 1; i++) {
          threads[i].join();
        }
      } else {
        // A single subcompaction still owns one chunk.
        build_sstable(0);
      }
      
      // Add an end flag
      char *index_cur = efile_buffer + EdgeBody_size * (write_all_edge_num + 1)
      + index_pair_size * src_vertex_num;
      const VertexId_t invalid_vertex = INVALID_VERTEX_ID;
      std::memcpy(index_cur, &invalid_vertex, sizeof(invalid_vertex));
      index_cur += sizeof_vid;
      const EdgeOffset_t sentinel_offset =
          EdgeBody_size * write_all_edge_num;
      std::memcpy(index_cur, &sentinel_offset, sizeof(sentinel_offset));
      index_cur += sizeof(EdgeOffset_t);
      (cache->indexes).push_back(
      Index(INVALID_VERTEX_ID, EdgeBody_size * write_all_edge_num));

      // Add an end flag
      body_offset = EdgeBody_size * write_all_edge_num;
      EdgePropertyOffset_t Offset_ = chunk_property_offsets[chunk_property_offsets.size() - 1];
      
      
      char *body_cur = efile_buffer + body_offset;

      EdgeBody_t edge_body(INVALID_VERTEX_ID, INVALID_VERTEX_ID, Offset_, 0, true);
      std::memcpy(body_cur, &edge_body, sizeof(edge_body));
      body_cur += EdgeBody_size;
      // wirte head
      std::memcpy(efile_buffer + real_efile_size - HEADER_SIZE,
                  &cache->header,
                  sizeof(cache->header));

      std::string e_filename = eFileName(temp_currentTime);
      cache->path = e_filename;

      FileIO file(e_filename);
      if (file.Open(O_WRONLY | O_CREAT | O_TRUNC) == false) {
      std::cerr << "Failed to open file for writing." << std::endl;
      exit(1);
      }
      // The trailing header is part of the durable SST and is required for
      // recovery after reopening the database.
      file.Write(efile_buffer, real_efile_size);
      for(int i = 0; i < property_num; i++){
        std::string p_filename = pFileName(temp_currentTime) + "_" + std::to_string(i);
        FileIO p_outFile(p_filename);
        if (p_outFile.Open(O_WRONLY | O_CREAT | O_TRUNC) == false){
        std::cerr << "Failed to open file for writing." << std::endl;
        exit(1);
        } 
        const auto file_size = static_cast<size_t>(write_all_edge_num)
                             * GetSubPropertyFixedLength(i);
        p_outFile.Write(pfile_buffers[i], file_size);
        
      }
      int dir_fd = open(GetCurrentDbPath().data(), O_DIRECTORY | O_RDONLY);
      fsync(dir_fd);
      close(dir_fd);
      delete[] efile_buffer;
      efile_buffer = nullptr;

      for(int i = 0; i < property_num; i++){
        delete[] pfile_buffers[i];
        pfile_buffers[i] = nullptr;
      }
      delete[] vertex_set; // Frees array memory allocated on the heap
      vertex_set = nullptr;
      cache->newest_edge = newest_edge;
      sstdata_manager_.put_data(temp_currentTime, cache->header.size, reinterpret_cast<uintptr_t>(cache), newest_edge);
      // The new SST can now accept lazy-file property updates.
      change_sst_state(cache->header.timeStamp, true);

      #ifdef DEBUG_COST
      std::chrono::steady_clock::time_point put_flush_waite_timet1 = std::chrono::steady_clock::now();
      #endif

      const uint32_t level_0_max_sst_num_ =
          compactor_.l0_auto_compaction_limit();
      VersionEdit edit;
      edit.AddFile(cache);
      int try_compaction = 3;
      while (true) {
        if (FLAGS_support_mulversion == true) {

          // Publish a new version containing the flushed table.
          int curr_level_0file_num =
          l0_versionset_->GetCurrent()->GetLevel0Files()->size();

          if (curr_level_0file_num >= level_0_max_sst_num_) {
            try_compaction--;
            if (try_compaction == 0) {
              compactor_.MaybeScheduleCompaction();
              try_compaction = 3;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
          }
          Version *v = new Version(l0_versionset_);

          l0_versionset_->VersionLock();
          l0_versionset_->LogAndApply(edit, v);
          level_0_cache = l0_versionset_->GetCurrent()->GetLevel0Files();
          this->SetFlash(true);
          #ifndef VM_RW_LOCK
          std::shared_ptr<VersionAndMemTable>
          old_vm = std::atomic_load(&sv_.version_memtable);
          #else
          std::shared_ptr<VersionAndMemTableAndMemPropertyAndLf> old_vm;
          {
            std::shared_lock r_lock(sv_.vm_rw_mtx);
            old_vm = sv_.version_memtable_memproperty_lazyfile;
          }
          #endif
          std::shared_ptr<VersionAndMemTableAndMemPropertyAndLf> new_vms = std::make_shared<VersionAndMemTableAndMemPropertyAndLf>();
          new_vms->batch_insert_tb(old_vm->menTables);
          new_vms->batch_insert_pp(old_vm->memProperties);
          new_vms->batch_insert_lf(&old_vm->sst_has_lf);

          new_vms->remove_tb(this);
          new_vms->set_vs(l0_versionset_->GetCurrent());
          #ifndef VM_RW_LOCK
          std::atomic_store(&sv_.version_memtable, new_vm);
          #else
          {
            std::unique_lock w_lock(sv_.vm_rw_mtx);
            sv_.version_memtable_memproperty_lazyfile = new_vms;
          }
          #endif
          global_version_id_.fetch_add(1, std::memory_order_acquire);

          l0_versionset_->VersionUnLock();

          break;
        } else {
          std::unique_lock<std::mutex> lk(level_0_mux_);
          std::cout << " level_0_cache->size()=" << level_0_cache->size() << std::endl;
          if (level_0_cache->size() >= level_0_max_sst_num_) {
            lk.unlock();
            try_compaction--;
            if (try_compaction == 0) {
              compactor_.MaybeScheduleCompaction();
              try_compaction = 3;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
          }
          level_0_cache->push_back(cache);
          std::sort(level_0_cache->begin(), level_0_cache->end(), cacheTimeCompare);
          break;
        }
      }

      #ifdef DEBUG_COST
      std::chrono::steady_clock::time_point put_flush_waite_timet2 = std::chrono::steady_clock::now();
      std::chrono::duration<double> put_flush_waite_time_time_span = std::chrono::duration_cast<std::chrono::duration<double>>(
      put_flush_waite_timet2 - put_flush_waite_timet1);
      write_add(&put_flush_waite_time, put_flush_waite_time_time_span.count());

      std::chrono::steady_clock::time_point t2 = std::chrono::steady_clock::now();
      std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
      write_add(&save2eSSTable_time, time_span.count());
      #endif
}


    bool MemTable::checkIsFinishWrite() {
      // 执行落盘前需要保证所有写线程已经写入完毕
      while (GetListLength() != GetMaxEdgeNum()) {
        std::this_thread::sleep_for(std::chrono::microseconds(5));
      }
      return true;
    }

    void MemTable::Ref() {
      refs.fetch_add(1);
    }

    void MemTable::Unref() {
      assert(refs.load(std::memory_order_acquire) >= 0);
      refs.fetch_sub(1);
    }

    int32_t MemTable::Getref() { return refs.load(std::memory_order_acquire); }

    void MemTable::SetLive(bool _islive) {
      islive.store(_islive, std::memory_order_release);
    }

    void MemTable::SetFid(FileId_t fid) { fid_ = fid; }

    FileId_t MemTable::GetFid() const { return fid_; }

    void MemTable::SetFlash(bool _isflash) {
      isflash.store(_isflash, std::memory_order_release);
    }

    bool MemTable::IsLive() { return islive.load(std::memory_order_acquire); }

    bool MemTable::IsFlash() { return isflash.load(std::memory_order_acquire); }

    ///
    /// 主要功能是将memtable转化为level-0的ssttable,
    /// 此外值得注意的是，如果memtable的size
    /// 比sst大于2倍，则将memtable切分为多个ssttable.
    ///
    /// save skiplist to a efile and generate SSTableCache, efile include five
    /// parts:
    ///  | body(16B*edge_num) | index(12B*(src_num+1)) |
    ///  Head(8B*5=40B) | variable-length property bytes |
    void MemTable::save2eSSTable_split(const std::string &dir,
                                       uint64_t &currentTime,
                                       std::vector<SSTableCache *> *level_0_cache) {
      int cut_num = MAX_TABLE_SIZE / MAX_EFILE_SiZE;
      if (cut_num < 2) {
        save2eSSTable_split_property(dir, currentTime, level_0_cache,
                                     GetActiveSubPropertyNum());
        return;
      }
      assert(cut_num >= 2);

      #ifdef DEBUG_COST
      std::chrono::steady_clock::time_point t1 = std::chrono::steady_clock::now();
      #endif

      #ifdef DEBUG_COST
      std::chrono::steady_clock::time_point t2 = std::chrono::steady_clock::now();
      std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
      write_add(&save2eSSTable_time, time_span.count());
      #endif
    }


    void MemTable::print(std::string label = "") {
      std::cout << "===========SkipList::print("
                << label << ")======" << std::endl;
#if VERTEX_ADJ_TYPE == 0

#elif VERTEX_ADJ_TYPE == 1 || VERTEX_ADJ_TYPE == 2
      src_vertex_num = vertex_adjs.size();
#endif
      VertexId_t vertex_set[src_vertex_num];
      int i = 0;
      for (size_t id = 0; id < vertex_id_.load(std::memory_order_relaxed); id++) {
        if (vertex_adjs[id] != NULLPOINTER) {
          vertex_set[i++] = id;
        }
      }
      std::cout << " src_vertex_num=" << src_vertex_num << std::endl;
      std::cout << " edge_num=" << listLength << std::endl;
      assert(i == src_vertex_num && i > 0);
      std::sort(vertex_set, vertex_set + src_vertex_num);

      for (int v_index = 0; v_index < src_vertex_num; v_index++) {
        VertexId_t cur_vid = vertex_set[v_index];
        std::cout << "src=" << cur_vid << std::endl;

        NeighBors::MemEdgeIterator it = NeighBors::MemEdgeIterator(
                this->get_vertex_adj(cur_vid));
        for (; it.valid(); it.next()) {
          it.key().print();
        }
      }

      std::cout << "=========================================\n" << std::endl;
    }

    uint64_t MemTable::GetListLength() { return listLength; }

    template<typename T>
    void MemTable::AutomicAdd(T &a, T b) {
      __sync_fetch_and_add(&a, b);
    }

    uint64_t MemTable::GetMaxEdgeNum() { return max_edge_num; }

    void MemTable::SetStartTime(SequenceNumber_t start_time) {
      start_time_ = start_time;
    }

    SequenceNumber_t MemTable::GetStartTime() {
      return start_time_;
    }

    void MemTable::reset() {
      assert(refs.load(std::memory_order_acquire) == 0);
      clear_vertex_adj();
      listLength = 0;
      remain_capacity = max_edge_num;
      newest_edge.store(0, std::memory_order_relaxed);
      edge_arena.Reset();
      SetLive(false);
      SetFlash(false);
    }

    void MemTable::clear_vertex_adj() {
      src_vertex_num = 0;
#if VERTEX_ADJ_TYPE == 0
#pragma omp parallel for num_threads(FLAGS_thread_num)
      for (size_t i = 0; i < vertex_id_.load(std::memory_order_relaxed); i++) {
        vertex_adjs[i] = NULLPOINTER;
      }
#elif VERTEX_ADJ_TYPE == 1 || VERTEX_ADJ_TYPE == 2
      vertex_adjs.clear();
#endif
    }

    NeighBors *MemTable::get_vertex_adj(VertexId_t src) {
    assert(vertex_id_.load(std::memory_order_relaxed) >= src);
      return reinterpret_cast<NeighBors *>(vertex_adjs[src]);
    }
  
    void MemTable::update_edge(VertexId_t src, VertexId_t dst, const EdgeProperty_t& s, Marker_t markear, SequenceNumber_t seq, int sub_property_id){
      // Reconstruct the complete property tuple around the updated column.
      EdgeProperty_t tmp_property;
      for(int i = 0; i < sub_property_id; i++){
        tmp_property += "~INPLACED~|";
      }
      tmp_property += s;
      for(int i = sub_property_id; i < GetActiveSubPropertyNum() - 1; i++){
        tmp_property += "|~INPLACED~";
      }
      auto prt = vertex_adjs[src];
      if(prt == NULLPOINTER){
        std::cout<<"src dont exist err!\n";
        exit(-1);
      }
      auto neighbors = (reinterpret_cast<NeighBors *>(prt));
      neighbors->update_edge(dst, seq, markear, tmp_property);
    }

    void MemTable::change_sst_state(FileId_t sst_id, bool state){
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
}
