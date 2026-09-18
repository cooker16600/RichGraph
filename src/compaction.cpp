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
#include <thread>

#include "core/utils.h"
#include "core/lsmstore.h"
#include "core/types.h"
#include "core/graph/edge.h"
#include "core/graph/edge_record.h"
#include "core/compaction.h"
#include "core/DataStructs.h"
#include "core/cache/SSTDataCache.h"

namespace lsmgraph {

/// is_grow表示是否利用已有范围对上进行范围扩展
/// 注意: 对于level>=1且多线程时需要设置为false
bool Compaction::PickCompactioFile(const int level, const bool is_grow) {
  bool is_reverse = false;
  for (auto f : *fileMetaCache_[level]) {
    // 注意：下面使用f->header.minKey或者f->header.maxKey会导致不一样的性能，min会导致
    //      compaction快，但是可能会导致顶点的边更加分散。
    if (compact_pointer_[level] == MAX_GLOBAL_SEQ ||
        f->header.minKey > compact_pointer_[level]) {
      inputs_[0].push_back(f);
      break;
    }
  }

  if (inputs_[0].empty()) {
    // Wrap-around to the beginning of the key space
    inputs_[0].push_back((*fileMetaCache_[level])[0]);
    is_reverse = true;
  }
  // Files in level 0 may overlap each other, so pick up all overlapping ones
  // 必须保证level-0范围有交叉的文件全部选择出来
  if (level == 0) {
    VertexId_t smallest, largest;
    GetRange(inputs_[0], smallest, largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    GetOverlappingInputs(0, smallest, largest, &inputs_[0]);
    assert(!inputs_[0].empty());
  }

  if (fileMetaCache_.size() > level + 1) {
    SetupOtherInputs(level, is_grow);
  } else {
    VertexId_t smallest, largest;
    GetRange(inputs_[0], smallest, largest);
    compact_pointer_[level] = largest; // update pointer
  }
  return is_reverse;
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
void Compaction::GetRange2(const std::vector<SSTableCache*>& inputs1,
                           const std::vector<SSTableCache*>& inputs2,
                           VertexId_t& smallest, VertexId_t& largest) {
  std::vector<SSTableCache*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void Compaction::GetRange(const std::vector<SSTableCache*>& inputs,
                        VertexId_t& smallest, VertexId_t& largest) {
  assert(!inputs.empty());
  for (size_t i = 0; i < inputs.size(); i++) {
    SSTableCache* f = inputs[i];
    if (i == 0) {
      smallest = f->header.minKey;
      largest = f->header.maxKey;
    } else {
      if (f->header.minKey < smallest) {
        smallest = f->header.minKey;
      }
      if (f->header.maxKey > largest) {
        largest = f->header.maxKey;
      }
    }
  }
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
void Compaction::GetOverlappingInputs(const int level, const VertexId_t begin,
                                    const VertexId_t end, 
                                    std::vector<SSTableCache*>* inputs) {
  assert(level >= 0);
  assert(level < MAX_LEVEL);
  inputs->clear();
  VertexId_t user_begin = begin, user_end = end;

  std::vector<SSTableCache*>& fileMetaCache_level = *fileMetaCache_[level];
  for (size_t i = 0; i < fileMetaCache_[level]->size();) {
    SSTableCache* f = fileMetaCache_level[i++];
    const VertexId_t file_start = f->header.minKey;
    const VertexId_t file_limit = f->header.maxKey;
    if (file_limit < user_begin) {
      // "f" is completely before specified range; skip it
    } else if (file_start > user_end) {
      // "f" is completely after specified range; skip it
    } else {
      inputs->push_back(f);
      if (level == 0) {
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        if (file_start < user_begin) {
          user_begin = file_start;
          inputs->clear();
          i = 0;
        } else if (file_limit > user_end) {
          user_end = file_limit;
          inputs->clear();
          i = 0;
        }
      }
    }
  }
}

// find files in level n+1 and update files in level n
void Compaction::SetupOtherInputs(const int pre_level, const bool is_grow) {
  assert(pre_level >= 0);
  VertexId_t smallest, largest;
  GetRange(inputs_[0], smallest, largest);

  GetOverlappingInputs(pre_level + 1, smallest, largest, &inputs_[1]);

  // Get entire range covered by compaction
  VertexId_t all_start, all_limit;
  GetRange2(inputs_[0], inputs_[1], all_start, all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  if (is_grow && !inputs_[1].empty()) {
    std::vector<SSTableCache*> expanded0;
    GetOverlappingInputs(pre_level, all_start, all_limit, &expanded0);
    if (expanded0.size() > inputs_[0].size()) {
      VertexId_t new_start, new_limit;
      GetRange(expanded0, new_start, new_limit);
      std::vector<SSTableCache*> expanded1;
      GetOverlappingInputs(pre_level + 1, new_start, new_limit, &expanded1);
      if (expanded1.size() == inputs_[1].size()) {
        smallest = new_start;
        largest = new_limit;
        inputs_[0] = expanded0;
        inputs_[1] = expanded1;
        GetRange2(inputs_[0], inputs_[1], all_start, all_limit);
      }
    }
  }

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  compact_pointer_[pre_level] = largest;
  last_merge_max_key_ = std::max(largest, all_limit);
}

// TODO(correctness): Cache removal must preserve file metadata while any
// multiversion reader can still reference it.
// level=0时, 无序的，需要O(m*k)查找, k是inputs_[level]的size, m是cache[level]的size
// level>=1时, 有序的，需要O(m+k)查找
// 参数free_space: 表示是否释放其空间
void Compaction::RemoveCache(const int level, const int inputs_index,
                             bool free_space) {
    if (level >= fileMetaCache_.size()
        || merged_fids_[inputs_index].size() == 0) {
      return;
    }
    // level-0 delete merged files
    if (FLAGS_support_mulversion == true && level == 0 && inputs_index == 0) {
      return ;
    }
    std::lock_guard<std::mutex> lock(*tablecache_mutex_[level]);
    auto iter = fileMetaCache_[level]->begin();
    while (iter != fileMetaCache_[level]->end()) {
      bool is_del = false;
      if (merged_fids_[inputs_index].find((*iter)->header.timeStamp)
          != merged_fids_[inputs_index].end()) {
        is_del = true;
      }
      if (is_del) {
        if (free_space) {
          delete *iter; // 因为下面的erase并不会释放其空间
        }
        iter = fileMetaCache_[level]->erase(iter);
      } else {
        ++iter;
      }
    }
}

// 将文件放入待删除队列
void Compaction::RemoveFile() {
  // remove all merged file
  for (auto del_fid : merged_fids_[0]) {
      delete_fids_set_.Put(del_fid);
  }
  for (auto del_fid : merged_fids_[1]) {
      delete_fids_set_.Put(del_fid);
  }
}

// 将文件从磁盘删除
void Compaction::RealRemoveFile() {
  // remove all merged file
  if (FLAGS_richgraph_verbose) {
    std::cout << " real remove files." << std::endl;
  }
  while (delete_fids_set_.Size() > 0) {
      FileId_t fid = -1;
      delete_fids_set_.Get(fid);
      auto rt = utils::rmfile(eFileName(fid).c_str());
      assert(rt == 0);
      std::string pfile_name = pFileName(fid);
      for(int i = 0; i < static_cast<int>(sub_property_lengths_.size()); i++){
        std::string sub_pfile_name = pFileName_with_id(pfile_name, i);
        rt = utils::rmfile(sub_pfile_name.c_str());
      }
      assert(rt == 0);
  }
}

/// 当level>=1时，level和level+1每一层都是有序的，所以直接进行二路归并即可
void Compaction::DoCompactionWorkTwoWay(const int level,

                                        const int inputs_0_size,
                                        const int inputs_1_size,
                                        std::vector<EdgeRecord>& edge_cache,
                                        std::vector<Way>& ways) {
  assert(level >= 1);
  assert(inputs_0_size >= 1);

  // [0, inputs_0_size) [inputs_0_size, inputs_1_size)
  int way_num = inputs_0_size + inputs_1_size;
  int level_way_ptr = 0;
  int next_level_way_ptr = inputs_0_size;
  int work_way = way_num;

  edge_cache.resize(2);
  if (inputs_0_size > 0) {
    ways[level_way_ptr].Init();
    edge_cache[0] = ways[level_way_ptr].NextEdgeRecord().value();
  }
  if (inputs_1_size > 0) {
    ways[next_level_way_ptr].Init();
    edge_cache[1] = ways[next_level_way_ptr].NextEdgeRecord().value();
  }

  // find min edge
  int min_edge_index = 0;
  int min_way_index = level_way_ptr;
  if (inputs_1_size > 0 && edge_cache[1] < edge_cache[0]) {
    min_edge_index = 1;
    min_way_index = next_level_way_ptr;
  }

  auto writer = SSTableWriter(fileMetaCache_, vid_to_levelIndex,
                              vid_to_mullevelIndex_,
                              vertex_futexes, vertex_rwlocks_,
                              vertex_max_level_,
                              vertex_lock_count_,
                              min_level_0_fid_,
                              level, buffer_manager,
                              tablecache_mutex_,
                              version_edit_,
                              sstdata_manager_,
                              sst_is_vaild_to_ins_lf_,
                              lf_mutex_);
  uint64_t temp_currentTime = __sync_fetch_and_add(&currentTime, 1);
  writer.Init(eFileName(temp_currentTime),
              pFileName(temp_currentTime),
              edge_cache[min_edge_index].src_,
              temp_currentTime);
  for (int i = 0; i < inputs_0_size; i++) {
    writer.InsertInput0Fid(ways[i].GetFildId());
  }

#ifdef DEL_EDGE_SEPARATE
  std::vector<bool> way_have_del_edge(way_num, false);
  std::vector<EidToTimeMap*> way_map(way_num, nullptr);
  for (int i = 0; i < ways.size(); i++) {
    EidToTimeMap* deleted_edge_map;
    bool have_del_edge = del_record_manager_.find_eidmap(ways[i].GetFildId(),
                                                         deleted_edge_map);
    if (have_del_edge == true) {
      way_have_del_edge[i] = true;
      way_map[i] = deleted_edge_map;
    } else {
    }
  }
#endif

  /// obj: 将level的数据合并到level+1
  // Optionally include adjacent levels in the current merge.
  ///  具体:
  ///    1.合并时，如果该顶点的level_index被置为空则不需要参与合并，直接下一个顶点
  ///    2.当合并过程遍历到顶点v时，检查其level_index,读取其在其他level的信息，如果满足条件
  ///    则加入到合并队列中，即加入一个way
  ///    3.合并时标记已经参与的其他level的leve_index为已copy状态
  ///    4.最后修改level_index时，需要将这些带标记的全部只为空
#ifdef OPT_MERGE_MULTI_LEVEL
  VertexId_t last_src = INVALID_VERTEX_ID;
  int SMALL_DEGREE_T = FLAGS_multi_level_merge_degree;
  bool is_merge = false;
  std::vector<SSTEdgeIterator> iter_set;
  iter_set.reserve(2);
  EdgeRecord write_merge_edge;
  VertexId_t last_src_way[2] = {INVALID_VERTEX_ID, INVALID_VERTEX_ID};
  // 统计每个iter, 并对应src是否有，或者全部比较
#endif

  // TODO(correctness): Duplicate insert/delete records must remain adjacent
  // during ordering; otherwise a tombstone can be separated from the edge it
  // invalidates.
#ifndef MERGE_LARGE_VERTEX
  while(work_way > 0) {
    auto& edge_record = edge_cache[min_edge_index];

#ifdef OPT_MERGE_MULTI_LEVEL
    if (last_src != edge_record.src_) {
      // 可以用max_level来过滤，避免检查
      while (true) {
        int small_it = -1;
        for (int i = 0; i < iter_set.size(); i++) {
          if (iter_set[i].valid() && (last_src < edge_record.src_
                                      || edge_record > iter_set[i])) {
            small_it = i;
          }
        }
        if (small_it == -1) {
          break;
        } else {
          write_merge_edge.SetValue(last_src,
                                    iter_set[small_it].dst_id(),
                                    iter_set[small_it].sequence(),
                                    iter_set[small_it].edge_slice_data(), // 这里拿到的是string_view
                                    iter_set[small_it].marker(),
                                    iter_set[small_it].is_out(),
                                    iter_set[small_it].edge_type());
          if(!writer.Write(write_merge_edge)) { // if output file full
              temp_currentTime = __sync_fetch_and_add(&currentTime, 1);
              writer.Init(eFileName(temp_currentTime),
                          pFileName(temp_currentTime),
                          write_merge_edge.src_,
                          temp_currentTime);
              bool rt = writer.Write(write_merge_edge);
              assert(rt == true); // 这次写必须成功！
          }
          iter_set[small_it].next();
        }
      }

      iter_set.clear();
      last_src = edge_record.src_;
      uint32_t fileID = 0;
      uint32_t offset = 0;
      uint32_t next_offset = 0;
      uint32_t adj_size_sum = 0;
      MulLevelIndex& findex = MutableMulLevelIndex(last_src);
      for (int levelID = 0; levelID < LEVEL_INDEX_SIZE; levelID++) {
        if (levelID == level - 1 || levelID == level) {
          continue;
        }
        fileID = findex.get_fileID(levelID);
        if (fileID == INVALID_File_ID) {
          continue;
        }
        offset = findex.get_offset(levelID);
        next_offset = findex.get_next_offset(levelID);
        assert(next_offset >= offset);
        uint32_t adj_size = (next_offset - offset) / EDGEBODY_SIZE;
        adj_size_sum += adj_size;
      }
      if (adj_size_sum > 0 && adj_size_sum <= SMALL_DEGREE_T) {
        MulLevelIndex& findex = MutableMulLevelIndex(last_src);
        for (int levelID = 0; levelID < LEVEL_INDEX_SIZE; levelID++) {
          if (levelID == level - 1 || levelID == level) {
            continue;
          }
          fileID = findex.get_fileID(levelID);
          if (fileID == INVALID_File_ID) {
            continue;
          }
          offset = findex.get_offset(levelID);
          next_offset = findex.get_next_offset(levelID);
          assert(next_offset >= offset);
          uint32_t adj_size = (next_offset - offset) / EDGEBODY_SIZE;
          SSTDataCache* sstcache = sstdata_manager_.get_data(fileID);
          assert(sstcache != nullptr);
          iter_set.emplace_back(
                        (EdgeBody_t *)(sstcache->GetEdgeData() + offset),
                        sstcache->GetPropertyData(),
                        adj_size,
                        fileID);
          // 需求修改迭代器标记，表示该内容已经被copy了
          findex.setCopyFlag(levelID, 1);
        }
      }
    }

    {
      while (true) {
        int small_it = -1;
        for (int i = 0; i < iter_set.size(); i++) {
          if (iter_set[i].valid() && edge_record > iter_set[i]) {
            small_it = i;
          }
        }
        if (small_it == -1) {
          break;
        } else {

          write_merge_edge.SetValue(last_src,
                                    iter_set[small_it].dst_id(),
                                    iter_set[small_it].sequence(),
                                    iter_set[small_it].edge_slice_data(), // 这里拿到的是string_view
                                    iter_set[small_it].marker(),
                                    iter_set[small_it].is_out(),
                                    iter_set[small_it].edge_type());

          if(!writer.Write(write_merge_edge)) { // if output file full

              temp_currentTime = __sync_fetch_and_add(&currentTime, 1);
              writer.Init(eFileName(temp_currentTime),
                          pFileName(temp_currentTime),
                          write_merge_edge.src_,
                          temp_currentTime);
              bool rt = writer.Write(write_merge_edge);
              assert(rt == true); // 这次写必须成功！
          }
          iter_set[small_it].next();
        }
      }
    }
#endif

#ifdef DEL_EDGE_SEPARATE
    // Merge the deleted records of this edge, if they exist.
     if (way_have_del_edge[min_way_index] == true
        && edge_record.marker_ == false) {
      SequenceNumber_t eid = edge_record.seq_;
      SequenceNumber_t del_time;
      if (del_record_manager_.get_time(way_map[min_way_index], eid, del_time)) {

        // TODO(correctness): Keep deletion metadata visible until the output
        // SST is published. Removing it here can make concurrent readers miss
        // the record; transfer it before retiring the old map entry.
        del_record_manager_.del_eid_frome_eidmap(way_map[min_way_index], eid);

#ifndef REAL_DELETE
        // write edge to output file
        // 这里的本意是，在原始插入的边前面插入一条删除边的记录，这样就和RocksDB追加删除边的
        // 操作类似，但是其实可以选择: 1.直接清理调这条边(即真正删除); 2.给原始边edge_record
        // 标记位置位1就行了，也可以保证读取时不会读取它。
        // 下面选择: 2
        edge_record.marker_ = true;
#endif
      }
    }
#endif

    // write edge to output file
#ifndef REAL_DELETE
    if(!writer.Write(edge_record)) { // if output file full
#else
    if(!way_have_del_edge[min_way_index] && !writer.Write(edge_record)) { // if output file full
#endif
        temp_currentTime = __sync_fetch_and_add(&currentTime, 1);
        writer.Init(eFileName(temp_currentTime),
                    pFileName(temp_currentTime),
                    edge_record.src_,
                    temp_currentTime);
        bool rt = writer.Write(edge_record);
        assert(rt == true); // 这次写必须成功！
    }

    auto new_edge_record_opt = ways[min_way_index].NextEdgeRecord();

#ifdef OPT_MERGE_MULTI_LEVEL
    // 如果切换了source,并且该source则需要检测是否为冗余边
    // 如果已经被copy则需要跳过该source的所有出边
    int level_id = level + (min_way_index >= inputs_0_size);
    int index_i = (min_way_index >= inputs_0_size);
    while (new_edge_record_opt != std::nullopt
           && new_edge_record_opt.value().src_ != last_src_way[index_i]) {
      last_src_way[index_i] = new_edge_record_opt.value().src_;
      MulLevelIndex& findex = MutableMulLevelIndex(last_src_way[index_i]);
      assert(level_id - 1 >= 0);
      if (findex.get_fileID(level_id - 1) == INVALID_File_ID) { // was copied
        do {
          new_edge_record_opt = ways[min_way_index].NextEdgeRecord();
        } while (new_edge_record_opt != std::nullopt
                 && new_edge_record_opt.value().src_ == last_src_way[index_i]);
      } else {
        break;
      }
    }
#endif

    if(new_edge_record_opt == std::nullopt) {
      ways[min_way_index].FreeBuffer();
      work_way--;
      if (work_way <= 0) {
        break;
      }
      level_way_ptr += (min_edge_index == 0);
      next_level_way_ptr += (min_edge_index == 1);
      if (min_edge_index == 0 && level_way_ptr < inputs_0_size) {
        ways[level_way_ptr].Init();
        edge_cache[0] = ways[level_way_ptr].NextEdgeRecord().value();
      } else if (min_edge_index == 1 && next_level_way_ptr < way_num) {
        assert(inputs_1_size > 0);
        ways[next_level_way_ptr].Init();
        edge_cache[1] = ways[next_level_way_ptr].NextEdgeRecord().value();
      }
    } else {
      edge_cache[min_edge_index] = new_edge_record_opt.value();
    }

    // find min edge
    if (level_way_ptr == inputs_0_size) {
      min_edge_index = 1;
      min_way_index = next_level_way_ptr;
      assert(inputs_1_size > 0);
    } else if (next_level_way_ptr == way_num) {
      min_edge_index = 0;
      min_way_index = level_way_ptr;
    } else {
      assert(inputs_1_size > 0);
      min_edge_index = 0;
      min_way_index = level_way_ptr;
      if (edge_cache[1] < edge_cache[0]) {
        min_edge_index = 1;
        min_way_index = next_level_way_ptr;
      }
    }
    #ifndef REAL_DELETE
  }
    #else
  }
    #endif

#ifdef OPT_MERGE_MULTI_LEVEL
  // Drain copied iterators after all file-backed ways are exhausted.
  while (true) {
    int small_it = -1;
    for (int i = 0; i < iter_set.size(); i++) {
      if (iter_set[i].valid()) {
        small_it = i;
      }
    }
    if (small_it == -1) {
      break;
    } else {
      write_merge_edge.SetValue(last_src,
                                iter_set[small_it].dst_id(),
                                iter_set[small_it].sequence(),
                                iter_set[small_it].edge_slice_data(), // 这里拿到的是string_view
                                iter_set[small_it].marker(),
                                iter_set[small_it].is_out(),
                                iter_set[small_it].edge_type());
      if(!writer.Write(write_merge_edge)) { // if output file full
          temp_currentTime = __sync_fetch_and_add(&currentTime, 1);
          writer.Init(eFileName(temp_currentTime),
                      pFileName(temp_currentTime),
                      write_merge_edge.src_,
                      temp_currentTime);
          bool rt = writer.Write(write_merge_edge);
          assert(rt == true); // 这次写必须成功！
      }
      iter_set[small_it].next();
    }
  }
#endif
#else
  while (work_way > 0) {
    VertexId_t  cur_vertex = edge_cache[min_edge_index].src_;
    for (int i = 0, way_ptr = level_way_ptr; i < ((inputs_0_size > 0) + (inputs_1_size > 0)) && work_way > 0; ++i,
            way_ptr = next_level_way_ptr) {
      if (ways[way_ptr].IsReleased() || edge_cache[i].src_ != cur_vertex) {
        continue;
      }
      VertexId_t cur_src = edge_cache[i].src_;
      MulLevelIndex &fidx = MutableMulLevelIndex(cur_src);
      int fileID = fidx.get_fileID(level - 1 + i);
      if (fileID != INVALID_File_ID ) {
        continue;
      }
#ifdef MERGE_LARGE_VERTEX_DEBUG
      printf("compaction level: %d, src: %lu, edge num: %lu\n",
             level - 1 + i,
             cur_src,
             (fidx.get_next_offset(level - 1 + i) - fidx.get_offset(level - 1 + i)) /sizeof (EdgeBody_t));
#endif

      while (edge_cache[i].src_ == cur_src) {
#ifdef MERGE_LARGE_VERTEX_DEBUG
        std::cout << "skip edge: "
               << " " <<  edge_cache[i].src_
               << " " <<  edge_cache[i].dst_
               << " " <<  edge_cache[i].seq_ << std::endl;
#endif
        auto new_edge_opt = ways[way_ptr].NextEdgeRecord();
        if (new_edge_opt == std::nullopt) {
          ways[way_ptr].FreeBuffer();
          work_way--;
          if (work_way <= 0) {
            break;
          }
          level_way_ptr += (i == 0);
          next_level_way_ptr += (i == 1);
          if (i == 0 && level_way_ptr < inputs_0_size) {
            ways[level_way_ptr].Init();
            edge_cache[0] = ways[level_way_ptr].NextEdgeRecord().value();
          } else if (i == 1 && next_level_way_ptr < way_num) {
            assert(inputs_1_size > 0);
            ways[next_level_way_ptr].Init();
            edge_cache[1] = ways[next_level_way_ptr].NextEdgeRecord().value();
          }
          break;
        } else {
          edge_cache[i] = new_edge_opt.value();
        }
      }
      if (level_way_ptr == inputs_0_size) {
        min_edge_index = 1;
        min_way_index = next_level_way_ptr;
        assert(inputs_1_size > 0);
      } else if (next_level_way_ptr == way_num) {
        min_edge_index = 0;
        min_way_index = level_way_ptr;
      } else {
        assert(inputs_1_size > 0);
        min_edge_index = 0;
        min_way_index = level_way_ptr;
        if (edge_cache[1] < edge_cache[0]) {
          min_edge_index = 1;
          min_way_index = next_level_way_ptr;
        }
      }
    }
    if (work_way < 0 || edge_cache[min_edge_index].src_ != cur_vertex) {
      continue;
    }

    while (work_way > 0 && cur_vertex == edge_cache[min_edge_index].src_) {
      auto& edge_record = edge_cache[min_edge_index];

#ifdef MERGE_LARGE_VERTEX_DEBUG
      printf("two way: %lu %lu %lu\n",edge_record.src_, edge_record.dst_, edge_record.seq_);
#endif
#ifdef DEL_EDGE_SEPARATE
      if (way_have_del_edge[min_way_index] == true
          && edge_record.marker_ == false) {
        SequenceNumber_t eid = edge_record.seq_;
        SequenceNumber_t del_time;
        if (del_record_manager_.get_time(way_map[min_way_index], eid, del_time)) {

          EdgeRecord del_edge_record(edge_record.src_,
                                     edge_record.dst_,
                                     del_time,
                                     std::vector<Slice>{},
                                     1,
                                     edge_record.is_out_,
                                     edge_record.edge_type_);

          // TODO(correctness): Keep deletion metadata visible until the output
          // SST is published. Removing it here can make concurrent readers miss
          // the record; transfer it before retiring the old map entry.
          del_record_manager_.del_eid_frome_eidmap(way_map[min_way_index], eid);
          if (!writer.Write(del_edge_record)) { // if output file full
            temp_currentTime = __sync_fetch_and_add(&currentTime, 1);
            writer.Init(eFileName(temp_currentTime),
                        pFileName(temp_currentTime),
                        edge_record.src_,
                        temp_currentTime);
            bool rt = writer.Write(edge_record);
            assert(rt == true); // 这次写必须成功！
          }
        }
      }
#endif
      if(!writer.Write(edge_record)) { // if output file full
        temp_currentTime = __sync_fetch_and_add(&currentTime, 1);
        writer.Init(eFileName(temp_currentTime),
                    pFileName(temp_currentTime),
                    edge_record.src_,
                    temp_currentTime);
        bool rt = writer.Write(edge_record);
        assert(rt == true); // 这次写必须成功！
      }
      auto new_edge_record_opt = ways[min_way_index].NextEdgeRecord();
      if(new_edge_record_opt == std::nullopt) {
        ways[min_way_index].FreeBuffer();
        work_way--;
        if (work_way <= 0) {
          break;
        }
        level_way_ptr += (min_edge_index == 0);
        next_level_way_ptr += (min_edge_index == 1);
        if (min_edge_index == 0 && level_way_ptr < inputs_0_size) {
          ways[level_way_ptr].Init();
          edge_cache[0] = ways[level_way_ptr].NextEdgeRecord().value();
        } else if (min_edge_index == 1 && next_level_way_ptr < way_num) {
          assert(inputs_1_size > 0);
          ways[next_level_way_ptr].Init();
          edge_cache[1] = ways[next_level_way_ptr].NextEdgeRecord().value();
        }
      } else {
        edge_cache[min_edge_index] = new_edge_record_opt.value();
      }

      // find min edge
      if (level_way_ptr == inputs_0_size) {
        min_edge_index = 1;
        min_way_index = next_level_way_ptr;
        assert(inputs_1_size > 0);
      } else if (next_level_way_ptr == way_num) {
        min_edge_index = 0;
        min_way_index = level_way_ptr;
      } else {
        assert(inputs_1_size > 0);
        min_edge_index = 0;
        min_way_index = level_way_ptr;
        if (edge_cache[1] < edge_cache[0]) {
          min_edge_index = 1;
          min_way_index = next_level_way_ptr;
        }
      }
    }
  }
#endif

    if (!writer.IsClear()) {
      writer.WriteEnd();
    }
    edge_cache.clear();
  
}

// The goal is to find some boundary keys so that we can evenly partition
// the compaction input data into max_subcompactions ranges.
// For every input file, we ask TableReader to estimate 128 anchor points
// that evenly partition the input file into 128 ranges and the range
// sizes. This can be calculated by scanning index blocks of the file.
// Once we have the anchor points for all the input files, we merge them
// together and try to find keys dividing ranges evenly.
// For example, if we have two input files, and each returns following
// ranges:
//   File1: (a1, 1000), (b1, 1200), (c1, 1100)
//   File2: (a2, 1100), (b2, 1000), (c2, 1000)
// We total sort the keys to following:
//  (a1, 1000), (a2, 1100), (b1, 1200), (b2, 1000), (c1, 1100), (c2, 1000)
// We calculate the total size by adding up all ranges' size, which is 6400.
// If we would like to partition into 2 subcompactions, the target of the
// range size is 3200. Based on the size, we take "b1" as the partition key
// since the first three ranges would hit 3200.
// Note that the ranges are actually overlapping. For example, in the example
// above, the range ending with "b1" is overlapping with the range ending with
// "b2". So the size 1000+1100+1200 is an underestimation of data size up to
// "b1". In extreme cases where we only compact N L0 files, a range can
// overlap with N-1 other ranges. Since we requested a relatively large number
// (128) of ranges from each input files, even N range overlapping would
// cause relatively small inaccuracy.
// ref: rocksdb/db/compaction/compaction_job.cc
void Compaction::GenSubcompactionBoundaries(
                                        std::vector<WayAnchor>& all_wayAnchors,
                                        std::vector<VertexId_t>& boundaries) {
  //  例如：对于文件s1内容如下：1, 2, 3, 4, 5, 6, 7
  //    对每个文件进行分割: all_anchors = {(3,2), (5,2), (7,1)}
  //    所有文件都遵循此分割点: boundaries = {3, 5, 7}
  //    根据boundaries将s1分割为四段：[1,2], [3,4], [5,6], [7].

  std::vector<Anchor> all_anchors;
  uint64_t total_size = 0;
  int segment_num = 16;
  // level=0
  for (auto it : inputs_[0]) {

    size_t edge_num = it->header.size;
    total_size += edge_num;
    size_t index_num = it->header.index_size;
    size_t edge_num_per_anchor = edge_num / segment_num;
    size_t range_size = 0;
    std::vector<Index>& indexes = it->indexes;
    EdgeOffset_t offset = indexes[0].offset;
    VertexId_t src = indexes[0].key;
    for (int j = 0; j < index_num; j++) {
      EdgeOffset_t now_offset = indexes[j].offset;
      assert(now_offset >= offset);
      range_size =  (now_offset - offset) / EDGEBODY_SIZE;
      if (range_size > edge_num_per_anchor) {
        all_anchors.emplace_back(src, range_size);
        range_size = 0;
        offset = now_offset;
        src = indexes[j].key;
      }
    }
    if (range_size > 0) {
      all_anchors.emplace_back(src, range_size);
    }
  }

  // level=1
  for (auto it : inputs_[1]) {
    size_t edge_num = it->header.size;
    total_size += edge_num;
    size_t index_num = it->header.index_size;
    size_t edge_num_per_anchor = edge_num / segment_num;
    size_t range_size = 0;
    char* indexBuf = it->GetIndex();
    EdgeOffset_t offset = *(EdgeOffset_t*)(indexBuf + 8);
    VertexId_t src = *(VertexId_t*)(indexBuf);
    for (int j = 0; j < index_num; j++) {
      EdgeOffset_t now_offset = *(EdgeOffset_t*)(indexBuf + 12*j + 8);
      range_size =  (now_offset - offset) / EDGEBODY_SIZE;
      if (range_size > edge_num_per_anchor) {
        all_anchors.emplace_back(src, range_size);
        range_size = 0;
        offset = now_offset;
        src = *(VertexId_t*)(indexBuf + 12*j);
      }
    }
    if (range_size > 0) {
      all_anchors.emplace_back(src, range_size);
    }
    delete[] indexBuf;
    indexBuf = nullptr;
  }

  std::sort(
      all_anchors.begin(), all_anchors.end(),
      [](const Anchor& a, const Anchor& b){
        return a.begin < b.begin;
      });

  // Remove duplicated entries from boundaries.
  all_anchors.erase(
      std::unique(all_anchors.begin(), all_anchors.end(),
                  [](Anchor& a, Anchor& b) {
                    return a.begin == b.begin;
                  }),
      all_anchors.end());

  uint64_t num_planned_subcompactions = max_subcompactions_;

  assert(num_planned_subcompactions > 1);

  // Group the ranges into subcompactions
  size_t max_edge_num_per_file = FLAGS_sstable_size;// 64;
  uint64_t target_range_size = std::max(
      total_size / num_planned_subcompactions, max_edge_num_per_file);

  assert(target_range_size < total_size); // 需要保证level-0的sst > level-1的sst

  uint64_t next_threshold = target_range_size;
  uint64_t cumulative_size = 0;
  uint64_t num_actual_subcompactions = 1U;
  for (Anchor& anchor : all_anchors) {
    cumulative_size += anchor.range_size;
    if (cumulative_size > next_threshold) {
      next_threshold += target_range_size;
      num_actual_subcompactions++;
      boundaries.push_back(anchor.begin);
    }
    if (num_actual_subcompactions == num_planned_subcompactions) {
      break;
    }
  }

  // 获取每个文件中对应的anchor
  // level=0
  for (auto it : inputs_[0]) {
    if (it->header.index_size <= 1) {
      continue;
    }
    size_t range_size = 0;
    EdgeOffset_t offset = it->indexes[0].offset;
    VertexId_t src = it->indexes[0].key;
    int j = 0;
    int last_index_id = 0;
    for (auto bd : boundaries) {
      j = it->low_bound(bd, 0, it->indexes.size()-1);
      if (j == 0 || bd < src || j == last_index_id) {
        continue;
      }
      VertexId_t now_key = it->indexes[j].key;
      EdgeOffset_t now_offset = it->indexes[j].offset;
      range_size = now_offset - offset; // / EDGEBODY_SIZE;
      // [begin, end]
      assert(j >= 1);
      VertexId_t end_key = it->indexes[j-1].key;
      all_wayAnchors.emplace_back(src,
                              end_key,
                              offset,
                              last_index_id,
                              j - last_index_id,
                              range_size,
                              it->path,
                              it->header.timeStamp,
                              true,
                              &it->indexes);
      assert(range_size > 0);
      offset = now_offset;
      src = now_key;
      last_index_id = j;
      if (j == it->header.index_size-1) {
        break;
      }
    }
    // 处理sstable只有一个元素和剩余的最后一部分
    if (j < it->header.index_size - 1){// || last_index_id == 0) {
      j = it->header.index_size - 1;
      range_size = (it->indexes[j].offset - offset); // / EDGEBODY_SIZE;
      assert(j >= 1);
      VertexId_t end_key = it->indexes[j-1].key;
      all_wayAnchors.emplace_back(src,
                              end_key,
                              offset,
                              last_index_id,
                              j - last_index_id,
                              range_size,
                              it->path,
                              it->header.timeStamp,
                              true,
                            &it->indexes);
      assert(range_size > 0);
    }
  }
  // level=1
  for (auto it : inputs_[1]) {
    if (it->header.index_size <= 1) {
      continue;
    }
    size_t range_size = 0;
    char* indexBuf = it->GetIndex();
    VertexId_t src = *(VertexId_t*)(indexBuf);
    EdgeOffset_t offset = *(EdgeOffset_t*)(indexBuf + 8);
    int j = 0;
    int last_index_id = 0;
    for (auto bd : boundaries) {
      j = it->low_bound(bd, 0, it->header.index_size-1, indexBuf);
      if (j == 0 || bd < src || j == last_index_id) {
        continue;
      }
      VertexId_t now_key = *(VertexId_t*)(indexBuf + 12*j);
      EdgeOffset_t now_offset = *(EdgeOffset_t*)(indexBuf + 12*j + 8);
      range_size = (now_offset - offset); // / EDGEBODY_SIZE;
      assert(j >= 1);
      VertexId_t end_key = *(VertexId_t*)(indexBuf + 12*(j-1));
      all_wayAnchors.emplace_back(src,
                              end_key,
                              offset,
                              last_index_id,
                              j - last_index_id,
                              range_size,
                              it->path,
                              it->header.timeStamp,
                              false,
                            &it->indexes);
      assert(range_size > 0);
      offset = now_offset;
      src = now_key;
      last_index_id = j;
      if (j == it->header.index_size-1) {
        break;
      }
    }
    if (j < it->header.index_size - 1){// || last_index_id == 0) {
      j = it->header.index_size - 1;
      VertexId_t now_key = *(VertexId_t*)(indexBuf + 12*j);
      EdgeOffset_t now_offset = *(EdgeOffset_t*)(indexBuf + 12*j + 8);
      range_size = (now_offset - offset); // / EDGEBODY_SIZE;
      assert(j >= 1);
      VertexId_t end_key = *(VertexId_t*)(indexBuf + 12*(j-1));
      all_wayAnchors.emplace_back(src,
                              end_key,
                              offset,
                              last_index_id,
                              j - last_index_id,
                              range_size,
                              it->path,
                              it->header.timeStamp,
                              false,
                            &it->indexes);
      assert(range_size > 0);
    }
    delete[] indexBuf;
    indexBuf = nullptr;
  }

  std::sort(
      all_wayAnchors.begin(), all_wayAnchors.end(),
      [](const WayAnchor& a, const WayAnchor& b){
        return a.begin < b.begin;
      });

}

// Coalesce ranges from the same file before assigning work.
// Performs a thread-safe multi-way merge over the supplied ranges.
void Compaction::ProcessMultiWaysCompaction(std::vector<Way>& ways,
                                           const int level) {
  int way_num = ways.size();
  assert(way_num > 0);
  int level_way_ptr = 0;
  std::vector<bool> way_status(way_num, true);
  int work_way = way_num;
  std::vector<EdgeRecord> edge_cache;
  edge_cache.reserve(way_num); // 注意：这里多线程调用，需要单独构造

  for (int i = 0; i < way_num; i++) {
    ways[i].Init();
    edge_cache.emplace_back(ways[i].NextEdgeRecord().value());
  }

  // find min edge
  int min_way_index = level_way_ptr;
  int ptr = level_way_ptr + 1;
  while (ptr < way_num) {
    if (edge_cache[ptr] < edge_cache[min_way_index]) {
      min_way_index = ptr;
    }
    ptr++;
  }

  uint64_t temp_currentTime = __sync_fetch_and_add(&currentTime, 1);
  auto writer = SSTableWriter(fileMetaCache_, vid_to_levelIndex,
                              vid_to_mullevelIndex_,
                              vertex_futexes, vertex_rwlocks_,
                              vertex_max_level_,
                              vertex_lock_count_,
                              min_level_0_fid_,
                              level, buffer_manager,
                              tablecache_mutex_,
                              version_edit_,
                              sstdata_manager_,
                              sst_is_vaild_to_ins_lf_,
                              lf_mutex_);
  writer.Init(eFileName(temp_currentTime),
              pFileName(temp_currentTime),
              edge_cache[min_way_index].src_,
              temp_currentTime);

  for (int i = 0; i < ways.size(); i++) {
    if (ways[i].Is_input_0()) {
      writer.InsertInput0Fid(ways[i].GetFildId());
    }
  }

#ifdef DEL_EDGE_SEPARATE
  std::vector<bool> way_have_del_edge(way_num, false);
  std::vector<EidToTimeMap*> way_map(way_num, nullptr);
  for (int i = 0; i < ways.size(); i++) {
    EidToTimeMap* deleted_edge_map;
    bool have_del_edge = del_record_manager_.find_eidmap(ways[i].GetFildId(),
                                                         deleted_edge_map);
    if (have_del_edge == true) {
      way_have_del_edge[i] = true;
      way_map[i] = deleted_edge_map;
    } else {
    }
  }
#endif

  while(work_way > 0) {
    auto& edge_record = edge_cache[min_way_index];

#ifdef DEL_EDGE_SEPARATE
    // Merge the deleted records of this edge, if they exist.
    if (way_have_del_edge[min_way_index] == true) {
      SequenceNumber_t eid = edge_record.seq_;
      SequenceNumber_t del_time;
      if (del_record_manager_.get_time(way_map[min_way_index], eid, del_time)) {

        // TODO(correctness): Keep deletion metadata visible until the output
        // SST is published. Removing it here can make concurrent readers miss
        // the record; transfer it before retiring the old map entry.
        del_record_manager_.del_eid_frome_eidmap(way_map[min_way_index], eid);

#ifndef REAL_DELETE
        // write edge to output file
        // 这里的本意是，在原始插入的边前面插入一条删除边的记录，这样就和RocksDB追加删除边的
        // 操作类似，但是其实可以选择: 1.直接清理调这条边(即真正删除); 2.给原始边edge_record
        // 标记位置位1就行了，也可以保证读取时不会读取它。
        // 下面选择: 2
        edge_record.marker_ = true;
#endif
      }
    }
#endif

#ifndef REAL_DELETE
    // write edge to output file
    if(!writer.Write(edge_record)) { // if output file full
#else
    if(!way_have_del_edge[min_way_index] && !writer.Write(edge_record)) { // if output file full
#endif
        temp_currentTime = __sync_fetch_and_add(&currentTime, 1);
        writer.Init(eFileName(temp_currentTime),
                    pFileName(temp_currentTime),
                    edge_record.src_,
                    temp_currentTime);
        bool rt = writer.Write(edge_record);
        assert(rt == true); // 这次写必须成功！
    }

    auto new_edge_record_opt = ways[min_way_index].NextEdgeRecord();
    if(new_edge_record_opt == std::nullopt) {
      way_status[min_way_index] = false;
      ways[min_way_index].FreeBuffer();
      work_way--;
      if (work_way <= 0) {
        break;
      }
    } else {
      edge_cache[min_way_index] = new_edge_record_opt.value();
    }

    // find min edge
    int ptr = level_way_ptr;
    bool is_first = true;
    while (ptr < way_num) {
      if (is_first && way_status[ptr]) {
        min_way_index = ptr;
        ptr++;
        is_first = false;
        continue;
      }
      if (way_status[ptr] && edge_cache[ptr] < edge_cache[min_way_index]) {
        min_way_index = ptr;
      }
      ptr++;
    }
    assert(way_status[min_way_index] == true);
  }

  if (!writer.IsClear()) {
    writer.WriteEnd();
  }
  edge_cache.clear();
}

// Performs a thread-safe multi-way merge over the supplied ranges.
/// 将有序的文件进行整理到同一个way中，减少归并路数
/// 当针对level-0与level-1合并时，理论上最大路数为: memtable_num + 1 路.
void Compaction::ProcessMultiWaysCompactionOpt(const int level,
                                      std::vector<WayAnchor>& all_wayAnchors,
                                      int anchor_begin,
                                      int anchor_end) {
  assert(level == 0); // If level >= 1, it is better to use two-way compaction
  assert(all_wayAnchors.size() > 0);
  assert(anchor_begin < anchor_end);
  std::vector<VertexId_t> end_per_way;
  std::vector<std::vector<Way>> mulways;
  // WayAnchor: [begin, end], 是全闭区间
  for (int i = anchor_begin; i < anchor_end; i++) {
    auto& anchor = all_wayAnchors[i];
    bool found = false;
    for (int way_id = 0; way_id < end_per_way.size(); way_id++) {
      if (anchor.begin > end_per_way[way_id]) {
        end_per_way[way_id] = anchor.end;
        mulways[way_id].emplace_back(buffer_manager,
                                      anchor.index_num + 1, // +哨兵
                                      anchor.index_start_offset,
                                      anchor.range_size/EDGEBODY_SIZE+1, // +哨兵
                                      anchor.path,
                                      anchor.fid,
                                      anchor.is_input_0,
                                      anchor.index_ptr);
        found = true;
        break;
      }
    }
    if (found == false) {
        end_per_way.push_back(anchor.end);
        mulways.push_back(std::vector<Way>());
        mulways[mulways.size()-1].emplace_back(buffer_manager,
                                        anchor.index_num + 1, // +哨兵
                                        anchor.index_start_offset,
                                        anchor.range_size/EDGEBODY_SIZE+1, // +哨兵
                                        anchor.path,
                                        anchor.fid,
                                        anchor.is_input_0,
                                        anchor.index_ptr);
    }
  }
  int way_num = mulways.size();
  assert(way_num > 0);
  assert(way_num <= LevelMaxSize(level) + 1);
  int level_way_ptr = 0;
  std::vector<bool> way_status(way_num, true);
  std::vector<int> way_ptr(way_num, 0);
  int work_way = way_num;
  std::vector<EdgeRecord> edge_cache;
  edge_cache.reserve(way_num); // 注意：这里多线程调用，需要单独构造
  for (int i = 0; i < way_num; i++) {
    mulways[i][way_ptr[i]].Init();
    edge_cache.emplace_back(mulways[i][way_ptr[i]].NextEdgeRecord().value());
  }
  // find min edge
  int min_way_index = level_way_ptr;
  int ptr = level_way_ptr + 1;
  while (ptr < way_num) {
    if (edge_cache[ptr] < edge_cache[min_way_index]) {
      min_way_index = ptr;
    }
    ptr++;
  }
  uint64_t temp_currentTime = __sync_fetch_and_add(&currentTime, 1);
  auto writer = SSTableWriter(fileMetaCache_, vid_to_levelIndex,
                              vid_to_mullevelIndex_,
                              vertex_futexes, vertex_rwlocks_,
                              vertex_max_level_,
                              vertex_lock_count_,
                              min_level_0_fid_,
                              level, buffer_manager,
                              tablecache_mutex_,
                              version_edit_,
                              sstdata_manager_,
                              sst_is_vaild_to_ins_lf_,
                              lf_mutex_);
  writer.Init(eFileName(temp_currentTime),
              pFileName(temp_currentTime),
              edge_cache[min_way_index].src_,
              temp_currentTime);
  std::vector<std::vector<int>> level_1_index(mulways.size());

  for (int i = 0; i < mulways.size(); i++) {
    for (int j = 0; j < mulways[i].size(); ++j){
      if (mulways[i][j].Is_input_0()) {
        writer.InsertInput0Fid(mulways[i][j].GetFildId());
      } else {
        level_1_index[i].emplace_back(j);
      }
    }
  }

#ifdef DEL_EDGE_SEPARATE
  std::vector<bool> way_have_del_edge(way_num, false);
  std::vector<EidToTimeMap*> way_map(way_num, nullptr);
  for (int i = 0; i < ways.size(); i++) {
    EidToTimeMap* deleted_edge_map;
    bool have_del_edge = del_record_manager_.find_eidmap(ways[i].GetFildId(),
                                                         deleted_edge_map);
    if (have_del_edge == true) {
      way_have_del_edge[i] = true;
      way_map[i] = deleted_edge_map;
    } else {
    }
  }
#endif
  while(work_way > 0) {
    uint32_t cur_vertex = edge_cache[min_way_index].src_;
    while (work_way > 0 && cur_vertex == edge_cache[min_way_index].src_ ) {
      auto& edge_record = edge_cache[min_way_index];




      #ifdef MERGE_LARGE_VERTEX
      if (!mulways[min_way_index][way_ptr[min_way_index]].Is_input_0()){
        MulLevelIndex& findex = MutableMulLevelIndex(edge_cache[min_way_index].src_);
        int fileID = findex.get_fileID(0);
        if (fileID == INVALID_File_ID) {
          while (edge_cache[min_way_index].src_ == cur_vertex) {
#ifdef MERGE_LARGE_VERTEX_DEBUG
            std::cout << "level 1 skip: "
                      << " " << edge_cache[min_way_index].src_
                      << " " << edge_cache[min_way_index].dst_
                      << " " << edge_cache[min_way_index].seq_
                      << ", " << min_way_index << std::endl;
#endif


            auto new_edge_record_opt = mulways[min_way_index][way_ptr[min_way_index]].NextEdgeRecord();
            if(new_edge_record_opt == std::nullopt) {
              mulways[min_way_index][way_ptr[min_way_index]].FreeBuffer();
              way_ptr[min_way_index]++;
              if (way_ptr[min_way_index] == mulways[min_way_index].size()) {
                way_status[min_way_index] = false;
                work_way--;
                if (work_way <= 0) {
                  break;
                }
              } else {
                mulways[min_way_index][way_ptr[min_way_index]].Init();
                edge_cache[min_way_index] =
                        mulways[min_way_index][way_ptr[min_way_index]].NextEdgeRecord()
                                .value();
              }
              break;
            } else {
              edge_cache[min_way_index] = new_edge_record_opt.value();
            }
          }
          if (work_way <= 0){
            break;
          }
          int ptr = level_way_ptr;
          bool is_first = true;
          while (ptr < way_num) {
            if (is_first && way_status[ptr]) {
              min_way_index = ptr;
              ptr++;
              is_first = false;
              continue;
            }
            if (way_status[ptr] && edge_cache[ptr] < edge_cache[min_way_index]) {
              min_way_index = ptr;
            }
            ptr++;
          }
        }
      }
      #endif

#ifdef DEL_EDGE_SEPARATE
      // Merge the deleted records of this edge, if they exist.
      if (way_have_del_edge[min_way_index] == true) {
        SequenceNumber_t eid = edge_record.seq_;
        SequenceNumber_t del_time;
        if (del_record_manager_.get_time(way_map[min_way_index], eid, del_time)) {

          // TODO(correctness): Keep deletion metadata visible until the output
          // SST is published. Removing it here can make concurrent readers miss
          // the record; transfer it before retiring the old map entry.
          del_record_manager_.del_eid_frome_eidmap(way_map[min_way_index], eid);

#ifndef REAL_DELETE
          // write edge to output file
          // 这里的本意是，在原始插入的边前面插入一条删除边的记录，这样就和RocksDB追加删除边的
          // 操作类似，但是其实可以选择: 1.直接清理调这条边(即真正删除); 2.给原始边edge_record
          // 标记位置位1就行了，也可以保证读取时不会读取它。
          // 下面选择: 2
          edge_record.marker_ = true;
#endif
        }
      }
#endif
      // write edge to output file
#ifndef REAL_DELETE
      if(!writer.Write(edge_record)) { // if output file full
#else
        // write this edge unless this edge has been deleted
    if(!way_have_del_edge[min_way_index] && !writer.Write(edge_record)) { // if output file full
#endif
        temp_currentTime = __sync_fetch_and_add(&currentTime, 1);
        writer.Init(eFileName(temp_currentTime),
                    pFileName(temp_currentTime),
                    edge_record.src_,
                    temp_currentTime);
        bool rt = writer.Write(edge_record);
        assert(rt == true); // 这次写必须成功！
    #ifndef REAL_DELETE
    }
    #else
      }
    #endif
      int curr_ptr = way_ptr[min_way_index];
      auto new_edge_record_opt = mulways[min_way_index][curr_ptr].NextEdgeRecord();
      if(new_edge_record_opt == std::nullopt) {
        mulways[min_way_index][curr_ptr].FreeBuffer();
        way_ptr[min_way_index]++;
        if (way_ptr[min_way_index] == mulways[min_way_index].size()) {
          way_status[min_way_index] = false;
          work_way--;
          if (work_way <= 0) {
            break;
          }
        } else {
          mulways[min_way_index][way_ptr[min_way_index]].Init();
          edge_cache[min_way_index] =
                  mulways[min_way_index][way_ptr[min_way_index]].NextEdgeRecord()
                          .value();
        }
      } else {
        edge_cache[min_way_index] = new_edge_record_opt.value();
      }
      // find min edge
      int ptr = level_way_ptr;
      bool is_first = true;
      while (ptr < way_num) {
        if (is_first && way_status[ptr]) {
          min_way_index = ptr;
          ptr++;
          is_first = false;
          continue;
        }
        if (way_status[ptr] && edge_cache[ptr] < edge_cache[min_way_index]) {
          min_way_index = ptr;
        }
        ptr++;
      }
      assert(way_status[min_way_index] == true);
        }
  }
  if (!writer.IsClear()) {
    writer.WriteEnd();
  }
  assert(edge_cache.size() == way_num);
  edge_cache.clear();
}

// TODO(correctness): Assigning adjacent sorted pieces to different workers can
// still produce overlapping output key ranges. For example, splitting
// input0=[1,10] into [1,5]/[6,10] and input1=[1,20] into [1,7]/[8,20]
// produces worker ranges [1,7] and [6,20]. Partition by non-overlapping key
// boundaries before enabling this subcompaction path.
void Compaction::DoSubCompactionWork(const int level) {
  assert(level == 0);

  std::vector<WayAnchor> all_wayAnchors;
  std::vector<VertexId_t> boundaries;  // the boundaries for each subcompaction
  GenSubcompactionBoundaries(all_wayAnchors, boundaries);

  if (FLAGS_support_mulversion == false) {
    RemoveCache(level, 0, true);
    RemoveCache(level + 1, 1, true);
    RemoveFile();
  } else {
    RemoveCache(level, 0, false);
    RemoveCache(level + 1, 1, false);
  }
  const size_t num_threads = boundaries.size();
  assert(num_threads > 0);
  assert(num_threads <= max_subcompactions_);

  // Launch a thread for each of subcompactions 1...num_threads-1
  std::vector<std::thread> thread_pool;
  thread_pool.reserve(num_threads);
  const auto* property_layout_override =
      GetCurrentFixedPropertyLayoutOverride();
  const auto* db_path_override = GetCurrentDbPathOverride();

  int cnt = 0;
  int anchor_begin = 0;
  int anchor_end = 0;

  // Each worker receives a half-open range of anchors: [begin, end).
  for (VertexId_t bd : boundaries) {
    anchor_begin = cnt;
    for (int i = cnt; i < all_wayAnchors.size(); i++, cnt++) {
      WayAnchor& anchor = all_wayAnchors[i];
      anchor_end = i;
      if (anchor.begin >= bd) {
        break;
      }
    }
    if (anchor_begin != anchor_end) {
      thread_pool.emplace_back([this, level, &all_wayAnchors,
                                anchor_begin, anchor_end,
                                property_layout_override,
                                db_path_override]() {
        const ScopedFixedPropertyLayout property_layout(
            property_layout_override);
        const ScopedDbPathOverride db_path_scope(db_path_override);
        ProcessMultiWaysCompactionOpt(level, all_wayAnchors,
                                      anchor_begin, anchor_end);
      });
    }
  }
  // Process the final range on the calling thread.
  if (cnt != all_wayAnchors.size()) {
    ProcessMultiWaysCompactionOpt(level, all_wayAnchors, cnt,
                                  all_wayAnchors.size());
  }
  // Wait for all other threads (if there are any) to finish execution
  for (auto& thread : thread_pool) {
    thread.join();
  }
}

// Level 0 requires a multi-way merge because key ranges can overlap. Higher
// levels are ordered and use a two-way merge.
void Compaction::ProcessCompactionWork(const int level,
                                       const int inputs_0_size,
                                       const int inputs_1_size,
                                       std::vector<EdgeRecord>& edge_cache,
                                       std::vector<Way>& ways) {

  if (level == 0) {
    // Level 0 can contain overlapping key ranges, so it needs a full
    // multi-way merge even when max_subcompactions is one. The old path
    // returned without producing output after removing its inputs.
    ProcessMultiWaysCompaction(ways, level);
    return;
  }
  if (level >= 1) {
    int max_level = fileMetaCache_.size() - 1;
    while (max_level > 0) {
      if (!fileMetaCache_[max_level]->empty()){
        break;
      }
      --max_level;
    }
    #ifdef MERGE_LARGE_VERTEX
    if (level > 1 && level == max_level - 1) {
      BottomCompactionWork(level, inputs_0_size, inputs_1_size, edge_cache, ways);
      return;
    }
    #endif
    DoCompactionWorkTwoWay(level, inputs_0_size,
                           inputs_1_size, edge_cache, ways);
    return;
  }
}
// Move an SST's level-index entries from level to level + 1.
void Compaction::UpdataLevelIndex(SSTableCache* it, const int level) {
  std::ifstream file(it->path, std::ios::binary);
  if (!file) {
      printf("Fail to open file %s\n", it->path.c_str());
      exit(-1);
  }
  int64_t index_length = it->header.index_size;
  char *indexBuf = new char[index_length * 12];
  file.seekg(it->header.size * EDGEBODY_SIZE, std::ios::beg);

  file.read(indexBuf, index_length * 12);
  int j = 0;
  assert(level > 0);
  for(int32_t i = 0; i < index_length - 1; ++i) {
    VertexId_t key = *(uint64_t*)(indexBuf + 12*i);
    write_max(&vertex_max_level_[key], Level_t(level+2));
    if (FLAGS_support_mulversion== false) {
      int index_id = key * LEVEL_INDEX_SIZE + level - 1;
      // TODO(correctness): Protect legacy level-index relocation from
      // concurrent readers.
      memcpy(&vid_to_levelIndex[index_id+1], &vid_to_levelIndex[index_id],
             sizeof(LevelIndex));
      vid_to_levelIndex[index_id].set_fileID(INVALID_File_ID);
    } else {
  #ifdef MMAP_DIFF_SIZE_LEVEL_INDEX
      // 本质就是修改当前索引中记录的level_id
      vertex_rwlocks_[key].WriteLock();
      char* ptr = vid_to_mullevelIndex_->get_level_index_ptr_by_vid(key);
      int level_num = vid_to_mullevelIndex_->get_level_num_by_vid(key);
      if (level_num > 0) {
        MulLevelIndexWithDiffSize level_index
            = MulLevelIndexWithDiffSize(ptr, level_num);
        for (int i = 0; i < level_num; ++i) {
          if (level_index.get_level_id(i) == level) {
            level_index.set_level_id(i, level+1); // 将level层改成level+1层
            break;
          }
        }
      }
      vertex_rwlocks_[key].WriteUnlock();
  #elif defined(MMAP_COLD_HOT_LEVEL_INDEX)
      VertexId_t array_id= 0;
      int old_level_num = 0;
      char* ptr = vid_to_mullevelIndex_->
          get_level_index_ptr_by_vid(key, old_level_num, array_id);
      if (ptr != nullptr) {
        MulLevelIndexWithDiffSize level_index 
            = MulLevelIndexWithDiffSize(ptr, old_level_num);
        for (int i = 0; i < old_level_num; ++i) {
          if (level_index.get_level_id(i) == level) {
            level_index.set_level_id(i, level+1); // 将level层改成level+1层
            break;
          }
        }
      }
  #else
      VertexRWLock(key).WriteLock();
      MulLevelIndex& findex = MutableMulLevelIndex(key);
  #ifdef MERGE_LARGE_VERTEX_DEBUG
      if (key == 97) {
       printf("adjust %d to %d\n", level, level + 1 );
       for (int i = 0; i < 4; ++i){
         printf("fid: %d, offset: %d, next offset: %d\n",
                findex.get_fileID(i),
                findex.get_offset(i),
                findex.get_next_offset(i));
       }
      }
  #endif
      findex.set_fileID(level, findex.get_fileID(level-1));
      findex.set_offset(level, findex.get_offset(level-1));
      findex.set_next_offset(level, findex.get_next_offset(level-1));
      findex.set_fileID(level-1, INVALID_File_ID);
      VertexRWLock(key).WriteUnlock();
  #endif
    }
  }

  delete[] indexBuf;
  file.close();
}

/// 如果inputs_[1].size==0且level>0时，则不需要合并，直接将inputs_[0]内的文件直接调整为
/// level+1层文件即可, 注意需要更新level_index.
void Compaction::DirectAdjustLevel(const int level) {
  assert(level >= 1);
  assert(level + 1 < fileMetaCache_.size());
  // add inputs[0] to level+1
  for (auto it : inputs_[0]) {
    fileMetaCache_[level+1]->emplace_back(it);
    // build file levelindex
    UpdataLevelIndex(it, level);
  }
  std::sort(fileMetaCache_[level+1]->begin(), fileMetaCache_[level+1]->end(),
    [](const SSTableCache *a, const SSTableCache *b){
      return (a->header).minKey < (b->header).minKey; // Sort minkey from small to large
  });
  // clear inputs[0] from level
  RemoveCache(level, 0, false);
}


void Compaction::PreCompaction(const int level, std::vector<Way>& ways) {
  for (auto it : inputs_[0]) {
    ways.emplace_back(it, buffer_manager, true);
  }
  for (auto it : inputs_[1]) {
    ways.emplace_back(it, buffer_manager, false);
  }

  if (FLAGS_support_mulversion == false) {
    RemoveCache(level, 0, true);
    RemoveCache(level + 1, 1, true);
    RemoveFile();
  } else {
    // Versioned readers retain file lifetime through metadata references.
    RemoveCache(level, 0, false);
    RemoveCache(level + 1, 1, false);
  }

}

/// 进行普通的压缩，将inputs_[0]和inputs_[1]内容文件并到level层
void Compaction::DoGeneralCompactionWork(const int level) {
  int reseve_size = inputs_[0].size() + inputs_[1].size();
  assert(reseve_size > 0);
  if(cache_max_size < reseve_size) {
      cache_max_size = reseve_size;
      ways.reserve(cache_max_size);
  }

  int inputs_0_num = inputs_[0].size();
  int inputs_1_num = inputs_[1].size();

  PreCompaction(level, ways);

  // Level 0 uses a single multi-way merge because its key ranges overlap.
  if ((!(max_subcompactions_ > 1
       && (fileMetaCache_[level]->size() > LevelMaxSize(level)))
      || level == 0)) {
    ProcessCompactionWork(level,
                          inputs_0_num,
                          inputs_1_num,
                          edge_cache,
                          ways);
  } else {
    // Higher-level input ranges must remain stable while workers partition
    // them; refreshing metadata here could create overlapping worker ranges.
    assert(level > 0);
    // Launch a thread for each of subcompactions 1...num_threads-1
    std::vector<std::thread> thread_pool;
    thread_pool.reserve(max_subcompactions_);
    const auto* property_layout_override =
        GetCurrentFixedPropertyLayoutOverride();
    const auto* db_path_override = GetCurrentDbPathOverride();

    VertexId_t all_start, all_limit;
    GetRange2(inputs_[0], inputs_[1], all_start, all_limit);
    VertexId_t first_compaction_min_key = all_start;

    // Partition this level using the next level's key boundaries.
    std::vector<std::vector<Way>> sub_ways;
    std::vector<std::vector<EdgeRecord>> sub_edge_caches;
    std::vector<int> inputs_0_num_vec;
    std::vector<int> inputs_1_num_vec;
    sub_ways.reserve(max_subcompactions_);
    sub_edge_caches.reserve(max_subcompactions_);
    inputs_0_num_vec.reserve(max_subcompactions_);
    inputs_1_num_vec.reserve(max_subcompactions_);
    int thread_id = 0;

    thread_pool.emplace_back([this, level, inputs_0_num, inputs_1_num,
                              property_layout_override,
                              db_path_override]() {
      const ScopedFixedPropertyLayout property_layout(
          property_layout_override);
      const ScopedDbPathOverride db_path_scope(db_path_override);
      ProcessCompactionWork(level, inputs_0_num, inputs_1_num,
                            edge_cache, ways);
    });

    // 如果当前level文件的数量依然超过了阈值，则需要继续压缩
    while (fileMetaCache_[level]->size() > LevelMaxSize(level)
           && thread_id < max_subcompactions_) {
      compact_pointer_[level] = last_merge_max_key_;

      inputs_[0].clear();
      inputs_[1].clear();
      merged_fids_[0].clear();
      merged_fids_[1].clear();
      bool is_reverse = false;

      {
        // Keep source selection atomic with concurrent publication into the
        // next level.
        std::lock_guard<std::mutex> lock(*tablecache_mutex_[level]);
        std::lock_guard<std::mutex> lock2(*tablecache_mutex_[level+1]);
        is_reverse = PickCompactioFile(level, false);
      }

      {
        VertexId_t all_start, all_limit;
        GetRange2(inputs_[0], inputs_[1], all_start, all_limit);
        if (is_reverse && all_limit < first_compaction_min_key) {
          std::cout << "skip the result." << std::endl;
          break;
        }
      }
      sub_ways.emplace_back(std::vector<Way>());
      sub_edge_caches.emplace_back(std::vector<EdgeRecord>());

      for (auto it : inputs_[0]) {
        merged_fids_[0].insert(it->header.timeStamp);
        if (FLAGS_support_mulversion == true) {
          delete_filemeta_set_.push_back(it);
        }
      }
      for (auto it : inputs_[1]) {
        merged_fids_[1].insert(it->header.timeStamp);
        if (FLAGS_support_mulversion == true) {
          delete_filemeta_set_.push_back(it);
        }
      }

      int inputs_0_num = inputs_[0].size();
      int inputs_1_num = inputs_[1].size();

      PreCompaction(level, sub_ways[thread_id]);

      inputs_0_num_vec.emplace_back(inputs_0_num);
      inputs_1_num_vec.emplace_back(inputs_1_num);

      thread_id++;
    }

    for (int t_id = 0; t_id < thread_id; t_id++) {
      thread_pool.emplace_back([this, level, t_id, &inputs_0_num_vec,
                                &inputs_1_num_vec, &sub_edge_caches,
                                &sub_ways, property_layout_override,
                                db_path_override]() {
        const ScopedFixedPropertyLayout property_layout(
            property_layout_override);
        const ScopedDbPathOverride db_path_scope(db_path_override);
        ProcessCompactionWork(level,
                              inputs_0_num_vec[t_id],
                              inputs_1_num_vec[t_id],
                              sub_edge_caches[t_id],
                              sub_ways[t_id]);
      });
    }

    // Wait for all other threads (if there are any) to finish execution
    for (auto& thread : thread_pool) {
      thread.join();
    }
  }
}


// Select overlapping files from adjacent levels, merge them, publish the new
// metadata, and retire the inputs only after publication succeeds.
void Compaction::BackgroundCompaction() {
    const ScopedFixedPropertyLayout property_layout(&sub_property_lengths_);
    const ScopedDbPathOverride db_path_scope(&dataDir);
    std::shared_ptr<void> property_delta_guard;
    if (preparation_callback_) {
      std::string error;
      property_delta_guard = preparation_callback_(&error);
      if (property_delta_guard == nullptr) {
        std::cerr << "[PROPERTY_DELTA_COMPACTION_ERROR] " << error
                  << std::endl;
        return;
      }
    }
    int level=0;

    // 对于level-0需要先获取curr_version文件
    if (FLAGS_support_mulversion == true && level == 0) {
      l0_versionset_->VersionLock();
      fileMetaCache_[0] = l0_versionset_->GetCurrent()->GetLevel0Files();
      l0_versionset_->VersionUnLock();
    }

    if (FLAGS_support_mulversion == false && delete_fids_set_.Size() > 100) {
      printf("Note: delete_fids_set_.size()=%ld\n", delete_fids_set_.Size());
      RealRemoveFile();
    }

#ifdef WRITE_STALL_TEST
    int file_num = 0;
    for (auto it : fileMetaCache_) {
      file_num += it->size();
    }
    auto start = std::chrono::high_resolution_clock::now();
#endif


    while(true) {
        if(level >= fileMetaCache_.size()
           || fileMetaCache_[level]->size() < LevelMaxSize(level)
           || level + 1 >= MAX_LEVEL) {
          break;
        }

        inputs_[0].clear();
        inputs_[1].clear();
        ways.clear();
        merged_fids_[0].clear();
        merged_fids_[1].clear();
        delete_filemeta_set_.clear();
        bool need_del_file = true;
        min_level_0_fid_ = 0;

        // 对于level-0需要先获取curr_version文件
        if (FLAGS_support_mulversion == true && level == 0) {
          l0_versionset_->VersionLock();
          fileMetaCache_[0] = l0_versionset_->GetCurrent()->GetLevel0Files();
          l0_versionset_->VersionUnLock();
        }

        {
          // Keep source selection atomic with concurrent publication into the
          // next level.
          std::lock_guard<std::mutex> lock(*tablecache_mutex_[level]);
          std::lock_guard<std::mutex> lock2(*tablecache_mutex_[level+1]);
          PickCompactioFile(level, true);
        }

        for (auto it : inputs_[0]) {
          assert(it->Getref() > 0);
          merged_fids_[0].insert(it->header.timeStamp);
          #ifndef MULTI_LEVEL_VERSION
          if (FLAGS_support_mulversion == true && level == 0) {
            version_edit_.RemoveFile(it->header.timeStamp);
            if (min_level_0_fid_ < it->header.timeStamp) {
              min_level_0_fid_ = it->header.timeStamp;
            }
          }
          #else
          if (FLAGS_support_mulversion == true ) {
            version_edit_.RemoveFile(it->header.timeStamp);
            if ( level == 0 && min_level_0_fid_ < it->header.timeStamp) {
              min_level_0_fid_ = it->header.timeStamp;
            }
          }
          #endif
          if (FLAGS_support_mulversion == true) {
            delete_filemeta_set_.push_back(it);
          }

        }
        for (auto it : inputs_[1]) {
          assert(it->Getref() > 0);
          merged_fids_[1].insert(it->header.timeStamp);
          #ifdef MULTI_LEVEL_VERSION
          if (FLAGS_support_mulversion == true ) {
            version_edit_.RemoveFile(it->header.timeStamp);
          }
          #endif
          if (FLAGS_support_mulversion == true) {
            delete_filemeta_set_.push_back(it);
          }
        }

        if(level > 0 && inputs_[1].size() == 0){
          need_del_file = false;
        }
        if (level > 0 && inputs_[1].size() == 0) {
          DirectAdjustLevel(level);
        }
      #ifndef MERGE_LARGE_VERTEX
        else if (level == 0 && max_subcompactions_ > 1) {
          DoSubCompactionWork(level);
        }
      #endif
        else {
      #ifdef MERGE_LARGE_VERTEX_DEBUG
          std::cout << "DoGeneralCompactionWork: " << level << std::endl;
      #endif
          DoGeneralCompactionWork(level);
        }

#ifdef MULTI_LEVEL_VERSION
        if (FLAGS_support_mulversion == true) {
#else
        if (FLAGS_support_mulversion == true && level == 0) {
#endif
          Version* v = new Version(l0_versionset_);

          l0_versionset_->VersionLock();

          l0_versionset_->LogAndApply(version_edit_, v);
          fileMetaCache_[0] = l0_versionset_->GetCurrent()->GetLevel0Files();

          std::shared_ptr<VersionAndMemTableAndMemPropertyAndLf> old_vms;
          {
            std::shared_lock read_lock(sv_.vm_rw_mtx);
            old_vms = sv_.version_memtable_memproperty_lazyfile;
          }
          std::shared_ptr<VersionAndMemTableAndMemPropertyAndLf> new_vms =
              std::make_shared<VersionAndMemTableAndMemPropertyAndLf>();
          new_vms->batch_insert_tb(old_vms->menTables);
          new_vms->batch_insert_pp(old_vms->memProperties);
          new_vms->batch_insert_lf(&old_vms->sst_has_lf);
          for (auto it : lf_need_del) {
            for (auto file : it.second) {
              new_vms->remove_lf(it.first.first, it.first.second, file);
              int refs = file->Getref();
            }
          }

          new_vms->set_vs(l0_versionset_->GetCurrent());
          {
            std::unique_lock write_lock(sv_.vm_rw_mtx);
            sv_.version_memtable_memproperty_lazyfile = new_vms;
          }

          global_version_id_.fetch_add(1, std::memory_order_acquire);

          l0_versionset_->VersionUnLock();
          for (auto it : lf_need_del) {
            for (auto file : it.second) {
              sstdata_manager_.del_data(file->fid_);
              auto rt = utils::rmfile(pLazyFileName(file->fid_).c_str());
              delete file;
            }
          }
          lf_need_del.clear();
        #ifdef MULTI_LEVEL_VERSION
        }
        #else
        }
        #endif

      #ifndef MULTI_LEVEL_VERSION
        if (FLAGS_support_mulversion == true && need_del_file == true) {
          for (auto it : delete_filemeta_set_) {
              it->Unref();
          }
        }
      #endif

        if (fileMetaCache_[level]->size() > LevelMaxSize(level)) {
          continue; // 保证每一层数量合法
        }
        level++;
    }
#ifdef WRITE_STALL_TEST
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::micro> elapsed = end - start;
    timings.emplace_back(file_num, elapsed.count());
#endif
}


void Compaction::MaybeScheduleCompaction() {

#ifdef DEBUG_COST
std::chrono::steady_clock::time_point t1 = std::chrono::steady_clock::now();
#endif

  int curr_level_0file_num = 0;
  if (FLAGS_support_mulversion == true) {
    curr_level_0file_num =
      l0_versionset_->GetCurrent()->GetLevel0Files()->size();
  } else {
    curr_level_0file_num = fileMetaCache_[0]->size();
  }

  bool state = GetState();
  if (curr_level_0file_num >= static_cast<int>(l0_auto_compaction_limit_) &&
      !state && SetState(false, true)) {

#ifdef WRITE_STALL
    bool large(false);
    int i = 1;
    while( i < fileMetaCache_.size() && ((double) fileMetaCache_[i]->size() / (double) LevelMaxSize(i) ) > 0.8 ){
      ++i;
    }
    if (i-1 >= 2) {
      ++large_job_;
      if (large_job_ > max_job_num_){
        max_job_num_ = large_job_.load();
        printf("max job: %d\n", max_job_num_.load());
      }
      large = true;
    }
#endif

    BackgroundCompaction();

#ifdef WRITE_STALL
    if (large){
      --large_job_;
    }
#endif

    SetState(true, false);
  }

#ifdef DEBUG_COST
std::chrono::steady_clock::time_point t2 = std::chrono::steady_clock::now();
std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
write_add(&compaction_time, time_span.count());
#endif

}

bool Compaction::ForceCompactAllL0ToL1() {
  const ScopedFixedPropertyLayout property_layout(&sub_property_lengths_);
  const ScopedDbPathOverride db_path_scope(&dataDir);

  std::shared_ptr<void> property_delta_guard;
  if (preparation_callback_) {
    std::string error;
    property_delta_guard = preparation_callback_(&error);
    if (property_delta_guard == nullptr) {
      std::cerr << "[PROPERTY_DELTA_COMPACTION_ERROR] " << error
                << std::endl;
      return false;
    }
  }

  if (!SetState(false, true)) {
    std::cout << "[HOT_EDGE_CSR_COMPACTION] already_running: true"
              << std::endl;
    return false;
  }
  struct StateGuard {
    Compaction* self;
    ~StateGuard() { self->SetState(true, false); }
  } state_guard{this};

  constexpr int level = 0;
  if (fileMetaCache_.size() <= 1 || level + 1 >= MAX_LEVEL) {
    return false;
  }

  inputs_[0].clear();
  inputs_[1].clear();
  ways.clear();
  edge_cache.clear();
  merged_fids_[0].clear();
  merged_fids_[1].clear();
  delete_filemeta_set_.clear();
  lf_need_del.clear();
  version_edit_.Clear();
  min_level_0_fid_ = 0;

  if (FLAGS_support_mulversion == true) {
    l0_versionset_->VersionLock();
    fileMetaCache_[0] = l0_versionset_->GetCurrent()->GetLevel0Files();
    l0_versionset_->VersionUnLock();
  }

  {
    std::lock_guard<std::mutex> lock(*tablecache_mutex_[level]);
    std::lock_guard<std::mutex> lock2(*tablecache_mutex_[level + 1]);
    if (fileMetaCache_[level] != nullptr) {
      inputs_[0] = *fileMetaCache_[level];
    }
    if (!inputs_[0].empty()) {
      VertexId_t smallest = 0;
      VertexId_t largest = 0;
      GetRange(inputs_[0], smallest, largest);
      GetOverlappingInputs(level + 1, smallest, largest, &inputs_[1]);
    }
  }

  if (inputs_[0].empty()) {
    std::cout << "[HOT_EDGE_CSR_COMPACTION] l0_input_files: 0" << std::endl;
    return true;
  }

  std::cout << "[HOT_EDGE_CSR_COMPACTION] l0_input_files: "
            << inputs_[0].size() << std::endl;
  std::cout << "[HOT_EDGE_CSR_COMPACTION] l1_overlap_files: "
            << inputs_[1].size() << std::endl;

  for (auto* it : inputs_[0]) {
    if (it == nullptr) {
      continue;
    }
    assert(it->Getref() > 0);
    merged_fids_[0].insert(it->header.timeStamp);
    if (FLAGS_support_mulversion == true) {
      version_edit_.RemoveFile(it->header.timeStamp);
      if (min_level_0_fid_ < it->header.timeStamp) {
        min_level_0_fid_ = it->header.timeStamp;
      }
      delete_filemeta_set_.push_back(it);
    }
  }
  for (auto* it : inputs_[1]) {
    if (it == nullptr) {
      continue;
    }
    assert(it->Getref() > 0);
    merged_fids_[1].insert(it->header.timeStamp);
#ifdef MULTI_LEVEL_VERSION
    if (FLAGS_support_mulversion == true) {
      version_edit_.RemoveFile(it->header.timeStamp);
    }
#endif
    if (FLAGS_support_mulversion == true) {
      delete_filemeta_set_.push_back(it);
    }
  }

  if (max_subcompactions_ > 1) {
    DoSubCompactionWork(level);
  } else {
    DoGeneralCompactionWork(level);
  }

#ifdef MULTI_LEVEL_VERSION
  if (FLAGS_support_mulversion == true) {
#else
  if (FLAGS_support_mulversion == true && level == 0) {
#endif
    Version* v = new Version(l0_versionset_);

    l0_versionset_->VersionLock();

    l0_versionset_->LogAndApply(version_edit_, v);
    fileMetaCache_[0] = l0_versionset_->GetCurrent()->GetLevel0Files();

    std::shared_ptr<VersionAndMemTableAndMemPropertyAndLf> old_vms;
    {
      std::shared_lock read_lock(sv_.vm_rw_mtx);
      old_vms = sv_.version_memtable_memproperty_lazyfile;
    }
    std::shared_ptr<VersionAndMemTableAndMemPropertyAndLf> new_vms =
        std::make_shared<VersionAndMemTableAndMemPropertyAndLf>();
    new_vms->batch_insert_tb(old_vms->menTables);
    new_vms->batch_insert_pp(old_vms->memProperties);
    new_vms->batch_insert_lf(&old_vms->sst_has_lf);
    for (auto it : lf_need_del) {
      for (auto file : it.second) {
        new_vms->remove_lf(it.first.first, it.first.second, file);
      }
    }
    new_vms->set_vs(l0_versionset_->GetCurrent());
    {
      std::unique_lock write_lock(sv_.vm_rw_mtx);
      sv_.version_memtable_memproperty_lazyfile = new_vms;
    }

    global_version_id_.fetch_add(1, std::memory_order_acquire);

    l0_versionset_->VersionUnLock();
    for (auto it : lf_need_del) {
      for (auto file : it.second) {
        sstdata_manager_.del_data(file->fid_);
        auto rt = utils::rmfile(pLazyFileName(file->fid_).c_str());
        delete file;
      }
    }
    lf_need_del.clear();
  }

#ifndef MULTI_LEVEL_VERSION
  if (FLAGS_support_mulversion == true) {
    for (auto* it : delete_filemeta_set_) {
      if (it != nullptr) {
        it->Unref();
      }
    }
  }
#endif

  inputs_[0].clear();
  inputs_[1].clear();
  ways.clear();
  edge_cache.clear();
  merged_fids_[0].clear();
  merged_fids_[1].clear();
  delete_filemeta_set_.clear();
  version_edit_.Clear();
  return true;
}

void Compaction::change_sst_state(FileId_t sst_id, bool state){
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

void Compaction::clean(){
  ways.clear();
  std::vector<lsmgraph::Way>().swap(ways);

  edge_cache.clear();
  std::vector<lsmgraph::EdgeRecord>().swap(edge_cache);

  inputs_[0].clear();
  inputs_[1].clear();
  std::vector<lsmgraph::SSTableCache*>().swap(inputs_[0]);
  std::vector<lsmgraph::SSTableCache*>().swap(inputs_[1]);

  merged_fids_[0].clear();
  merged_fids_[1].clear();
  std::set<lsmgraph::FileId_t>().swap(merged_fids_[0]);
  std::set<lsmgraph::FileId_t>().swap(merged_fids_[1]);

  delete_filemeta_set_.clear();
  std::vector<SSTableCache*>().swap(delete_filemeta_set_);

  sst_is_vaild_to_ins_lf_.clear();
  std::map<lsmgraph::FileId_t, bool>().swap(sst_is_vaild_to_ins_lf_);

  lf_need_del.clear();
  std::map<std::pair<FileId_t, int>, std::vector<LazyFile*>>().swap(lf_need_del);  // 也适用于 unordered_map

}
} // lsmgraph namespace
