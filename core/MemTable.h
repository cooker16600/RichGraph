
#pragma once

#ifndef LSMGRAPH_NEWSKIPLIST_H
#define LSMGRAPH_NEWSKIPLIST_H
#include "core/DataStructs.h"
#include "core/SSTable.h"
#include "core/cache/SSTDataManager.h"
#include "core/compaction.h"
#include "core/config.h"
#include "core/del_record_manage.h"
#include "core/graph/edge.h"
#include "core/neighbors.h"
#include "core/types.h"
#include "core/version_set.h"
#include "util/ConcurrentHashMap.h"
#include "util/ConcurrentUnorderedMap.h"
#include "util/arena.h"
#include "util/char_arena.h"
#include "util/livegraph/allocator.hpp"
#include "util/livegraph/futex.hpp"
#include <fstream>
#include <iostream>
#include <set>
#include <string_view>
#include <unordered_map>
#include <vector>

#define VERTEX_ADJ_TYPE 0 // 0: array, 1: hashmap, 2: unordermap

using Futex = livegraph::Futex;

class SuperVersion;

namespace lsmgraph {
class Compaction;

/// listSize大小不包括属性的大小，即仅仅代表efile文件大小。
class MemTable {
private:
#if VERTEX_ADJ_TYPE == 0
  uintptr_t *vertex_adjs;
#elif VERTEX_ADJ_TYPE == 1
  ConcurrentHashMap<VertexId_t, uintptr_t> vertex_adjs;
#elif VERTEX_ADJ_TYPE == 2
  ConcurrentUnorderedMap<VertexId_t, uintptr_t> vertex_adjs;
#else
#endif
  uint64_t listLength; // the number of edges
  Arena<NeighBors> edge_arena;
  const size_t max_edge_num;
  size_t max_vertex_num;

public:
  std::atomic<VertexId_t> &vertex_id_; // max vertex_id actually used
  std::mutex update_mutex;

private:
  size_t src_vertex_num = 0;
  std::mutex &level_0_mux_;
  VersionSet *l0_versionset_;
  Futex *vertex_futexes_;
  size_t vertex_lock_count_;
  Level_t* vertex_max_level_;
  Compaction& compactor_;
  SuperVersion& sv_;
  std::atomic<SequenceNumber_t>& global_version_id_;
  SSTDataManager& sstdata_manager_;
  DelRecordManage& del_record_manager_;
  std::atomic<int32_t> refs;
  std::atomic<bool> islive{false};   // can read if free=true
  std::atomic<bool> isflash{false};  // can read if free=false
  livegraph::SparseArrayAllocator<void> array_allocator;
  FileId_t fid_;
  SequenceNumber_t start_time_;
 public:
  int64_t remain_capacity;
  std::atomic<SequenceNumber_t> newest_edge{0};
#ifdef DEBUG_COST
  static double save2eSSTable_time;
  static double put_flush_waite_time;
  static double put_memtable_time;
#endif
  MemTable(std::mutex& level_0_mux, const size_t _max_vertex_num,
           std::atomic<VertexId_t>& vertex_id, Futex* vertex_futexes,
           size_t vertex_lock_count, Level_t* vertex_max_level,
           VersionSet* l0_versionset, Compaction& compactor, SuperVersion& sv,
           SSTDataManager& sstdata_manager, DelRecordManage& del_record_manager,
           std::atomic<SequenceNumber_t>& global_version_id,
           size_t _max_edge_num = FLAGS_memtable_size)
      : listLength(0),
        max_edge_num(_max_edge_num),
        edge_arena(_max_edge_num + 1),
        level_0_mux_(level_0_mux),
        max_vertex_num(_max_vertex_num),
        vertex_id_(vertex_id),
        vertex_futexes_(vertex_futexes),
        vertex_lock_count_(vertex_lock_count),
        vertex_max_level_(vertex_max_level),
        l0_versionset_(l0_versionset),
        compactor_(compactor),
        sv_(sv),
        sstdata_manager_(sstdata_manager),
        del_record_manager_(del_record_manager),
        global_version_id_(global_version_id),
        fid_(0),
        array_allocator(),
#if VERTEX_ADJ_TYPE == 1 || VERTEX_ADJ_TYPE == 2
        vertex_adjs(NULLPOINTER),
#endif
        refs(0) {
#if VERTEX_ADJ_TYPE == 0
    auto pointer_allocater = std::allocator_traits<
        decltype(array_allocator)>::rebind_alloc<uintptr_t>(array_allocator);
    vertex_adjs = pointer_allocater.allocate(max_vertex_num);
#endif

    remain_capacity = max_edge_num;
    if (FLAGS_richgraph_verbose) {
      std::cout << " fid=" << fid_ << std::endl;
      std::cout << " max_vertex_num=" << max_vertex_num << std::endl;
      std::cout << " max_edge_num=" << max_edge_num << std::endl;
      std::cout << " vertex_id_=" << vertex_id_.load(std::memory_order_relaxed)
                << std::endl;
      std::cout << " max_skiplist: " << (max_edge_num * sizeof(Edge))
                << std::endl;
      std::cout << " sizeof(NeighBors): " << sizeof(NeighBors)
                << " w prenode: " << FLAGS_reserve_node * 64 << std::endl;
    }
    clear_vertex_adj();
  }

  ~MemTable() {
#if VERTEX_ADJ_TYPE == 0
    auto pointer_allocater = std::allocator_traits<
        decltype(array_allocator)>::rebind_alloc<uintptr_t>(array_allocator);
    pointer_allocater.deallocate(vertex_adjs, max_vertex_num);
#endif
  };

  void put_edge(VertexId_t src, VertexId_t dis, const EdgeProperty_t &s,
                Marker_t marker, SequenceNumber_t seq, size_t id,
                bool is_out = true, uint8_t edge_type = 0);

  void update_edge(VertexId_t src, VertexId_t dis, const EdgeProperty_t &s,
                   Marker_t marker, SequenceNumber_t seq, int sub_property_id);

  Status get(VertexId_t src_, VertexId_t dst_, std::string *property);

  Status get(VertexId_t src_, VertexId_t dst_, bool is_out, uint8_t edge_type,
             std::string *property);

  Status get(VertexId_t src, VertexId_t dst, FileId_t &fid,
             SequenceNumber_t &seq);

  Status get(VertexId_t src, VertexId_t dst, bool is_out, uint8_t edge_type,
             FileId_t &fid, SequenceNumber_t &seq);

  void get_edges(VertexId_t src, std::vector<Edge> &edges);

  void save2eSSTable(const std::string &dir, uint64_t &currentTime,
                     std::vector<SSTableCache *> *level_0_cache);

  void save2eSSTable_split_property_novar(
      const std::string &dir, uint64_t &currentTime,
      std::vector<SSTableCache *> *level_0_cache, int property_num);

  void save2eSSTable_split_property(const std::string &dir,
                                    uint64_t &currentTime,
                                    std::vector<SSTableCache *> *level_0_cache,
                                    int property_num);

  void save2eSSTable_split(const std::string &dir, uint64_t &currentTime,
                           std::vector<SSTableCache *> *level_0_cache);

  void print(std::string label);
  uint64_t GetListLength();
  template <typename T> void AutomicAdd(T &a, T b);
  uint64_t GetMaxEdgeNum();
  void SetStartTime(SequenceNumber_t start_time);
  SequenceNumber_t GetStartTime();
  void SetFid(FileId_t fid);
  FileId_t GetFid() const;
  void reset();
  void clear_vertex_adj();
  NeighBors *get_vertex_adj(VertexId_t src);

  bool checkIsFinishWrite();

  void Ref();
  void Unref();
  int32_t Getref();
  void SetLive(bool _islive);
  void SetFlash(bool _isflash);
  bool IsLive();
  bool IsFlash();

  void init_vertex_adjs(VertexId_t vid) {
#if VERTEX_ADJ_TYPE == 0
    vertex_adjs[vid] = NULLPOINTER;
#endif
  }
};
} // namespace lsmgraph

#endif // LSMGRAPH_NEWSKIPLIST_H
