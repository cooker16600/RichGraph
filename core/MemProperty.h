#pragma once
#ifndef MEMPROPERTY_H
#define MEMPROPERTY_H
#include "core/DataStructs.h"
#include "core/SSTable.h"
#include "util/arena.h"
#include "util/char_arena.h"
#include "core/graph/edge.h"
#include "core/types.h"
#include "core/config.h"
#include "core/version_set.h"
#include "core/compaction.h"
#include <unordered_map>
#include <string_view>
#include <set>
#include <iostream>
#include <fstream>
#include <vector>
#include "util/livegraph/futex.hpp"
#include "util/livegraph/allocator.hpp"
#include "util/ConcurrentHashMap.h"
#include "util/ConcurrentUnorderedMap.h"
#include "core/cache/SSTDataManager.h"
#include "core/neighbors.h"
#include "core/del_record_manage.h"

using Futex = livegraph::Futex;

class SuperVersion;

namespace lsmgraph {

class MemProperty {
private:
    uint64_t listLength;


    Arena<NeighBors> edge_arena;

    const size_t max_edge_num;
    
    size_t max_vertex_num;
    std::atomic<VertexId_t>& vertex_id_;  // max vertex_id actually used
    size_t src_vertex_num = 0;
    std::mutex& level_0_mux_;
    VersionSet* l0_versionset_;
    Futex* vertex_futexes_;
    size_t vertex_lock_count_;
    Level_t* vertex_max_level_;
    SuperVersion& sv_;
    std::atomic<SequenceNumber_t>& global_version_id_;
    SSTDataManager& sstdata_manager_;
    DelRecordManage& del_record_manager_;
    std::atomic<int32_t> refs;
    std::atomic<bool> islive{false};   // can read if free=true
    std::atomic<bool> isflash{false};  // can read if free=false
    livegraph::SparseArrayAllocator<void> array_allocator;
    FileId_t fid_;
    SequenceNumber_t start_time_ = 0;

   public:
    int64_t remain_capacity;
    uintptr_t* vertex_adjs;
    std::atomic<SequenceNumber_t> newest_edge{0};

    MemProperty(std::mutex& level_0_mux, const size_t _max_vertex_num,
                std::atomic<VertexId_t>& vertex_id, Futex* vertex_futexes,
                size_t vertex_lock_count, Level_t* vertex_max_level,
                VersionSet* l0_versionset,
                SuperVersion& sv, SSTDataManager& sstdata_manager,
                DelRecordManage& del_record_manager,
                std::atomic<SequenceNumber_t>& global_version_id,
                size_t _max_edge_num = FLAGS_memproperty_size)
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
          sv_(sv),
          sstdata_manager_(sstdata_manager),
          del_record_manager_(del_record_manager),
          global_version_id_(global_version_id),
          fid_(0),
          array_allocator(),
          refs(0) {
      auto pointer_allocater = std::allocator_traits<
          decltype(array_allocator)>::rebind_alloc<uintptr_t>(array_allocator);
      vertex_adjs = pointer_allocater.allocate(max_vertex_num);
      clear_vertex_adj();
    }

        ~MemProperty() {
        auto pointer_allocater =
            std::allocator_traits<decltype(array_allocator)>::
            rebind_alloc<uintptr_t>(array_allocator);
        pointer_allocater.deallocate(vertex_adjs, max_vertex_num);
    };
    void clear_vertex_adj();

    void update_edge(VertexId_t src, VertexId_t dst, const EdgeProperty_t& s, 
        Marker_t marker, SequenceNumber_t seq, size_t id,
        bool is_out = true, uint8_t edge_type = 0);
    Status get_property(VertexId_t src_, VertexId_t dst_, std::string* property);
    Status get_property(VertexId_t src_, VertexId_t dst_, bool is_out,
                        uint8_t edge_type, std::string* property);
    // 用于获取边的给定类型的属性

    void save2eSSTable(const std::string &dir, uint64_t &currentTime,
                       std::vector<SSTableCache*>* level_0_cache);
    // 用于将内存表中缓存的更新数据存储到SSTable（segment）中，参数可能还需要调整
    
    void Ref();
  
    void Unref();

    int32_t Getref();

    bool IsLive();

    bool IsFlash();

    void reset();

    void SetLive(bool _islive);

    void SetFlash(bool _isflash);

    void SetFid(FileId_t fid);

    FileId_t GetFid();

    uint64_t GetMaxEdgeNum();

    void SetStartTime(SequenceNumber_t start_time);

    SequenceNumber_t GetStartTime();

    NeighBors *get_vertex_adj(VertexId_t src);
    
    uint64_t GetListLength();
};

}

#endif // MEMPROPERTY_H
